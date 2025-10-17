use crate::engine::core::ZonePlan;
use crate::engine::core::column::compression::compression_codec::{
    ALGO_LZ4, CompressionCodec, FLAG_COMPRESSED, Lz4Codec,
};
use crate::engine::core::filter::surf_encoding::encode_value;
use crate::engine::core::filter::surf_trie::SurfTrie;
use crate::engine::schema::registry::MiniSchema;
use crate::engine::schema::types::FieldType;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::simd::Simd;
use std::simd::prelude::*;
use tracing::{debug, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZoneSurfEntry {
    pub zone_id: u32,
    pub trie: SurfTrie,
}
fn is_field_numeric_consistent(zone_plans: &[ZonePlan], key: &str) -> bool {
    // Determine a stable numeric kind for this key across all events in all zones
    // Return false if any value is non-numeric or mixes numeric kinds
    #[derive(Clone, Copy, PartialEq)]
    enum Kind {
        I,
        U,
        F,
        Unknown,
    }
    let mut kind = Kind::Unknown;
    for zp in zone_plans {
        for ev in &zp.events {
            let Some(v) = ev.payload.get(key) else {
                continue;
            };
            match v {
                Value::Number(n) => {
                    let this = if n.as_i64().is_some() {
                        Kind::I
                    } else if n.as_u64().is_some() {
                        Kind::U
                    } else if n.as_f64().is_some() {
                        Kind::F
                    } else {
                        return false;
                    };
                    kind = match (kind, this) {
                        (Kind::Unknown, k) => k,
                        (k, t) if k == t => k,
                        // allow I/U mix by promoting to F only if all integral; to simplify, reject mix here
                        _ => return false,
                    };
                }
                _ => return false,
            }
        }
    }
    kind != Kind::Unknown
}

#[inline]
fn is_time_field(ft: &FieldType) -> bool {
    match ft {
        FieldType::Timestamp | FieldType::Date => true,
        FieldType::Optional(inner) => matches!(**inner, FieldType::Timestamp | FieldType::Date),
        _ => false,
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZoneSurfFilter {
    pub entries: Vec<ZoneSurfEntry>,
}

impl ZoneSurfFilter {
    // Removed id-like heuristic; we will filter by numeric-consistency instead

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        // Serialize
        let data = bincode::serialize(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        // Compress with LZ4
        let codec = Lz4Codec;
        let compressed = codec
            .compress(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        // Header with compressed flag
        let header = BinaryHeader::new(FileKind::ZoneSurfFilter.magic(), 1, FLAG_COMPRESSED);
        header.write_to(&mut writer)?;
        // Algo id then payload
        writer.write_all(&ALGO_LZ4.to_le_bytes())?;
        writer.write_all(&compressed)?;
        writer.flush()?;
        Ok(())
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let mut slice = &mmap[..];
        let header = BinaryHeader::read_from(&mut slice)?;
        if header.magic != FileKind::ZoneSurfFilter.magic() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid zonesurffilter magic",
            ));
        }
        if (header.flags & FLAG_COMPRESSED) != 0 {
            // read algo id
            if mmap.len() < BinaryHeader::TOTAL_LEN + 2 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "truncated compressed zonesurffilter",
                ));
            }
            let mut algo_bytes = [0u8; 2];
            algo_bytes.copy_from_slice(&mmap[BinaryHeader::TOTAL_LEN..BinaryHeader::TOTAL_LEN + 2]);
            let algo_id = u16::from_le_bytes(algo_bytes);
            if algo_id != ALGO_LZ4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported SuRF compression algo: {}", algo_id),
                ));
            }
            let codec = Lz4Codec;
            let decompressed = codec
                .decompress(&mmap[BinaryHeader::TOTAL_LEN + 2..], 0)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let filter: ZoneSurfFilter = bincode::deserialize(&decompressed)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            return Ok(filter);
        }
        // legacy
        let filter: ZoneSurfFilter = bincode::deserialize(&mmap[BinaryHeader::TOTAL_LEN..])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(filter)
    }

    fn get_filter_path(uid: &str, field: &str, segment_dir: &Path) -> PathBuf {
        segment_dir.join(format!("{}_{}.zsrf", uid, field))
    }

    pub fn build_all(zone_plans: &[ZonePlan], segment_dir: &Path) -> std::io::Result<()> {
        debug!(target: "sneldb::surf", "Building Zone SuRF for {} zones", zone_plans.len());
        let mut map: HashMap<(String, String), Vec<(u32, Vec<Vec<u8>>)>> = HashMap::new();

        for zp in zone_plans {
            // fixed fields: include only if numeric
            for field in ["timestamp"] {
                let mut values: Vec<Vec<u8>> = Vec::new();
                for ev in &zp.events {
                    if let Some(v) = ev.get_field(field) {
                        if v.is_number() {
                            if let Some(bytes) = encode_value(&v) {
                                values.push(bytes);
                            }
                        }
                    }
                }
                if !values.is_empty() {
                    map.entry((zp.uid.clone(), field.to_string()))
                        .or_default()
                        .push((zp.id, values));
                }
            }
            // dynamic payload: only numeric-consistent fields (same numeric kind across all events)
            let mut dynamic_keys: Vec<String> = Vec::new();
            if let Some(obj) = zp.events.get(0).and_then(|e| e.payload.as_object()) {
                dynamic_keys.extend(obj.keys().cloned());
            }
            for key in dynamic_keys {
                if !is_field_numeric_consistent(zone_plans, &key) {
                    continue;
                }
                let mut values: Vec<Vec<u8>> = Vec::new();
                for ev in &zp.events {
                    if let Some(val) = ev.payload.get(&key) {
                        if let Some(bytes) = encode_value(val) {
                            values.push(bytes);
                        }
                    }
                }
                if !values.is_empty() {
                    map.entry((zp.uid.clone(), key))
                        .or_default()
                        .push((zp.id, values));
                }
            }
        }

        for ((uid, field), per_zone) in map.into_iter() {
            let mut entries: Vec<ZoneSurfEntry> = Vec::new();
            for (zone_id, mut values) in per_zone {
                values.sort();
                values.dedup();
                let trie = SurfTrie::build_from_sorted(&values);
                entries.push(ZoneSurfEntry { zone_id, trie });
            }
            entries.sort_by_key(|e| e.zone_id);
            let out = ZoneSurfFilter { entries };
            let path = Self::get_filter_path(&uid, &field, segment_dir);
            info!(target: "sneldb::surf", uid = %uid, field = %field, path = %path.display(), "Writing Zone SuRF");
            if let Err(e) = out.save(&path) {
                warn!(target: "sneldb::surf", error = %e, "Failed to save Zone SuRF; continuing");
            }
        }
        Ok(())
    }

    pub fn build_all_with_schema(
        zone_plans: &[ZonePlan],
        segment_dir: &Path,
        schema: &MiniSchema,
    ) -> std::io::Result<()> {
        debug!(target: "sneldb::surf", "Building Zone SuRF (schema-aware) for {} zones", zone_plans.len());
        let mut map: HashMap<(String, String), Vec<(u32, Vec<Vec<u8>>)>> = HashMap::new();

        // Collect schema-defined time fields
        let mut time_fields: Vec<String> = Vec::new();
        for (name, ty) in &schema.fields {
            if is_time_field(ty) {
                time_fields.push(name.clone());
            }
        }

        for zp in zone_plans {
            // fixed field: timestamp (only if numeric)
            for field in ["timestamp"] {
                let mut values: Vec<Vec<u8>> = Vec::new();
                for ev in &zp.events {
                    if let Some(v) = ev.get_field(field) {
                        if v.is_number() {
                            if let Some(bytes) = encode_value(&v) {
                                values.push(bytes);
                            }
                        }
                    }
                }
                if !values.is_empty() {
                    map.entry((zp.uid.clone(), field.to_string()))
                        .or_default()
                        .push((zp.id, values));
                }
            }

            // schema time fields in payload
            for key in &time_fields {
                let mut values: Vec<Vec<u8>> = Vec::new();
                for ev in &zp.events {
                    if let Some(val) = ev.payload.get(key) {
                        if val.is_number() {
                            if let Some(bytes) = encode_value(val) {
                                values.push(bytes);
                            }
                        }
                    }
                }
                if !values.is_empty() {
                    map.entry((zp.uid.clone(), key.clone()))
                        .or_default()
                        .push((zp.id, values));
                }
            }

            // Also include numeric-consistent fields beyond time fields
            let mut dynamic_keys: Vec<String> = Vec::new();
            if let Some(obj) = zp.events.get(0).and_then(|e| e.payload.as_object()) {
                dynamic_keys.extend(obj.keys().cloned());
            }
            for key in dynamic_keys {
                if time_fields.contains(&key) {
                    continue;
                }
                if !is_field_numeric_consistent(zone_plans, &key) {
                    continue;
                }
                let mut values: Vec<Vec<u8>> = Vec::new();
                for ev in &zp.events {
                    if let Some(val) = ev.payload.get(&key) {
                        if let Some(bytes) = encode_value(val) {
                            values.push(bytes);
                        }
                    }
                }
                if !values.is_empty() {
                    map.entry((zp.uid.clone(), key))
                        .or_default()
                        .push((zp.id, values));
                }
            }
        }

        for ((uid, field), per_zone) in map.into_iter() {
            let mut entries: Vec<ZoneSurfEntry> = Vec::new();
            for (zone_id, mut values) in per_zone {
                values.sort();
                values.dedup();
                let trie = SurfTrie::build_from_sorted(&values);
                entries.push(ZoneSurfEntry { zone_id, trie });
            }
            entries.sort_by_key(|e| e.zone_id);
            let out = ZoneSurfFilter { entries };
            let path = Self::get_filter_path(&uid, &field, segment_dir);
            info!(target = "sneldb::surf", uid = %uid, field = %field, path = %path.display(), "Writing Zone SuRF");
            if let Err(e) = out.save(&path) {
                warn!(target = "sneldb::surf", error = %e, "Failed to save Zone SuRF; continuing");
            }
        }
        Ok(())
    }

    pub fn zones_overlapping_ge(
        &self,
        lower: &[u8],
        inclusive: bool,
        segment_id: &str,
    ) -> Vec<crate::engine::core::zone::candidate_zone::CandidateZone> {
        let mut result = Vec::new();
        let mut total_nodes_visited: usize = 0;
        let mut total_edges_examined: usize = 0;
        let mut total_backtracks: usize = 0;
        let mut total_left_descents: usize = 0;

        for e in &self.entries {
            let q = SurfQuery { trie: &e.trie };
            let (overlaps, stats) = q.may_overlap_ge_with_stats(lower, inclusive);
            total_nodes_visited += stats.nodes_visited;
            total_edges_examined += stats.edges_examined;
            total_backtracks += stats.backtracks;
            total_left_descents += stats.leftmost_descents;

            if overlaps {
                result.push(
                    crate::engine::core::zone::candidate_zone::CandidateZone::new(
                        e.zone_id,
                        segment_id.to_string(),
                    ),
                );
            }
        }

        info!(
            target: "sneldb::surf",
            segment_id = %segment_id,
            zones_total = self.entries.len(),
            zones_matched = result.len(),
            nodes_visited = total_nodes_visited,
            edges_examined = total_edges_examined,
            backtracks = total_backtracks,
            left_descents = total_left_descents,
            inclusive,
            lower_b64 = %B64.encode(lower),
            "SuRF GE probe summary"
        );

        result
    }

    pub fn zones_overlapping_le(
        &self,
        upper: &[u8],
        inclusive: bool,
        segment_id: &str,
    ) -> Vec<crate::engine::core::zone::candidate_zone::CandidateZone> {
        let mut result = Vec::new();
        let mut total_nodes_visited: usize = 0;
        let mut total_edges_examined: usize = 0;
        let mut total_backtracks: usize = 0;
        let mut total_right_descents: usize = 0;

        for e in &self.entries {
            let q = SurfQuery { trie: &e.trie };
            let (overlaps, stats) = q.may_overlap_le_with_stats(upper, inclusive);
            total_nodes_visited += stats.nodes_visited;
            total_edges_examined += stats.edges_examined;
            total_backtracks += stats.backtracks;
            total_right_descents += stats.rightmost_descents;

            if overlaps {
                result.push(
                    crate::engine::core::zone::candidate_zone::CandidateZone::new(
                        e.zone_id,
                        segment_id.to_string(),
                    ),
                );
            }
        }

        info!(
            target: "sneldb::surf",
            segment_id = %segment_id,
            zones_total = self.entries.len(),
            zones_matched = result.len(),
            nodes_visited = total_nodes_visited,
            edges_examined = total_edges_examined,
            backtracks = total_backtracks,
            right_descents = total_right_descents,
            inclusive,
            upper_b64 = %B64.encode(upper),
            "SuRF LE probe summary"
        );

        result
    }
}

struct SurfQuery<'a> {
    trie: &'a SurfTrie,
}

#[derive(Default, Debug, Clone, Copy)]
struct SurfProbeStats {
    nodes_visited: usize,
    edges_examined: usize,
    backtracks: usize,
    leftmost_descents: usize,
    rightmost_descents: usize,
}

impl<'a> SurfQuery<'a> {
    #[inline]
    fn simd_first_ge(slice: &[u8], tb: u8) -> Option<usize> {
        const LANES: usize = 16;
        let mut i = 0;
        let len = slice.len();
        while i + LANES <= len {
            let v = Simd::<u8, LANES>::from_array(
                slice[i..i + LANES]
                    .try_into()
                    .expect("slice to array of LANES"),
            );
            let m = v.simd_ge(Simd::splat(tb));
            let bits = m.to_bitmask();
            if bits != 0 {
                let j = bits.trailing_zeros() as usize;
                return Some(i + j);
            }
            i += LANES;
        }
        while i < len {
            if slice[i] >= tb {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    #[inline]
    fn simd_last_le(slice: &[u8], tb: u8) -> Option<usize> {
        const LANES: usize = 16;
        let len = slice.len();
        let mut i = len;
        while i >= LANES {
            let start = i - LANES;
            let v = Simd::<u8, LANES>::from_array(
                slice[start..i].try_into().expect("slice to array of LANES"),
            );
            let m = v.simd_le(Simd::splat(tb));
            let bits = m.to_bitmask();
            if bits != 0 {
                let j = (LANES - 1) - (bits.leading_zeros() as usize);
                return Some(start + j);
            }
            i -= LANES;
        }
        while i > 0 {
            let idx = i - 1;
            if slice[idx] <= tb {
                return Some(idx);
            }
            i -= 1;
        }
        None
    }

    fn child_range(&self, node_idx: usize) -> (usize, usize) {
        self.trie.child_range(node_idx)
    }

    fn find_first_key_geq_with_stats(
        &self,
        target: &[u8],
        stats: &mut SurfProbeStats,
    ) -> Option<Vec<u8>> {
        // Stack of backtrack points: (node_idx, s, e, chosen_edge_idx, path_len)
        let mut stack: Vec<(usize, usize, usize, usize, usize)> = Vec::new();
        let mut node = 0usize;
        let mut depth = 0usize;
        let mut path: Vec<u8> = Vec::new();

        // Helper: descend to the leftmost terminal from node, appending to path
        let mut descend_leftmost = |mut node_idx: usize, mut out: Vec<u8>| -> Option<Vec<u8>> {
            loop {
                stats.leftmost_descents += 1;
                if self.trie.is_terminal(node_idx) {
                    return Some(out);
                }
                let (s, e) = self.child_range(node_idx);
                if s == e {
                    return None;
                }
                let edge = s;
                out.push(self.trie.labels[edge]);
                node_idx = self.trie.edge_to_child[edge] as usize;
            }
        };

        loop {
            stats.nodes_visited += 1;
            let (s, e) = self.child_range(node);
            if depth == target.len() {
                return descend_leftmost(node, path);
            }
            let tb = target[depth];

            // Vector-accelerated search for first child with label >= tb
            let mut equal_idx: Option<usize> = None;
            let mut first_ge_idx: Option<usize> = None;
            let slice = &self.trie.labels[s..e];
            if !slice.is_empty() {
                if let Some(off) = Self::simd_first_ge(slice, tb) {
                    let idx = s + off;
                    first_ge_idx = Some(idx);
                    if self.trie.labels[idx] == tb {
                        equal_idx = Some(idx);
                    }
                    // Approximate accounting: one probe per SIMD chunk
                    stats.edges_examined += (slice.len() + 15) / 16;
                }
            }

            if let Some(eq) = equal_idx {
                // Follow equal; push backtrack point to try next greater sibling later if needed
                stack.push((node, s, e, eq, path.len()));
                path.push(self.trie.labels[eq]);
                node = self.trie.edge_to_child[eq] as usize;
                depth += 1;
                continue;
            }

            if let Some(ge) = first_ge_idx {
                // Found greater at this level; descend leftmost and return
                path.push(self.trie.labels[ge]);
                let child = self.trie.edge_to_child[ge] as usize;
                return descend_leftmost(child, path);
            }

            // No child >= tb at this level: backtrack to find next greater sibling at some ancestor
            while let Some((bnode, _bs, be, chosen, plen)) = stack.pop() {
                let _ = bnode;
                if chosen + 1 < be {
                    stats.backtracks += 1;
                    let nxt = chosen + 1;
                    path.truncate(plen);
                    path.push(self.trie.labels[nxt]);
                    let child = self.trie.edge_to_child[nxt] as usize;
                    return descend_leftmost(child, path);
                } else {
                    // continue backtracking
                }
            }
            return None;
        }
    }

    fn find_last_key_leq_with_stats(
        &self,
        target: &[u8],
        stats: &mut SurfProbeStats,
    ) -> Option<Vec<u8>> {
        // Stack of backtrack points: (node_idx, s, e, chosen_edge_idx, path_len)
        let mut stack: Vec<(usize, usize, usize, usize, usize)> = Vec::new();
        let mut node = 0usize;
        let mut depth = 0usize;
        let mut path: Vec<u8> = Vec::new();

        // Helper: descend to the rightmost terminal from node, appending to path
        let mut descend_rightmost = |mut node_idx: usize, mut out: Vec<u8>| -> Option<Vec<u8>> {
            loop {
                let (s, e) = self.child_range(node_idx);
                if s == e {
                    return if self.trie.is_terminal(node_idx) {
                        Some(out)
                    } else {
                        None
                    };
                }
                let edge = e - 1;
                out.push(self.trie.labels[edge]);
                node_idx = self.trie.edge_to_child[edge] as usize;
                stats.rightmost_descents += 1;
            }
        };

        loop {
            stats.nodes_visited += 1;
            let (s, e) = self.child_range(node);
            if depth == target.len() {
                // We have matched full target; prefer the rightmost under current node
                if let Some(k) = descend_rightmost(node, path.clone()) {
                    return Some(k);
                }
                return if self.trie.is_terminal(node) {
                    Some(path)
                } else {
                    None
                };
            }
            let tb = target[depth];

            // Vector-accelerated search for last child with label <= tb
            let mut equal_idx: Option<usize> = None;
            let mut last_le_idx: Option<usize> = None;
            let slice = &self.trie.labels[s..e];
            if !slice.is_empty() {
                if let Some(off) = Self::simd_last_le(slice, tb) {
                    let idx = s + off;
                    last_le_idx = Some(idx);
                    if self.trie.labels[idx] == tb {
                        equal_idx = Some(idx);
                    }
                    // Approximate accounting: one probe per SIMD chunk
                    stats.edges_examined += (slice.len() + 15) / 16;
                }
            }

            if let Some(eq) = equal_idx {
                // Follow equal; push backtrack point to try previous smaller sibling later if needed
                stack.push((node, s, e, eq, path.len()));
                path.push(self.trie.labels[eq]);
                node = self.trie.edge_to_child[eq] as usize;
                depth += 1;
                continue;
            }

            if let Some(le) = last_le_idx {
                // Found smaller at this level; descend rightmost and return
                path.push(self.trie.labels[le]);
                let child = self.trie.edge_to_child[le] as usize;
                return descend_rightmost(child, path);
            }

            // No child <= tb at this level: backtrack to find previous smaller sibling at an ancestor
            while let Some((bnode, bs, _be, chosen, plen)) = stack.pop() {
                if chosen > bs {
                    stats.backtracks += 1;
                    let prev = chosen - 1;
                    path.truncate(plen);
                    path.push(self.trie.labels[prev]);
                    let child = self.trie.edge_to_child[prev] as usize;
                    return descend_rightmost(child, path);
                } else {
                    let _ = bnode; // continue backtracking
                }
            }
            // If no backtrack available, and current node is terminal, it itself may be <= target
            return if self.trie.is_terminal(node) {
                Some(path)
            } else {
                None
            };
        }
    }

    fn find_last_key(&self) -> Option<Vec<u8>> {
        let mut out: Vec<u8> = Vec::new();
        let mut node_idx = 0usize;
        loop {
            let (s, e) = self.child_range(node_idx);
            if s == e {
                return if self.trie.is_terminal(node_idx) {
                    Some(out)
                } else {
                    None
                };
            }
            let edge = e - 1;
            out.push(self.trie.labels[edge]);
            node_idx = self.trie.edge_to_child[edge] as usize;
        }
    }

    fn find_first_key(&self) -> Option<Vec<u8>> {
        let mut out: Vec<u8> = Vec::new();
        let mut node_idx = 0usize;
        loop {
            if self.trie.is_terminal(node_idx) {
                return Some(out);
            }
            let (s, e) = self.child_range(node_idx);
            if s == e {
                return None;
            }
            let edge = s;
            out.push(self.trie.labels[edge]);
            node_idx = self.trie.edge_to_child[edge] as usize;
        }
    }

    fn may_overlap_ge_with_stats(&self, lower: &[u8], inclusive: bool) -> (bool, SurfProbeStats) {
        let mut stats = SurfProbeStats::default();
        if inclusive {
            let found = self
                .find_first_key_geq_with_stats(lower, &mut stats)
                .is_some();
            return (found, stats);
        } else {
            match self.find_first_key_geq_with_stats(lower, &mut stats) {
                Some(k) if k.as_slice() > lower => (true, stats),
                Some(_k_eq) => {
                    if let Some(last) = self.find_last_key() {
                        (last.as_slice() > lower, stats)
                    } else {
                        (false, stats)
                    }
                }
                None => (false, stats),
            }
        }
    }

    fn may_overlap_le_with_stats(&self, upper: &[u8], inclusive: bool) -> (bool, SurfProbeStats) {
        let mut stats = SurfProbeStats::default();
        if inclusive {
            let found = self
                .find_last_key_leq_with_stats(upper, &mut stats)
                .is_some();
            return (found, stats);
        } else {
            match self.find_last_key_leq_with_stats(upper, &mut stats) {
                Some(k) if k.as_slice() < upper => (true, stats),
                Some(_k_eq) => {
                    if let Some(min_k) = self.find_first_key() {
                        (min_k.as_slice() < upper, stats)
                    } else {
                        (false, stats)
                    }
                }
                None => (false, stats),
            }
        }
    }
}
