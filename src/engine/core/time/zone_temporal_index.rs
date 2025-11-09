use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::command::types::CompareOp;
use crate::engine::core::time::temporal_traits::ZoneRangeIndex;
use crate::shared::storage_header::{BinaryHeader, FileKind};

#[derive(Debug, Clone)]
pub struct ZoneTemporalIndex {
    pub min_ts: i64,
    pub max_ts: i64,
    pub stride: i64,
    // Minimal placeholder representation until EF is implemented
    pub keys: Vec<u64>,
    // fences: (sample_ts, approx_row)
    pub fences: Vec<(i64, u32)>,
}

impl ZoneTemporalIndex {
    pub fn from_timestamps(mut ts: Vec<i64>, stride: i64, fence_count: usize) -> Self {
        ts.sort_unstable();
        ts.dedup();
        let min_ts = *ts.first().unwrap_or(&0);
        let max_ts = *ts.last().unwrap_or(&0);
        let keys: Vec<u64> = ts
            .iter()
            .map(|&t| ((t - min_ts) / stride).max(0) as u64)
            .collect();
        let fences = Self::build_fences(&ts, fence_count);
        Self {
            min_ts,
            max_ts,
            stride,
            keys,
            fences,
        }
    }

    fn build_fences(ts: &Vec<i64>, fence_count: usize) -> Vec<(i64, u32)> {
        if ts.is_empty() || fence_count == 0 {
            return Vec::new();
        }
        let n = ts.len();
        let step = (n as f64 / fence_count as f64).max(1.0);
        let mut out = Vec::with_capacity(fence_count);
        let mut i = 0.0;
        while (i as usize) < n {
            let idx = (i as usize).min(n - 1);
            out.push((ts[idx], idx as u32));
            i += step;
        }
        out
    }

    #[inline]
    pub fn contains_ts(&self, ts: i64) -> bool {
        if ts < self.min_ts || ts > self.max_ts {
            return false;
        }
        let off = ts - self.min_ts;
        if self.stride > 1 && (off % self.stride) != 0 {
            return false;
        }
        let key = (off / self.stride).max(0) as u64;
        // Temporary binary search on keys until EF is wired
        self.keys.binary_search(&key).is_ok()
    }

    pub fn predecessor_ts(&self, ts: i64) -> Option<i64> {
        if ts < self.min_ts {
            return None;
        }
        let key = ((ts - self.min_ts) / self.stride).max(0) as u64;
        match self.keys.binary_search(&key) {
            Ok(_) => Some(ts),
            Err(pos) => {
                if pos == 0 {
                    None
                } else {
                    Some(self.min_ts + (self.keys[pos - 1] as i64) * self.stride)
                }
            }
        }
    }

    pub fn fence_lb_ub(&self, _ts: i64) -> (u32, u32) {
        // Placeholder: full range; refine later with fences
        (0, (self.keys.len() as u32).saturating_sub(1))
    }

    pub fn save(&self, uid: &str, zone_id: u32, dir: &Path) -> std::io::Result<()> {
        let path = dir.join(format!("{}_{}.tfi", uid, zone_id));
        let file = std::fs::File::create(&path)?;
        let mut w = BufWriter::with_capacity(256 * 1024, file);
        let header = BinaryHeader::new(FileKind::TemporalIndex.magic(), 1, 0);
        header.write_to(&mut w)?;

        w.write_all(&self.min_ts.to_le_bytes())?;
        w.write_all(&self.max_ts.to_le_bytes())?;
        w.write_all(&self.stride.to_le_bytes())?;

        w.write_all(&(self.keys.len() as u32).to_le_bytes())?;
        // Write keys in a single contiguous buffer when possible
        for k in &self.keys {
            w.write_all(&k.to_le_bytes())?;
        }

        w.write_all(&(self.fences.len() as u32).to_le_bytes())?;
        for (ts, row) in &self.fences {
            w.write_all(&ts.to_le_bytes())?;
            w.write_all(&row.to_le_bytes())?;
        }

        w.flush()
    }

    pub fn load(uid: &str, zone_id: u32, dir: &Path) -> std::io::Result<Self> {
        let path = dir.join(format!("{}_{}.tfi", uid, zone_id));
        let mut file = std::fs::File::open(&path)?;
        let _ = BinaryHeader::read_from(&mut file)?;

        fn read_i64<R: Read>(r: &mut R) -> std::io::Result<i64> {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(i64::from_le_bytes(b))
        }
        fn read_u64<R: Read>(r: &mut R) -> std::io::Result<u64> {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(u64::from_le_bytes(b))
        }
        fn read_u32<R: Read>(r: &mut R) -> std::io::Result<u32> {
            let mut b = [0u8; 4];
            r.read_exact(&mut b)?;
            Ok(u32::from_le_bytes(b))
        }

        let min_ts = read_i64(&mut file)?;
        let max_ts = read_i64(&mut file)?;
        let stride = read_i64(&mut file)?;

        let key_len = read_u32(&mut file)? as usize;
        let mut keys = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            keys.push(read_u64(&mut file)?);
        }

        let fence_len = read_u32(&mut file)? as usize;
        let mut fences = Vec::with_capacity(fence_len);
        for _ in 0..fence_len {
            let ts = read_i64(&mut file)?;
            let row = read_u32(&mut file)?;
            fences.push((ts, row));
        }

        Ok(Self {
            min_ts,
            max_ts,
            stride,
            keys,
            fences,
        })
    }

    /// Field-aware variants (new format): {uid}_{field}_{zone}.tfi
    pub fn save_for_field(
        &self,
        uid: &str,
        field: &str,
        zone_id: u32,
        dir: &Path,
    ) -> std::io::Result<()> {
        // Deprecated: per-zone per-field files replaced by slab file. Use save_field_slab instead.
        let entries: Vec<(u32, &ZoneTemporalIndex)> = vec![(zone_id, self)];
        Self::save_field_slab(uid, field, dir, &entries)
    }

    /// Load a specific zone's temporal index for a given field from the per-field slab file.
    pub fn load_for_field(
        uid: &str,
        field: &str,
        zone_id: u32,
        dir: &Path,
    ) -> std::io::Result<Self> {
        let path = dir.join(format!("{}_{}.tfi", uid, field));
        let mut file = std::fs::File::open(&path)?;
        let _ = BinaryHeader::read_from(&mut file)?; // version 2 (slab)

        // Directory
        let mut u32buf = [0u8; 4];
        let mut u64buf = [0u8; 8];
        file.read_exact(&mut u32buf)?;
        let count = u32::from_le_bytes(u32buf) as usize;
        let mut found: Option<(u64, u32)> = None; // (offset, len)
        for _ in 0..count {
            file.read_exact(&mut u32buf)?;
            let zid = u32::from_le_bytes(u32buf);
            file.read_exact(&mut u64buf)?;
            let offset = u64::from_le_bytes(u64buf);
            file.read_exact(&mut u32buf)?;
            let len = u32::from_le_bytes(u32buf);
            if zid == zone_id {
                found = Some((offset, len));
            }
        }
        let (offset, _len) = found.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "zone id not found in temporal slab",
            )
        })?;
        file.seek(SeekFrom::Start(offset))?;
        Self::read_body(&mut file)
    }

    fn write_body<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.min_ts.to_le_bytes())?;
        w.write_all(&self.max_ts.to_le_bytes())?;
        w.write_all(&self.stride.to_le_bytes())?;

        w.write_all(&(self.keys.len() as u32).to_le_bytes())?;
        for k in &self.keys {
            w.write_all(&k.to_le_bytes())?;
        }

        w.write_all(&(self.fences.len() as u32).to_le_bytes())?;
        for (ts, row) in &self.fences {
            w.write_all(&ts.to_le_bytes())?;
            w.write_all(&row.to_le_bytes())?;
        }
        Ok(())
    }

    fn read_body<R: Read>(r: &mut R) -> std::io::Result<Self> {
        fn read_i64<RR: Read>(r: &mut RR) -> std::io::Result<i64> {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(i64::from_le_bytes(b))
        }
        fn read_u64<RR: Read>(r: &mut RR) -> std::io::Result<u64> {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(u64::from_le_bytes(b))
        }
        fn read_u32<RR: Read>(r: &mut RR) -> std::io::Result<u32> {
            let mut b = [0u8; 4];
            r.read_exact(&mut b)?;
            Ok(u32::from_le_bytes(b))
        }

        let min_ts = read_i64(r)?;
        let max_ts = read_i64(r)?;
        let stride = read_i64(r)?;
        let key_len = read_u32(r)? as usize;
        let mut keys = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            keys.push(read_u64(r)?);
        }
        let fence_len = read_u32(r)? as usize;
        let mut fences = Vec::with_capacity(fence_len);
        for _ in 0..fence_len {
            let ts = read_i64(r)?;
            let row = read_u32(r)?;
            fences.push((ts, row));
        }
        Ok(Self {
            min_ts,
            max_ts,
            stride,
            keys,
            fences,
        })
    }

    /// Save a per-field slab file that contains all zone temporal indexes for the field.
    pub fn save_field_slab(
        uid: &str,
        field: &str,
        dir: &Path,
        entries: &[(u32, &ZoneTemporalIndex)],
    ) -> std::io::Result<()> {
        let path = dir.join(format!("{}_{}.tfi", uid, field));
        let file = std::fs::File::create(&path)?;
        let mut w = BufWriter::with_capacity(256 * 1024, file);

        // Header: TemporalIndex magic, version 2 for slab format
        let header = BinaryHeader::new(FileKind::TemporalIndex.magic(), 2, 0);
        header.write_to(&mut w)?;

        // Pre-serialize bodies to compute offsets
        let mut bodies: Vec<(u32, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (zid, zti) in entries.iter() {
            let mut buf = Vec::with_capacity(64 + zti.keys.len() * 8 + zti.fences.len() * 12);
            zti.write_body(&mut buf)?;
            bodies.push((*zid, buf));
        }

        // Write directory
        let count = bodies.len() as u32;
        w.write_all(&count.to_le_bytes())?;
        let dir_start = BinaryHeader::TOTAL_LEN as u64 + 4 + (count as u64) * (4 + 8 + 4);
        let mut offset = dir_start;
        for (zid, buf) in &bodies {
            w.write_all(&zid.to_le_bytes())?;
            w.write_all(&offset.to_le_bytes())?;
            let len_u32 = buf.len() as u32;
            w.write_all(&len_u32.to_le_bytes())?;
            offset += len_u32 as u64;
        }

        // Write bodies
        for (_zid, buf) in &bodies {
            w.write_all(buf)?;
        }

        w.flush()
    }
}

impl ZoneRangeIndex<i64> for ZoneTemporalIndex {
    fn may_match(&self, op: CompareOp, v: i64) -> bool {
        match op {
            CompareOp::Eq => self.contains_ts(v),
            CompareOp::Neq => {
                // If there exists any value, then Neq(v) may match unless the only value equals v.
                if self.min_ts > self.max_ts {
                    return false;
                }
                if self.min_ts == self.max_ts {
                    return self.min_ts != v;
                }
                true
            }
            CompareOp::Gt => v < self.max_ts,
            CompareOp::Gte => v <= self.max_ts,
            CompareOp::Lt => v > self.min_ts,
            CompareOp::Lte => v >= self.min_ts,
            CompareOp::In => {
                // IN operations require multiple values and should be handled at a higher level
                // (e.g., in filter plan or condition evaluator) where the full list is available.
                // This method only accepts a single value, so IN should not reach here.
                unreachable!("IN operations should not use single-value may_match")
            }
        }
    }

    fn may_match_range(&self, min: i64, max: i64) -> bool {
        if max < min {
            return false;
        }
        // Overlap of [min,max] with [self.min_ts, self.max_ts]
        !(max < self.min_ts || min > self.max_ts)
    }
}
