use crate::engine::core::{CandidateZone, LogicalOp};
use tracing::{info, warn};

pub struct ZoneCombiner {
    zones: Vec<Vec<CandidateZone>>,
    op: LogicalOp,
}

impl ZoneCombiner {
    pub fn new(zones: Vec<Vec<CandidateZone>>, op: LogicalOp) -> Self {
        Self { zones, op }
    }

    /// Combine zones using sorted-Vec merges to avoid HashMap churn.
    /// - AND: k-way intersection by key (segment_id, zone_id)
    /// - OR: union with dedup by key
    /// - NOT: unchanged for single input, empty for multi-input (preserving previous behavior)
    pub fn combine(mut self) -> Vec<CandidateZone> {
        if self.zones.is_empty() {
            return vec![];
        }

        if self.zones.len() == 1 {
            let mut only = take_vec(&mut self.zones[0]);
            sort_by_key(&mut only);
            return dedup_sorted(only);
        }

        match self.op {
            LogicalOp::And => self.combine_and_sorted(),
            LogicalOp::Or => self.combine_or_sorted(),
            LogicalOp::Not => {
                // Historical behavior: when multiple inputs and NOT, return empty.
                // For single input we already returned above.
                warn!(target: "sneldb::zone_combiner", "ZoneCombiner: NOT operation not implemented");
                vec![]
            }
        }
    }

    fn combine_or_sorted(&mut self) -> Vec<CandidateZone> {
        let total_in: usize = self.zones.iter().map(|v| v.len()).sum();
        let mut all: Vec<CandidateZone> = Vec::with_capacity(total_in);
        for set in &mut self.zones {
            all.extend(take_vec(set));
        }
        sort_by_key(&mut all);
        let out = dedup_sorted(all);
        info!(
            target: "sneldb::zone_combiner",
            in_count = total_in,
            out_count = out.len(),
            "OR combined with sorted-vec dedup"
        );
        out
    }

    fn combine_and_sorted(&mut self) -> Vec<CandidateZone> {
        // Choose the smallest set as the starting base to minimize work.
        let smallest_idx = self
            .zones
            .iter()
            .enumerate()
            .min_by_key(|(_, v)| v.len())
            .map(|(i, _)| i)
            .unwrap_or(0);

        // Move smallest into base and sort+dedup
        let mut base: Vec<CandidateZone> = take_vec(&mut self.zones[smallest_idx]);
        sort_by_key(&mut base);
        base = dedup_sorted(base);

        info!(
            target: "sneldb::zone_combiner",
            base_count = base.len(),
            "Base size before AND intersection"
        );

        // Intersect with every other set
        for (idx, set) in self.zones.iter_mut().enumerate() {
            if idx == smallest_idx {
                continue;
            }
            if base.is_empty() {
                break;
            }

            let mut other = take_vec(set);
            sort_by_key(&mut other);
            other = dedup_sorted(other);

            base = intersect_sorted(base, &other);
        }

        info!(
            target: "sneldb::zone_combiner",
            out_count = base.len(),
            "AND combined with sorted-vec intersection"
        );
        base
    }
}

#[inline]
fn key_of(zone: &CandidateZone) -> (&str, u32) {
    (zone.segment_id.as_str(), zone.zone_id)
}

#[inline]
fn sort_by_key(zones: &mut [CandidateZone]) {
    zones.sort_by(|a, b| key_of(a).cmp(&key_of(b)));
}

#[inline]
fn dedup_sorted(mut zones: Vec<CandidateZone>) -> Vec<CandidateZone> {
    if zones.len() <= 1 {
        return zones;
    }
    let mut out: Vec<CandidateZone> = Vec::with_capacity(zones.len());
    for z in zones.drain(..) {
        let is_new = match out.last() {
            None => true,
            Some(last) => key_of(last) != key_of(&z),
        };
        if is_new {
            out.push(z);
        }
    }
    out
}

#[inline]
fn intersect_sorted(base: Vec<CandidateZone>, other: &[CandidateZone]) -> Vec<CandidateZone> {
    let mut i = 0usize;
    let mut j = 0usize;
    let mut out: Vec<CandidateZone> = Vec::with_capacity(base.len().min(other.len()));

    while i < base.len() && j < other.len() {
        let kb = key_of(&base[i]);
        let ko = key_of(&other[j]);
        if kb == ko {
            // Keep the representation from base; clone to avoid unsafe moves
            out.push(base[i].clone());
            i += 1;
            j += 1;
        } else if kb < ko {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

#[inline]
fn take_vec(v: &mut Vec<CandidateZone>) -> Vec<CandidateZone> {
    std::mem::take(v)
}
