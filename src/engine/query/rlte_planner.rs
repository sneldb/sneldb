use std::collections::HashMap;
use std::path::PathBuf;

use crate::command::types::PickedZones;
use crate::engine::core::read::cache::GlobalIndexCatalogCache;
use crate::engine::core::read::catalog::IndexKind;
use crate::engine::core::read::query_plan::QueryPlan;
use crate::engine::core::zone::rlte_index::RlteIndex;
use tracing::debug;

// ──────────────────────────────────────────────────────────────────────────────
// Small structs to keep catalog state
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct ZoneKey {
    shard_id: usize,
    segment_id: String,
    zone_id: u32,
}

struct CatalogField {
    // (zone key, ladder values as strings, descending in file but we won't assume)
    ladders: Vec<(ZoneKey, Vec<String>)>,
}

struct RlteCatalog {
    fields: HashMap<String, CatalogField>,
}

impl RlteCatalog {
    fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    fn add_ladders(
        &mut self,
        shard_id: usize,
        segment_id: &str,
        field: String,
        ladders: &HashMap<u32, Vec<String>>,
    ) {
        let entry = self.fields.entry(field).or_insert(CatalogField {
            ladders: Vec::new(),
        });
        for (zid, ladder) in ladders.iter() {
            entry.ladders.push((
                ZoneKey {
                    shard_id,
                    segment_id: segment_id.to_string(),
                    zone_id: *zid,
                },
                ladder.clone(),
            ));
        }
    }

    fn zones_with_field(&self, field: &str) -> usize {
        self.fields
            .get(field)
            .map(|cf| cf.ladders.len())
            .unwrap_or(0)
    }

    // ── numeric helpers ───────────────────────────────────────────────────────

    /// Parse ladder into sorted numeric values (u64). Returns empty vec if none parse.
    fn ladder_as_numbers(ladder: &[String]) -> Vec<u64> {
        let mut nums: Vec<u64> = Vec::with_capacity(ladder.len());
        for v in ladder {
            if let Ok(n) = v.parse::<u64>() {
                nums.push(n);
            }
        }
        nums.sort_unstable();
        nums
    }

    #[inline]
    fn lb_ub_one_numeric(ladder: &[String], t: u64, asc: bool, zone_size: usize) -> (usize, usize) {
        if ladder.is_empty() {
            return (0, 0);
        }
        let nums = Self::ladder_as_numbers(ladder);
        if nums.is_empty() {
            return (0, 0);
        }
        let min = nums[0];
        let max = *nums.last().unwrap();

        if asc {
            // ORDER BY ASC — we want values <= t to surface first
            if min > t {
                // whole zone is > t → contributes 0 to the top side
                return (0, 0);
            }
            if max <= t {
                // whole zone is <= t → we will exhaust this zone before crossing t
                return (zone_size, zone_size);
            }
            // Mixed: estimate fraction <= t from ladder checkpoints
            let pos = nums.partition_point(|&x| x <= t); // count of <= t
            let est = ((pos as f64 / nums.len() as f64) * zone_size as f64) as usize;
            let est = est.min(zone_size);
            (est, est)
        } else {
            // ORDER BY DESC — we want values >= t to surface first
            if max < t {
                // whole zone < t → 0
                return (0, 0);
            }
            if min >= t {
                // whole zone >= t → full zone
                return (zone_size, zone_size);
            }
            // Mixed: estimate fraction >= t
            let first_ge = nums.partition_point(|&x| x < t);
            let cnt = nums.len().saturating_sub(first_ge);
            let est = ((cnt as f64 / nums.len() as f64) * zone_size as f64) as usize;
            let est = est.min(zone_size);
            (est, est)
        }
    }

    /// Extract (min, max) numeric envelope for a ladder, if any numeric exists.
    fn min_max_numeric(ladder: &[String]) -> Option<(u64, u64)> {
        let nums = Self::ladder_as_numbers(ladder);
        if nums.is_empty() {
            None
        } else {
            Some((nums[0], *nums.last().unwrap()))
        }
    }

    fn per_zone_bounds_numeric(
        &self,
        field: &str,
        t: u64,
        asc: bool,
        zone_size: usize,
    ) -> Vec<(ZoneKey, (usize, usize), String /*rank1*/)> {
        let mut out = Vec::new();
        if let Some(cf) = self.fields.get(field) {
            for (zk, ladder) in &cf.ladders {
                let (lb, ub) = Self::lb_ub_one_numeric(ladder, t, asc, zone_size);
                let rank1 = ladder.get(0).cloned().unwrap_or_default();
                out.push((zk.clone(), (lb, ub), rank1));
            }
        }
        out
    }

    // ── string fallback helpers ───────────────────────────────────────────────

    #[inline]
    fn lb_ub_one_string(ladder: &[String], t: &str, asc: bool, zone_size: usize) -> (usize, usize) {
        // Geometric checkpoints (1,2,4,8,...) based on ladder order.
        // We do not assume perfect monotonicity, but use string compare consistently.
        let mut r = 1usize;
        let mut lb = 0usize;
        let mut ub = 0usize;
        for v in ladder {
            let ord = if asc {
                // ASC → want <= t
                t.cmp(v.as_str())
            } else {
                // DESC → want >= t
                v.as_str().cmp(t)
            };
            match ord {
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                    lb = r;
                }
                std::cmp::Ordering::Less => {
                    ub = r.saturating_sub(1);
                    break;
                }
            }
            r <<= 1;
        }
        if ub == 0 {
            ub = r.saturating_sub(1);
        }
        (lb.min(zone_size), ub.min(zone_size))
    }

    fn per_zone_bounds_string(
        &self,
        field: &str,
        t: &str,
        asc: bool,
        zone_size: usize,
    ) -> Vec<(ZoneKey, (usize, usize), String /*rank1*/)> {
        let mut out = Vec::new();
        if let Some(cf) = self.fields.get(field) {
            for (zk, ladder) in &cf.ladders {
                let (lb, ub) = Self::lb_ub_one_string(ladder, t, asc, zone_size);
                let rank1 = ladder.get(0).cloned().unwrap_or_default();
                out.push((zk.clone(), (lb, ub), rank1));
            }
        }
        out
    }

    /// Returns the ladder for a given (field, zone) if present.
    fn get_ladder_for_zone(&self, field: &str, zk: &ZoneKey) -> Option<&Vec<String>> {
        self.fields
            .get(field)
            .and_then(|cf| cf.ladders.iter().find(|(k, _)| k == zk))
            .map(|(_, ladder)| ladder)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Load helpers
// ──────────────────────────────────────────────────────────────────────────────

fn load_rlte_for_shard(
    uid: &str,
    _shard_id: usize,
    base_dir: &PathBuf,
    segment_ids: &[String],
) -> Vec<(
    String,                    /*field*/
    String,                    /*segment_id*/
    HashMap<u32, Vec<String>>, /*ladders*/
)> {
    let mut out = Vec::new();
    for seg in segment_ids {
        // Check catalog whether RLTE is present for this segment/uid
        if let Ok((catalog, _)) =
            GlobalIndexCatalogCache::instance().get_or_load(base_dir, seg, uid)
        {
            if !catalog.global_kinds.contains(IndexKind::RLTE) {
                continue;
            }
            let seg_dir = base_dir.join(seg);
            if let Ok(idx) = RlteIndex::load(uid, &seg_dir) {
                for (f, m) in idx.ladders.into_iter() {
                    out.push((f, seg.clone(), m));
                }
            }
        }
    }
    out
}

// ──────────────────────────────────────────────────────────────────────────────
// Greedy cutoff (Option A): use per-zone numeric envelopes, accumulate UBs
// until we cover k, then pick a threshold t* from the frontier zone.
// If numeric is impossible, fall back to string heuristic with same shape.
// ──────────────────────────────────────────────────────────────────────────────

/// For DESC: we want largest first. Sort zones by `max` desc; accumulate.
/// For ASC: we want smallest first. Sort zones by `min` asc; accumulate.
fn greedy_cutoff_numeric(
    catalog: &RlteCatalog,
    field: &str,
    asc: bool,
    k: usize,
    zone_size: usize,
) -> Option<(
    u64,
    Vec<(ZoneKey, (usize, usize), String)>,
    u64, /*t_star*/
)> {
    let cf = catalog.fields.get(field)?;

    // Collect (zone, min, max) where ladder has numeric data.
    let mut envs: Vec<(ZoneKey, u64, u64, Vec<String>)> = Vec::new();
    for (zk, ladder) in &cf.ladders {
        if let Some((min, max)) = RlteCatalog::min_max_numeric(ladder) {
            envs.push((zk.clone(), min, max, ladder.clone()));
        }
    }
    if envs.is_empty() {
        return None;
    }

    // Sort for the direction we need
    if asc {
        // ASC: smallest first → sort by min ASC, tie-break by max ASC
        envs.sort_by(|a, b| {
            let o = a.1.cmp(&b.1);
            if o == std::cmp::Ordering::Equal {
                a.2.cmp(&b.2)
            } else {
                o
            }
        });
    } else {
        // DESC: largest first → sort by max DESC, tie-break by min DESC
        envs.sort_by(|a, b| {
            let o = b.2.cmp(&a.2);
            if o == std::cmp::Ordering::Equal {
                b.1.cmp(&a.1)
            } else {
                o
            }
        });
    }

    // Greedily accumulate per-zone UB at the frontier `t`.
    // For DESC: frontier at the max of the first chosen zone
    // For ASC:  frontier at the min of the first chosen zone
    let mut cum_ub = 0usize;
    let mut picked: Vec<(ZoneKey, u64 /*min*/, u64 /*max*/, Vec<String>)> = Vec::new();

    for (_i, (zk, min, max, ladder)) in envs.iter().enumerate() {
        // For ASC, frontier should be at the zone's min (smallest values first);
        // For DESC, frontier is at the zone's max (largest values first).
        let t_new = if asc { *min } else { *max }; // t at frontier for this zone
        let (_lb, ub) = RlteCatalog::lb_ub_one_numeric(ladder, t_new, asc, zone_size);

        debug!(target: "rlte::planner",
            "greedy_step({}): add zone shard={} seg={} zone={} max={:?} min={:?} -> t_new={} cum_ub_before={} cum_ub_after={} K={}",
            if asc {"asc"} else {"desc"},
            zk.shard_id, zk.segment_id, zk.zone_id, Some(max), Some(min), t_new, cum_ub, cum_ub.saturating_add(ub), k
        );

        cum_ub = cum_ub.saturating_add(ub);
        picked.push((zk.clone(), *min, *max, ladder.clone()));
        if cum_ub >= k {
            // The moment we can satisfy k with envelopes
            // Pick t* as the boundary of this frontier zone
            let t_star = t_new;

            let sample: Vec<(u64, u64)> = envs
                .iter()
                .take(10)
                .map(|(_, min, max, _)| (*max, *min))
                .collect();
            debug!(target: "rlte::planner",
                "greedy_{} summary: zones={} picked={} cum_ub={} t_star={} top10 [(max,min)]={:?}",
                if asc {"asc"} else {"desc"},
                envs.len(),
                picked.len(),
                cum_ub,
                t_star,
                sample
            );

            // Now compute actual per-zone bounds at t_star and keep ub>0 zones
            let bounds_all = catalog.per_zone_bounds_numeric(field, t_star, asc, zone_size);
            let total_zones = bounds_all.len();
            let mut candidates = bounds_all
                .into_iter()
                .filter(|(_, (_lb, ub), _)| *ub > 0)
                .collect::<Vec<_>>();

            // Sort kept zones by rank1 (string) to be deterministic: ASC → rank1 ASC, DESC → rank1 DESC
            candidates.sort_by(|a, b| if asc { a.2.cmp(&b.2) } else { b.2.cmp(&a.2) });

            // Log a quick diagnosis using some zones that were near the frontier
            for (zk, min, max, ladder) in picked.iter().take(16) {
                let (lb, ub) = RlteCatalog::lb_ub_one_numeric(ladder, t_star, asc, zone_size);
                debug!(target: "rlte::planner",
                    "zone_sample: shard={} seg={} zone={} rank1='{}' min={:?} max={:?} -> lb={} ub={}",
                    zk.shard_id, zk.segment_id, zk.zone_id,
                    ladder.get(0).cloned().unwrap_or_default(),
                    Some(*min), Some(*max),
                    lb, ub
                );
            }

            let kept = candidates.len();
            let pruned = total_zones.saturating_sub(kept);
            debug!(target: "rlte::planner",
                "Zone diagnostics: total={} kept={} pruned={} t_star={} ({})",
                total_zones, kept, pruned, t_star, if asc {"asc"} else {"desc"}
            );

            {
                let mut lines: Vec<String> = Vec::new();
                lines.push(format!(
                    "RLTE plan: field={} asc={} cutoff='{}' k={} total_zones={} kept={} pruned={}",
                    field, asc, t_star, k, total_zones, kept, pruned
                ));
                lines.push("Chosen zones (ordered by rank-1, sampled):".to_string());
                for (i, (zk, (lb, ub), rank1)) in candidates.iter().take(10).enumerate() {
                    lines.push(format!(
                        "  [{:02}] shard={} segment={} zone={} lb={} ub={} rank1='{}' (kept because ub>0)",
                        i, zk.shard_id, zk.segment_id, zk.zone_id, lb, ub, rank1
                    ));
                }
                debug!(target: "rlte::planner", "{}", lines.join("\n"));
            }

            return Some((t_star, candidates, t_star));
        }
    }

    None
}

/// String fallback: use per-zone string first/last as envelope:
/// - For DESC use zone's "max" ~ lexicographically largest token we saw
/// - For ASC  use zone's "min" ~ lexicographically smallest token
/// Greedily accumulate UBs the same way.
fn greedy_cutoff_string(
    catalog: &RlteCatalog,
    field: &str,
    asc: bool,
    k: usize,
    zone_size: usize,
) -> Option<(
    String,
    Vec<(ZoneKey, (usize, usize), String)>,
    String, /*t_star*/
)> {
    let cf = catalog.fields.get(field)?;

    // Prepare "envelopes" using string order:
    // We take ladder sorted ASC lexicographically and use first/last
    let mut envs: Vec<(
        ZoneKey,
        String, /*min*/
        String, /*max*/
        Vec<String>,
    )> = Vec::new();
    for (zk, ladder) in &cf.ladders {
        if ladder.is_empty() {
            continue;
        }
        let mut svals = ladder.clone();
        svals.sort();
        let min = svals.first().cloned().unwrap();
        let max = svals.last().cloned().unwrap();
        envs.push((zk.clone(), min, max, ladder.clone()));
    }
    if envs.is_empty() {
        return None;
    }

    if asc {
        // ASC → smallest first
        envs.sort_by(|a, b| {
            let o = a.1.cmp(&b.1); // min ASC
            if o == std::cmp::Ordering::Equal {
                a.2.cmp(&b.2) // max ASC
            } else {
                o
            }
        });
    } else {
        // DESC → largest first
        envs.sort_by(|a, b| {
            let o = b.2.cmp(&a.2); // max DESC
            if o == std::cmp::Ordering::Equal {
                b.1.cmp(&a.1) // min DESC
            } else {
                o
            }
        });
    }

    let mut cum_ub = 0usize;
    let mut picked: Vec<(ZoneKey, String, String, Vec<String>)> = Vec::new();

    for (_i, (zk, min, max, ladder)) in envs.iter().enumerate() {
        let t_new = if asc { max } else { min };
        let (_lb, ub) = RlteCatalog::lb_ub_one_string(ladder, t_new, asc, zone_size);

        debug!(target: "rlte::planner",
            "greedy_step(str, {}): add zone shard={} seg={} zone={} max='{}' min='{}' -> t_new='{}' cum_ub_before={} cum_ub_after={} K={}",
            if asc {"asc"} else {"desc"},
            zk.shard_id, zk.segment_id, zk.zone_id, max, min, t_new, cum_ub, cum_ub.saturating_add(ub), k
        );

        cum_ub = cum_ub.saturating_add(ub);
        picked.push((zk.clone(), min.clone(), max.clone(), ladder.clone()));
        if cum_ub >= k {
            let t_star = t_new.clone();

            // Convert to final per-zone bounds @ t_star
            let bounds_all = catalog.per_zone_bounds_string(field, &t_star, asc, zone_size);
            let total_zones = bounds_all.len();
            let mut candidates = bounds_all
                .into_iter()
                .filter(|(_, (_lb, ub), _)| *ub > 0)
                .collect::<Vec<_>>();
            candidates.sort_by(|a, b| if asc { a.2.cmp(&b.2) } else { b.2.cmp(&a.2) });

            for (zk, min, max, ladder) in picked.iter().take(16) {
                let (lb, ub) = RlteCatalog::lb_ub_one_string(ladder, &t_star, asc, zone_size);
                debug!(target: "rlte::planner",
                    "zone_sample(str): shard={} seg={} zone={} rank1='{}' min='{}' max='{}' -> lb={} ub={}",
                    zk.shard_id, zk.segment_id, zk.zone_id,
                    ladder.get(0).cloned().unwrap_or_default(),
                    min, max, lb, ub
                );
            }

            let kept = candidates.len();
            let pruned = total_zones.saturating_sub(kept);
            debug!(target: "rlte::planner",
                "Zone diagnostics(str): total={} kept={} pruned={} t_star='{}' ({})",
                total_zones, kept, pruned, t_star, if asc {"asc"} else {"desc"}
            );

            {
                let mut lines: Vec<String> = Vec::new();
                lines.push(format!(
                    "RLTE plan: field={} asc={} cutoff='{}' k={} total_zones={} kept={} pruned={}",
                    field, asc, t_star, k, total_zones, kept, pruned
                ));
                lines.push("Chosen zones (ordered by rank-1, sampled):".to_string());
                for (i, (zk, (lb, ub), rank1)) in candidates.iter().take(10).enumerate() {
                    lines.push(format!(
                        "  [{:02}] shard={} segment={} zone={} lb={} ub={} rank1='{}' (kept because ub>0)",
                        i, zk.shard_id, zk.segment_id, zk.zone_id, lb, ub, rank1
                    ));
                }
                debug!(target: "rlte::planner", "{}", lines.join("\n"));
            }

            return Some((t_star.clone(), candidates, t_star));
        }
    }

    None
}

// ──────────────────────────────────────────────────────────────────────────────
// Public planner entry
// ──────────────────────────────────────────────────────────────────────────────

pub struct PlannerOutput {
    pub per_shard: HashMap<usize, PickedZones>,
}

pub async fn plan_with_rlte(
    plan: &QueryPlan,
    shard_bases: &HashMap<usize, PathBuf>,
    shard_segments: &HashMap<usize, Vec<String>>,
) -> Option<PlannerOutput> {
    let Some(ob) = plan.order_by() else {
        return None;
    };
    let asc = !ob.desc;
    let field = ob.field.clone();
    let k_user = plan.limit().unwrap_or(0) + plan.offset().unwrap_or(0);
    let k = k_user.saturating_mul(10);
    if k == 0 {
        return None;
    }
    let uid = plan.event_type_uid().await?;

    // Load RLTE and build catalog
    let mut catalog = RlteCatalog::new();
    let mut loaded_fields = 0usize;
    for (shard_id, base) in shard_bases {
        let segs = shard_segments.get(shard_id).cloned().unwrap_or_default();
        for (f, seg_id, ladders) in load_rlte_for_shard(&uid, *shard_id, base, &segs) {
            loaded_fields += 1;
            catalog.add_ladders(*shard_id, &seg_id, f, &ladders);
        }
    }
    let zones_with_field = catalog.zones_with_field(&field);
    let zone_size = crate::shared::config::CONFIG.engine.event_per_zone;
    debug!(target: "rlte::planner",
        "RLTE load summary: uid={} field='{}' asc={} k={} zone_size={} rlte_fields_loaded={} zones_with_field={}",
        uid, field, asc, k, zone_size, loaded_fields, zones_with_field
    );

    if zones_with_field == 0 {
        return None;
    }

    // Try numeric greedy first
    let plan_numeric = greedy_cutoff_numeric(&catalog, &field, asc, k, zone_size);
    let (mut candidates, t_star_str, used_numeric) = if let Some((_t, candidates, t_star)) =
        plan_numeric
    {
        debug!(target: "rlte::planner",
            "Cutoff chosen: field='{}' asc={} t_star='{}' k={} zone_size={} (numeric-greedy)",
            field, asc, t_star, k, zone_size
        );
        (candidates, t_star.to_string(), true)
    } else {
        // String fallback greedy
        let (_ts, candidates, t_star) = greedy_cutoff_string(&catalog, &field, asc, k, zone_size)?;
        debug!(target: "rlte::planner",
            "Cutoff chosen: field='{}' asc={} t_star='{}' k={} zone_size={} (string-greedy)",
            field, asc, t_star, k, zone_size
        );
        (candidates, t_star, false)
    };

    // Optional: refine candidates using a simple numeric WHERE bound on the same field
    if let Some(bound) = WhereBound::from_plan_field(plan, &field) {
        candidates.retain(|(zk, _bounds, _rank1)| {
            if let Some(ladder) = catalog.get_ladder_for_zone(&field, zk) {
                if let Some((min, max)) = RlteCatalog::min_max_numeric(ladder) {
                    return bound.keep_zone(min, max);
                }
            }
            true
        });
        if candidates.is_empty() {
            // No viable zones remain after applying WHERE → skip RLTE
            return None;
        }
    }

    // Partition by shard → PickedZones (what the worker will honor)
    let mut per_shard: HashMap<usize, PickedZones> = HashMap::new();
    for (zk, _bounds, _rank1) in candidates {
        let entry = per_shard.entry(zk.shard_id).or_insert(PickedZones {
            uid: uid.clone(),
            field: field.clone(),
            asc,
            cutoff: t_star_str.clone(),
            k,
            zones: Vec::new(),
        });
        entry.zones.push((zk.segment_id.clone(), zk.zone_id));
    }

    // Final confirmation log
    debug!(target: "rlte::planner",
        "RLTE planner done: uid={} field='{}' asc={} cutoff='{}' path={} shards={}",
        uid,
        field,
        asc,
        t_star_str,
        if used_numeric { "numeric-greedy" } else { "string-greedy" },
        per_shard.len()
    );

    Some(PlannerOutput { per_shard })
}

// ──────────────────────────────────────────────────────────────────────────────
// WHERE clause helpers (OO-style: encapsulated extractor + predicate)
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Copy, Clone)]
enum NumericBoundKind {
    Lt,
    Lte,
    Gt,
    Gte,
}

struct WhereBound {
    kind: NumericBoundKind,
    value: u64,
}

impl WhereBound {
    fn from_plan_field(plan: &QueryPlan, field: &str) -> Option<Self> {
        use crate::command::types::{CompareOp, Expr};
        let expr = plan.where_clause()?;
        match expr {
            Expr::Compare {
                field: f,
                op,
                value,
            } if f == field => {
                let v = value.as_u64()?;
                let kind = match op {
                    CompareOp::Lt => NumericBoundKind::Lt,
                    CompareOp::Lte => NumericBoundKind::Lte,
                    CompareOp::Gt => NumericBoundKind::Gt,
                    CompareOp::Gte => NumericBoundKind::Gte,
                    _ => return None,
                };
                Some(Self { kind, value: v })
            }
            _ => None,
        }
    }

    #[inline]
    fn keep_zone(&self, min: u64, max: u64) -> bool {
        match self.kind {
            NumericBoundKind::Lt => min < self.value,
            NumericBoundKind::Lte => min <= self.value,
            NumericBoundKind::Gt => max > self.value,
            NumericBoundKind::Gte => max >= self.value,
        }
    }
}
