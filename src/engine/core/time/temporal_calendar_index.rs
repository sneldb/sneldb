use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::command::types::{CompareOp, TimeGranularity};
use crate::engine::core::time::temporal_traits::FieldIndex;
use crate::shared::datetime::time_bucketing::naive_bucket_of;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::io::{Read, Write};
use std::path::Path;

/// Per-field temporal calendar index mapping time buckets to candidate zone ids.
#[derive(Debug, Default, Clone)]
pub struct TemporalCalendarIndex {
    pub field: String,
    pub day: HashMap<u32, RoaringBitmap>,
    pub hour: HashMap<u32, RoaringBitmap>,
}

impl TemporalCalendarIndex {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            day: HashMap::new(),
            hour: HashMap::new(),
        }
    }

    #[inline]
    fn bucket_id(ts: u64, gran: TimeGranularity) -> u32 {
        let start = naive_bucket_of(ts, &gran);
        (start & u32::MAX as u64) as u32
    }

    /// Record that the zone covers the inclusive timestamp range [min_ts, max_ts].
    pub fn add_zone_range(&mut self, zone_id: u32, min_ts: u64, max_ts: u64) {
        // Hour buckets
        let mut t = naive_bucket_of(min_ts, &TimeGranularity::Hour);
        let end = naive_bucket_of(max_ts, &TimeGranularity::Hour);
        while t <= end {
            let b = Self::bucket_id(t, TimeGranularity::Hour);
            self.hour.entry(b).or_default().insert(zone_id);
            t += 3600;
        }
        // Day buckets
        let mut td = naive_bucket_of(min_ts, &TimeGranularity::Day);
        let end_day = naive_bucket_of(max_ts, &TimeGranularity::Day);
        while td <= end_day {
            let b = Self::bucket_id(td, TimeGranularity::Day);
            self.day.entry(b).or_default().insert(zone_id);
            td += 86_400;
        }
    }

    fn zones_for_ts(&self, ts: u64) -> RoaringBitmap {
        // Prefer hour, then day
        let hb = Self::bucket_id(ts, TimeGranularity::Hour);
        if let Some(bm) = self.hour.get(&hb) {
            return bm.clone();
        }
        let db = Self::bucket_id(ts, TimeGranularity::Day);
        if let Some(bm) = self.day.get(&db) {
            return bm.clone();
        }
        RoaringBitmap::new()
    }

    fn zones_for_range(&self, min_ts: u64, max_ts: u64) -> RoaringBitmap {
        if max_ts < min_ts {
            return RoaringBitmap::new();
        }
        // Union only existing day buckets between bounds
        let start_b = Self::bucket_id(min_ts, TimeGranularity::Day);
        let end_b = Self::bucket_id(max_ts, TimeGranularity::Day);
        let mut out = RoaringBitmap::new();
        for (bucket, bm) in &self.day {
            if *bucket >= start_b && *bucket <= end_b {
                out |= bm;
            }
        }
        out
    }

    fn zones_for_ge(&self, min_ts: u64) -> RoaringBitmap {
        let start_b = Self::bucket_id(min_ts, TimeGranularity::Day);
        let mut out = RoaringBitmap::new();
        for (bucket, bm) in &self.day {
            if *bucket >= start_b {
                out |= bm;
            }
        }
        out
    }

    fn zones_for_le(&self, max_ts: u64) -> RoaringBitmap {
        let end_b = Self::bucket_id(max_ts, TimeGranularity::Day);
        let mut out = RoaringBitmap::new();
        for (bucket, bm) in &self.day {
            if *bucket <= end_b {
                out |= bm;
            }
        }
        out
    }
}

impl FieldIndex<i64> for TemporalCalendarIndex {
    fn zones_intersecting(&self, op: CompareOp, v: i64) -> RoaringBitmap {
        match op {
            CompareOp::Eq => {
                if v < 0 {
                    return RoaringBitmap::new();
                }
                self.zones_for_ts(v as u64)
            }
            CompareOp::Gt | CompareOp::Gte => {
                if v < 0 {
                    return RoaringBitmap::new();
                }
                self.zones_for_ge(v as u64)
            }
            CompareOp::Lt | CompareOp::Lte => {
                if v < 0 {
                    return RoaringBitmap::new();
                }
                self.zones_for_le(v as u64)
            }
            CompareOp::Neq => {
                // Fallback: union all known zones then subtract Eq(v) if needed
                let mut all = RoaringBitmap::new();
                for bm in self.day.values() {
                    all |= bm;
                }
                let eq = if v < 0 {
                    RoaringBitmap::new()
                } else {
                    self.zones_for_ts(v as u64)
                };
                &all - &eq
            }
        }
    }

    fn zones_intersecting_range(&self, min: i64, max: i64) -> RoaringBitmap {
        if max < min {
            return RoaringBitmap::new();
        }
        let min_u = if min < 0 { 0 } else { min as u64 };
        let max_u = if max < 0 { 0 } else { max as u64 };
        self.zones_for_range(min_u, max_u)
    }
}

impl TemporalCalendarIndex {
    pub fn save(&self, uid: &str, segment_dir: &Path) -> std::io::Result<()> {
        let path = segment_dir.join(format!("{}_{}.cal", uid, self.field));
        let mut file = std::fs::File::create(&path)?;
        let header = BinaryHeader::new(FileKind::CalendarDir.magic(), 1, 0);
        header.write_to(&mut file)?;

        fn write_map<W: Write>(
            w: &mut W,
            map: &HashMap<u32, RoaringBitmap>,
        ) -> std::io::Result<()> {
            w.write_all(&(map.len() as u32).to_le_bytes())?;
            for (bucket, bm) in map.iter() {
                w.write_all(&bucket.to_le_bytes())?;
                let count = bm.len() as u32;
                w.write_all(&count.to_le_bytes())?;
                for id in bm.iter() {
                    let id_u32: u32 = id.try_into().unwrap_or(u32::MAX);
                    w.write_all(&id_u32.to_le_bytes())?;
                }
            }
            Ok(())
        }

        write_map(&mut file, &self.hour)?;
        write_map(&mut file, &self.day)?;
        Ok(())
    }

    pub fn load(uid: &str, field: &str, segment_dir: &Path) -> std::io::Result<Self> {
        let path = segment_dir.join(format!("{}_{}.cal", uid, field));
        let mut file = std::fs::File::open(&path)?;
        let _ = BinaryHeader::read_from(&mut file)?;

        fn read_map<R: Read>(r: &mut R) -> std::io::Result<HashMap<u32, RoaringBitmap>> {
            let mut u32buf = [0u8; 4];
            r.read_exact(&mut u32buf)?;
            let count = u32::from_le_bytes(u32buf) as usize;
            let mut out = HashMap::with_capacity(count);
            for _ in 0..count {
                r.read_exact(&mut u32buf)?;
                let bucket = u32::from_le_bytes(u32buf);
                r.read_exact(&mut u32buf)?;
                let zcount = u32::from_le_bytes(u32buf) as usize;
                let mut bm = RoaringBitmap::new();
                for _ in 0..zcount {
                    r.read_exact(&mut u32buf)?;
                    let id = u32::from_le_bytes(u32buf);
                    bm.insert(id as u32);
                }
                out.insert(bucket, bm);
            }
            Ok(out)
        }

        let hour = read_map(&mut file)?;
        let day = read_map(&mut file)?;
        Ok(Self {
            field: field.to_string(),
            day,
            hour,
        })
    }
}
