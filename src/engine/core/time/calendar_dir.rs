use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

use crate::command::types::TimeGranularity;
use crate::shared::datetime::time_bucketing::naive_bucket_of;
use crate::shared::storage_header::{BinaryHeader, FileKind};

#[derive(Debug, Default, Clone)]
pub struct CalendarDir {
    pub day: HashMap<u32, RoaringBitmap>,
    pub hour: HashMap<u32, RoaringBitmap>,
}

#[derive(Debug, Clone, Copy)]
pub enum GranularityPref {
    Hour,
    Day,
}

impl CalendarDir {
    pub fn new() -> Self {
        Self {
            day: HashMap::new(),
            hour: HashMap::new(),
        }
    }

    #[inline]
    fn bucket_id(ts: u64, gran: TimeGranularity) -> u32 {
        let start = naive_bucket_of(ts, &gran);
        (start & u32::MAX as u64) as u32
    }

    pub fn add_zone_range(&mut self, zone_id: u32, min_ts: u64, max_ts: u64) {
        // Hour buckets
        let mut t = naive_bucket_of(min_ts, &TimeGranularity::Hour);
        let end = naive_bucket_of(max_ts, &TimeGranularity::Hour);
        while t <= end {
            let b = Self::bucket_id(t, TimeGranularity::Hour);
            self.hour.entry(b).or_default().insert(zone_id);
            t += 3600;
        }
        // Day buckets: mark both min and max days inclusive, even if same day
        let start_day = naive_bucket_of(min_ts, &TimeGranularity::Day);
        let end_day = naive_bucket_of(max_ts, &TimeGranularity::Day);
        let mut td = start_day;
        while td <= end_day {
            let b = Self::bucket_id(td, TimeGranularity::Day);
            self.day.entry(b).or_default().insert(zone_id);
            td += 86_400;
        }
    }

    pub fn zones_for(&self, ts: u64, pref: GranularityPref) -> RoaringBitmap {
        match pref {
            GranularityPref::Hour => {
                let b = Self::bucket_id(ts, TimeGranularity::Hour);
                self.hour.get(&b).cloned().unwrap_or_default()
            }
            GranularityPref::Day => {
                let b = Self::bucket_id(ts, TimeGranularity::Day);
                self.day.get(&b).cloned().unwrap_or_default()
            }
        }
    }

    pub fn save(&self, uid: &str, segment_dir: &Path) -> std::io::Result<()> {
        let path = segment_dir.join(format!("{}.cal", uid));
        let mut file = std::fs::File::create(&path)?;
        let header = BinaryHeader::new(FileKind::CalendarDir.magic(), 1, 0);
        header.write_to(&mut file)?;

        // Serialize as: counts | [hour entries] | [day entries]
        // hour: u32 count, then for each: u32 bucket | u32 zone_count | [u32 zone_id]*
        // day: same
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

    pub fn load(uid: &str, segment_dir: &Path) -> std::io::Result<Self> {
        let path = segment_dir.join(format!("{}.cal", uid));
        let mut file = std::fs::File::open(&path)?;
        let _ = BinaryHeader::read_from(&mut file)?; // validate

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
        Ok(Self { day, hour })
    }
}
