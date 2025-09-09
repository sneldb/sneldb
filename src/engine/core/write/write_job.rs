use std::path::{Path, PathBuf};

use crate::engine::core::{ColumnKey, Event, UidResolver, ZonePlan};
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct WriteJob {
    pub key: ColumnKey,
    pub zone_id: u32,
    pub path: PathBuf,
    pub value: String,
}

impl WriteJob {
    pub fn build(
        zone_plans: &[ZonePlan],
        segment_dir: &Path,
        resolver: &UidResolver,
    ) -> Vec<WriteJob> {
        let mut jobs = Vec::new();

        info!(
            target: "sneldb::write_job",
            zones = zone_plans.len(),
            "Building write jobs for segment at {:?}",
            segment_dir
        );

        for zone_plan in zone_plans {
            for event in &zone_plan.events {
                jobs.extend(Self::from_event(event, zone_plan.id, segment_dir, resolver));
            }
        }

        info!(
            target: "sneldb::write_job",
            count = jobs.len(),
            "Write job build complete"
        );

        jobs
    }

    fn from_event(
        event: &Event,
        zone_id: u32,
        segment_dir: &Path,
        resolver: &UidResolver,
    ) -> Vec<WriteJob> {
        let mut jobs = Vec::new();
        let event_type = &event.event_type;

        let Some(uid) = resolver.get(event_type) else {
            error!(
                target: "sneldb::write_job",
                %event_type,
                "UID missing for event_type"
            );
            return jobs;
        };

        for field in event.collect_all_fields() {
            let value = event.get_field_value(&field);
            let path = segment_dir.join(format!("{}_{}.col", uid, &field));
            let key = (event_type.clone(), field);

            jobs.push(WriteJob {
                key,
                zone_id,
                path,
                value: value.to_owned(),
            });
        }

        jobs
    }
}
