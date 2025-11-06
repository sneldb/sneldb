use std::path::{Path, PathBuf};

use crate::engine::core::{ColumnKey, Event, UidResolver, ZonePlan};
use crate::engine::types::ScalarValue;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct WriteJob {
    pub key: ColumnKey,
    pub zone_id: u32,
    pub path: PathBuf,
    pub value: ScalarValue,
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
            // Optimization: collect all field names once per zone
            // This is required for columnar storage - all columns must have same row count
            let mut all_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            all_fields.insert("context_id".to_string());
            all_fields.insert("event_type".to_string());
            all_fields.insert("timestamp".to_string());
            all_fields.insert("event_id".to_string());

            for event in &zone_plan.events {
                for key in event.payload.keys() {
                    all_fields.insert(key.clone());
                }
            }

            // Now create jobs for each event using the cached field list
            for event in &zone_plan.events {
                jobs.extend(Self::from_event_with_fields(
                    event,
                    &all_fields,
                    zone_plan.id,
                    segment_dir,
                    resolver,
                ));
            }
        }

        info!(
            target: "sneldb::write_job",
            count = jobs.len(),
            "Write job build complete"
        );

        jobs
    }

    fn from_event_with_fields(
        event: &Event,
        fields: &std::collections::HashSet<String>,
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

        for field in fields {
            let value = event.get_field_scalar(field).unwrap_or(ScalarValue::Null);
            let path = segment_dir.join(format!("{}_{}.col", uid, field));
            let key = (event_type.clone(), field.clone());

            jobs.push(WriteJob {
                key,
                zone_id,
                path,
                value,
            });
        }

        jobs
    }
}
