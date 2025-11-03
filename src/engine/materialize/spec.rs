use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::command::types::{Command, MaterializedQuerySpec};
use crate::engine::materialize::high_water::HighWaterMark;
use crate::shared::time::{TimeKind, TimeParser};
use serde_json;

use super::MaterializationError;

pub trait MaterializedQuerySpecExt {
    fn alias(&self) -> &str;
    fn query(&self) -> &Command;
    fn cloned_query(&self) -> Command;
    fn plan_hash(&self) -> Result<u64, MaterializationError>;
    fn delta_command(
        &self,
        watermark: Option<HighWaterMark>,
    ) -> Result<Command, MaterializationError>;
}

impl MaterializedQuerySpecExt for MaterializedQuerySpec {
    fn alias(&self) -> &str {
        &self.name
    }

    fn query(&self) -> &Command {
        &self.query
    }

    fn cloned_query(&self) -> Command {
        (*self.query).clone()
    }

    fn plan_hash(&self) -> Result<u64, MaterializationError> {
        let serialized = serde_json::to_vec(&self.query)?;
        let mut hasher = DefaultHasher::new();
        serialized.hash(&mut hasher);
        Ok(hasher.finish())
    }

    fn delta_command(
        &self,
        watermark: Option<HighWaterMark>,
    ) -> Result<Command, MaterializationError> {
        let mut command = self.cloned_query();

        let Some(watermark) = watermark else {
            return Ok(command);
        };

        if watermark.is_zero() {
            return Ok(command);
        }

        let Command::Query { ref mut since, .. } = command else {
            return Ok(command);
        };

        if should_update_since(since.as_deref(), watermark.timestamp) {
            *since = Some(watermark.timestamp.to_string());
        }

        Ok(command)
    }
}

fn should_update_since(existing: Option<&str>, watermark_ts: u64) -> bool {
    match existing.and_then(parse_since_epoch) {
        Some(existing_ts) => existing_ts < watermark_ts,
        None => true,
    }
}

fn parse_since_epoch(value: &str) -> Option<u64> {
    TimeParser::parse_str_to_epoch_seconds(value, TimeKind::DateTime)
        .map(|ts| ts.max(0) as u64)
        .or_else(|| value.parse::<u64>().ok())
}
