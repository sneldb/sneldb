use crate::command::types::{Command, OrderSpec, PickedZones};
use std::borrow::Cow;
use std::collections::HashMap;

/// Builds commands for shards with minimal cloning using Cow (Clone-on-Write).
///
/// When picked_zones are not needed, returns a borrowed reference.
/// Only clones and modifies when picked_zones must be injected.
pub struct ShardCommandBuilder<'a> {
    base_cmd: &'a Command,
    picked_zones_map: Option<&'a HashMap<usize, PickedZones>>,
}

impl<'a> ShardCommandBuilder<'a> {
    /// Creates a new builder.
    pub fn new(
        base_cmd: &'a Command,
        picked_zones_map: Option<&'a HashMap<usize, PickedZones>>,
    ) -> Self {
        Self {
            base_cmd,
            picked_zones_map,
        }
    }

    /// Builds a command for a specific shard.
    ///
    /// Returns Cow::Borrowed when no modification is needed,
    /// or Cow::Owned when picked_zones must be injected.
    pub fn build_for_shard(&self, shard_id: usize) -> Cow<'a, Command> {
        // If no picked_zones map, return borrowed reference
        let Some(map) = self.picked_zones_map else {
            return Cow::Borrowed(self.base_cmd);
        };

        // Extract the Command::Query fields
        let Command::Query {
            event_type,
            context_id,
            since,
            time_field,
            sequence_time_field,
            where_clause,
            limit,
            offset,
            order_by,
            picked_zones: _,
            return_fields,
            link_field,
            aggs,
            time_bucket,
            group_by,
            event_sequence,
        } = self.base_cmd
        else {
            // Not a Query command, return borrowed
            return Cow::Borrowed(self.base_cmd);
        };

        // Check if this shard has picked zones
        if let Some(pz) = map.get(&shard_id) {
            // Clone and inject picked_zones
            Cow::Owned(Command::Query {
                event_type: event_type.clone(),
                context_id: context_id.clone(),
                since: since.clone(),
                time_field: time_field.clone(),
                sequence_time_field: sequence_time_field.clone(),
                where_clause: where_clause.clone(),
                limit: *limit,
                offset: *offset,
                order_by: order_by.clone(),
                picked_zones: Some(pz.clone()),
                return_fields: return_fields.clone(),
                link_field: link_field.clone(),
                aggs: aggs.clone(),
                time_bucket: time_bucket.clone(),
                group_by: group_by.clone(),
                event_sequence: event_sequence.clone(),
            })
        } else {
            // Shard has no zones - send empty picked_zones to enforce zero results
            Cow::Owned(Self::build_empty_picked_zones_command(
                self.base_cmd,
                limit,
                offset,
                order_by,
            ))
        }
    }

    /// Creates a command with empty picked_zones to enforce zero results.
    fn build_empty_picked_zones_command(
        base_cmd: &Command,
        limit: &Option<u32>,
        offset: &Option<u32>,
        order_by: &Option<OrderSpec>,
    ) -> Command {
        let Command::Query {
            event_type,
            context_id,
            since,
            time_field,
            sequence_time_field,
            where_clause,
            return_fields,
            link_field,
            aggs,
            time_bucket,
            group_by,
            event_sequence,
            ..
        } = base_cmd
        else {
            return base_cmd.clone();
        };

        let k = limit.unwrap_or(0).saturating_add(offset.unwrap_or(0)) as usize;
        let asc = order_by.as_ref().map(|o| !o.desc).unwrap_or(true);
        let field = order_by
            .as_ref()
            .map(|o| o.field.clone())
            .unwrap_or_default();

        let pz = PickedZones {
            uid: String::new(),
            field,
            asc,
            cutoff: String::new(),
            k,
            zones: Vec::new(),
        };

        Command::Query {
            event_type: event_type.clone(),
            context_id: context_id.clone(),
            since: since.clone(),
            time_field: time_field.clone(),
            sequence_time_field: sequence_time_field.clone(),
            where_clause: where_clause.clone(),
            limit: *limit,
            offset: *offset,
            order_by: order_by.clone(),
            picked_zones: Some(pz),
            return_fields: return_fields.clone(),
            link_field: link_field.clone(),
            aggs: aggs.clone(),
            time_bucket: time_bucket.clone(),
            group_by: group_by.clone(),
            event_sequence: event_sequence.clone(),
        }
    }
}
