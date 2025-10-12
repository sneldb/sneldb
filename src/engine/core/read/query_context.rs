use crate::command::types::{Command, OrderSpec};
use std::collections::HashSet;

/// Encapsulates query-specific configuration extracted from a Command.
///
/// This struct extracts and holds data specific to Query commands,
/// avoiding repeated pattern matching on the Command enum.
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// Set of allowed (segment_id, zone_id) pairs for zone filtering
    pub picked_zones: Option<HashSet<(String, u32)>>,
    /// Ordering specification for results
    pub order_by: Option<OrderSpec>,
}

impl QueryContext {
    /// Creates a QueryContext from a Command.
    ///
    /// If the command is not a Query, returns an empty context.
    pub fn from_command(command: &Command) -> Self {
        match command {
            Command::Query {
                picked_zones,
                order_by,
                ..
            } => Self {
                picked_zones: picked_zones
                    .as_ref()
                    .map(|pz| pz.zones.iter().cloned().collect::<HashSet<(String, u32)>>()),
                order_by: order_by.clone(),
            },
            _ => Self::empty(),
        }
    }

    /// Creates an empty QueryContext with no restrictions or ordering.
    pub fn empty() -> Self {
        Self {
            picked_zones: None,
            order_by: None,
        }
    }

    /// Returns true if the context has zone filtering enabled.
    pub fn has_zone_filter(&self) -> bool {
        self.picked_zones.is_some()
    }

    /// Returns true if the context has an ORDER BY clause.
    pub fn has_ordering(&self) -> bool {
        self.order_by.is_some()
    }

    /// Returns true if limiting should be deferred until after sorting.
    ///
    /// When ORDER BY is present, we need all events to sort correctly,
    /// so limiting should happen after sorting, not during evaluation.
    pub fn should_defer_limit(&self) -> bool {
        self.has_ordering()
    }

    /// Returns the number of zones in the filter, if any.
    pub fn zone_filter_count(&self) -> usize {
        self.picked_zones.as_ref().map_or(0, |pz| pz.len())
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::empty()
    }
}

impl std::fmt::Display for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext(zones={}, order={})",
            self.has_zone_filter(),
            self.has_ordering()
        )
    }
}
