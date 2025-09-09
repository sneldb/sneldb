use crate::engine::core::{Event, MemTable, MemTableQuery, QueryPlan};
use std::sync::Arc;
use tracing::{debug, info};

pub struct MemTableQueryRunner<'a> {
    memtable: Option<&'a MemTable>,
    passive_memtable: Option<&'a Arc<tokio::sync::Mutex<MemTable>>>,
    plan: &'a QueryPlan,
}

impl<'a> MemTableQueryRunner<'a> {
    pub fn new(
        memtable: Option<&'a MemTable>,
        passive_memtable: Option<&'a Arc<tokio::sync::Mutex<MemTable>>>,
        plan: &'a QueryPlan,
    ) -> Self {
        Self {
            memtable,
            passive_memtable,
            plan,
        }
    }

    pub async fn run(&self) -> Vec<Event> {
        let mut events = Vec::new();

        if let Some(m) = self.memtable {
            let count = m.len();
            debug!(
                target: "sneldb::query_memtable",
                count,
                "Querying active MemTable"
            );
            let found = MemTableQuery::new(m, self.plan).query();
            debug!(
                target: "sneldb::query_memtable",
                found = found.len(),
                "Found events in active MemTable"
            );
            events.extend(found);
        }

        if let Some(pm) = self.passive_memtable {
            debug!(
                target: "sneldb::query_memtable",
                "Acquiring lock for passive MemTable"
            );
            let guard = pm.lock().await;
            let count = guard.len();
            debug!(
                target: "sneldb::query_memtable",
                count,
                "Querying passive MemTable"
            );
            let found = MemTableQuery::new(&*guard, self.plan).query();
            debug!(
                target: "sneldb::query_memtable",
                found = found.len(),
                "Found events in passive MemTable"
            );
            events.extend(found);
        }

        if events.is_empty() {
            info!(
                target: "sneldb::query_memtable",
                "No events found in any MemTable, fallback to disk"
            );
        } else {
            info!(
                target: "sneldb::query_memtable",
                count = events.len(),
                "Total events found in memory"
            );
        }

        events
    }
}
