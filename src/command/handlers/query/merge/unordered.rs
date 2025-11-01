use crate::command::handlers::query::context::QueryContext;
use crate::engine::core::read::result::QueryResult;

pub struct UnorderedMerger {
    limit: Option<u32>,
    offset: Option<u32>,
}

impl UnorderedMerger {
    pub fn new(limit: Option<u32>, offset: Option<u32>) -> Self {
        Self { limit, offset }
    }

    pub fn merge(
        &self,
        _ctx: &QueryContext<'_>,
        shard_results: Vec<QueryResult>,
    ) -> Result<QueryResult, String> {
        let mut acc: Option<QueryResult> = None;

        for result in shard_results {
            if let Some(current) = &mut acc {
                current.merge(result);
            } else {
                acc = Some(result);
            }
        }

        let mut merged = acc.ok_or_else(|| "No results from shards".to_string())?;

        if let QueryResult::Selection(selection) = &mut merged {
            if let Some(offset) = self.offset {
                let offset = offset as usize;
                if selection.rows.len() > offset {
                    selection.rows.drain(0..offset);
                } else {
                    selection.rows.clear();
                }
            }

            if let Some(limit) = self.limit {
                let limit = limit as usize;
                if selection.rows.len() > limit {
                    selection.rows.truncate(limit);
                }
            }
        }

        Ok(merged)
    }
}
