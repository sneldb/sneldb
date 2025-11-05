use crate::command::handlers::kway_merger::KWayMerger;
use crate::command::handlers::query::context::QueryContext;
use crate::engine::core::read::result::{ColumnSpec, QueryResult, SelectionResult};
use crate::engine::types::ScalarValue;

pub struct OrderedMerger {
    field: String,
    ascending: bool,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl OrderedMerger {
    pub fn new(field: String, ascending: bool, limit: Option<u32>, offset: Option<u32>) -> Self {
        Self {
            field,
            ascending,
            limit,
            offset,
        }
    }

    pub fn merge(
        &self,
        _ctx: &QueryContext<'_>,
        shard_results: Vec<QueryResult>,
    ) -> Result<QueryResult, String> {
        let mut per_shard_rows: Vec<Vec<Vec<ScalarValue>>> = Vec::new();
        let mut columns_opt: Option<Vec<ColumnSpec>> = None;

        for result in shard_results {
            if let QueryResult::Selection(selection) = result {
                let table = selection.finalize();
                if columns_opt.is_none() {
                    columns_opt = Some(table.columns.clone());
                }
                per_shard_rows.push(table.rows);
            }
        }

        if per_shard_rows.is_empty() {
            return Ok(QueryResult::Selection(SelectionResult {
                columns: Vec::new(),
                rows: Vec::new(),
            }));
        }

        // Calculate how many rows we need to merge before applying offset/limit
        // If offset is present, we need to merge at least (offset + limit) rows
        // If only limit is present, we only need to merge limit rows
        // If neither, merge everything
        let merge_limit = match (self.limit, self.offset) {
            (Some(limit), Some(offset)) => limit.saturating_add(offset) as usize,
            (Some(limit), None) => limit as usize,
            (None, Some(_offset)) => usize::MAX, // Need all rows to apply offset
            (None, None) => usize::MAX,
        };
        let effective_limit = if merge_limit == 0 {
            usize::MAX
        } else {
            merge_limit
        };
        let merger = KWayMerger::new(
            &per_shard_rows,
            &self.field,
            self.ascending,
            effective_limit,
        );
        let merged_rows = merger.merge();
        let paginated = KWayMerger::apply_pagination(merged_rows, self.offset, self.limit);

        let columns = columns_opt.unwrap_or_default();
        Ok(QueryResult::Selection(SelectionResult {
            columns,
            rows: paginated,
        }))
    }
}
