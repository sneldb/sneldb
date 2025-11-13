use super::group_key::GroupKey;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::command::types::TimeGranularity;
use ahash::RandomState as AHashRandomState;
use std::collections::HashMap;

/// Columnar processing logic for aggregate sink
pub(crate) struct ColumnarProcessor;

impl ColumnarProcessor {
    /// Check if columnar processing can be used for this slice
    pub(crate) fn can_use_columnar_processing(
        specs: &[AggregateOpSpec],
        columns: &HashMap<String, ColumnValues>,
    ) -> bool {
        Self::supports_columnar_aggregators(specs, columns)
    }

    /// Check if all aggregators support columnar processing and required columns are typed
    fn supports_columnar_aggregators(
        specs: &[AggregateOpSpec],
        columns: &HashMap<String, ColumnValues>,
    ) -> bool {
        for spec in specs {
            match spec {
                AggregateOpSpec::CountAll => {
                    // CountAll supports columnar
                    continue;
                }
                AggregateOpSpec::Total { field } | AggregateOpSpec::Avg { field } => {
                    // Sum and Avg support columnar if column is typed_i64
                    if let Some(col) = columns.get(field) {
                        if !col.is_typed_i64() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                _ => {
                    // Other aggregators don't support columnar yet
                    return false;
                }
            }
        }
        true
    }

    /// Process a column slice using columnar/SIMD operations (no grouping case)
    pub(crate) fn process_columnar_slice(
        groups: &mut std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
        specs: &[AggregateOpSpec],
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
    ) {
        let default_key = GroupKey {
            prehash: 0,
            bucket: None,
            groups: Vec::new(),
            groups_str: None, // Lazy - will be computed when needed during finalization
        };

        let entry = groups.entry(default_key).or_insert_with(|| {
            specs.iter().map(|s| AggregatorImpl::from_spec(s)).collect::<Vec<_>>()
        });

        for agg in entry.iter_mut() {
            agg.update_column(start, end, columns);
        }
    }

    /// Process a column slice with grouping using columnar/SIMD operations
    pub(crate) fn process_columnar_slice_with_grouping(
        groups: &mut std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
        specs: &[AggregateOpSpec],
        time_bucket: Option<&TimeGranularity>,
        group_by: Option<&[String]>,
        time_field: &str,
        column_indices: Option<&HashMap<String, usize>>,
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
        group_limit: Option<usize>,
    ) {
        // Step 1: Compute group keys and partition by prehash
        let partitioned_groups =
            Self::compute_and_partition_by_prehash(
                time_bucket,
                group_by,
                time_field,
                column_indices,
                start,
                end,
                columns,
            );

        // Step 2: Process each group's rows using columnar processing
        Self::process_groups_columnarly_by_prehash(
            groups,
            specs,
            partitioned_groups,
            columns,
            group_limit,
        );
    }

    /// Compute group keys for all rows and partition by prehash
    /// Optimized to compute prehash first without allocating GroupKey, only creating
    /// GroupKey when encountering a new unique prehash
    fn compute_and_partition_by_prehash(
        time_bucket: Option<&TimeGranularity>,
        group_by: Option<&[String]>,
        time_field: &str,
        column_indices: Option<&HashMap<String, usize>>,
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
    ) -> HashMap<u64, (GroupKey, Vec<usize>)> {
        let slice_len = end - start;
        let estimated_groups = (slice_len / 8).max(1).min(1000);
        let mut groups: HashMap<u64, (GroupKey, Vec<usize>)> =
            HashMap::with_capacity(estimated_groups);
        let mut prehash_to_key: HashMap<u64, GroupKey> = HashMap::with_capacity(estimated_groups);

        for row_idx in start..end {
            // Compute prehash without allocating GroupKey
            let prehash = GroupKey::compute_prehash_from_columns(
                time_bucket,
                group_by,
                time_field,
                columns,
                column_indices,
                row_idx,
            );

            let estimated_capacity = (slice_len / estimated_groups.max(1)).max(4).min(slice_len);

            groups
                .entry(prehash)
                .and_modify(|(_, row_indices)| {
                    row_indices.push(row_idx);
                })
                .or_insert_with(|| {
                    // Only create GroupKey when we encounter a new unique prehash
                    let key = prehash_to_key.entry(prehash).or_insert_with(|| {
                        GroupKey::from_row_with_indices(
                            time_bucket,
                            group_by,
                            time_field,
                            columns,
                            column_indices,
                            row_idx,
                        )
                    }).clone();

                    let mut row_indices = Vec::with_capacity(estimated_capacity);
                    row_indices.push(row_idx);
                    (key, row_indices)
                });
        }

        // Sort row indices within each group for better cache locality
        for (_, (_, row_indices)) in groups.iter_mut() {
            row_indices.sort_unstable();
        }

        groups
    }

    /// Process each group's rows using columnar processing
    fn process_groups_columnarly_by_prehash(
        groups: &mut std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
        specs: &[AggregateOpSpec],
        partitioned_groups: HashMap<u64, (GroupKey, Vec<usize>)>,
        columns: &HashMap<String, ColumnValues>,
        group_limit: Option<usize>,
    ) {
        for (_prehash, (key, row_indices)) in partitioned_groups {
            // Enforce group limit
            if !groups.contains_key(&key) {
                if let Some(max) = group_limit {
                    if groups.len() >= max {
                        continue;
                    }
                }
            }

            let entry = groups.entry(key).or_insert_with(|| {
                specs.iter().map(|s| AggregatorImpl::from_spec(s)).collect()
            });

            Self::process_contiguous_ranges(&row_indices, entry, columns);
        }
    }

    /// Process contiguous ranges of row indices for a group
    /// Maximizes SIMD efficiency by grouping consecutive row indices
    fn process_contiguous_ranges(
        row_indices: &[usize],
        aggregators: &mut [AggregatorImpl],
        columns: &HashMap<String, ColumnValues>,
    ) {
        if row_indices.is_empty() {
            return;
        }

        let mut range_start = row_indices[0];
        let mut range_end = range_start + 1;

        for i in 1..row_indices.len() {
            if row_indices[i] == row_indices[i - 1] + 1 {
                range_end += 1;
            } else {
                for agg in aggregators.iter_mut() {
                    agg.update_column(range_start, range_end, columns);
                }
                range_start = row_indices[i];
                range_end = range_start + 1;
            }
        }

        // Process the last range
        for agg in aggregators.iter_mut() {
            agg.update_column(range_start, range_end, columns);
        }
    }
}

