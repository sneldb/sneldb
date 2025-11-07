#[cfg(test)]
mod tests {
    use crate::engine::core::CandidateZone;
    use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
    use crate::engine::core::read::sequence::group::ColumnarGrouper;
    use std::collections::HashMap;

    /// Helper to create a test zone with columnar data using CandidateZoneFactory pattern
    fn create_test_zone(
        zone_id: u32,
        segment_id: &str,
        context_ids: &[&str],
        user_ids: &[&str],
        timestamps: &[i64],
    ) -> CandidateZone {
        use crate::engine::core::column::column_values::ColumnValues;
        use crate::engine::core::read::cache::DecompressedBlock;
        use std::sync::Arc;

        let mut values_map: HashMap<String, ColumnValues> = HashMap::new();

        // Helper to create ColumnValues from string slice
        let create_column = |strings: &[&str]| -> ColumnValues {
            let mut bytes: Vec<u8> = Vec::new();
            let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(strings.len());
            for s in strings {
                let start = bytes.len();
                bytes.extend_from_slice(s.as_bytes());
                let len = s.as_bytes().len();
                ranges.push((start, len));
            }
            let block = Arc::new(DecompressedBlock::from_bytes(bytes));
            ColumnValues::new(block, ranges)
        };

        values_map.insert("context_id".to_string(), create_column(context_ids));
        values_map.insert("user_id".to_string(), create_column(user_ids));

        // Timestamps as strings
        let timestamp_strings: Vec<String> = timestamps.iter().map(|t| t.to_string()).collect();
        let timestamp_strs: Vec<&str> = timestamp_strings.iter().map(|s| s.as_str()).collect();
        values_map.insert("timestamp".to_string(), create_column(&timestamp_strs));

        let mut zone = CandidateZone::new(zone_id, segment_id.to_string());
        zone.set_values(values_map);
        zone
    }

    #[test]
    fn test_group_by_user_id() {
        let mut zones_by_type = HashMap::new();

        // Create zones for page_view events
        let page_view_zones = vec![create_test_zone(
            0,
            "seg1",
            &["ctx1", "ctx2"],
            &["user1", "user2"],
            &[1000, 2000],
        )];
        zones_by_type.insert("page_view".to_string(), page_view_zones);

        // Create zones for order_created events
        let order_zones = vec![create_test_zone(
            0,
            "seg1",
            &["ctx3", "ctx4"],
            &["user1", "user2"],
            &[1500, 2500],
        )];
        zones_by_type.insert("order_created".to_string(), order_zones);

        let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
        let groups = grouper.group_zones_by_link_field(&zones_by_type);

        // Should have 2 groups: user1 and user2
        assert_eq!(groups.len(), 2);

        // Check user1 group
        let user1_key = "str:user1";
        assert!(groups.contains_key(user1_key));
        let user1_group = &groups[user1_key];
        assert_eq!(user1_group.rows_by_type.len(), 2); // page_view and order_created
        assert_eq!(user1_group.rows_by_type["page_view"].len(), 1);
        assert_eq!(user1_group.rows_by_type["order_created"].len(), 1);

        // Check user2 group
        let user2_key = "str:user2";
        assert!(groups.contains_key(user2_key));
        let user2_group = &groups[user2_key];
        assert_eq!(user2_group.rows_by_type.len(), 2);
        assert_eq!(user2_group.rows_by_type["page_view"].len(), 1);
        assert_eq!(user2_group.rows_by_type["order_created"].len(), 1);
    }

    #[test]
    fn test_group_sorts_by_timestamp() {
        let mut zones_by_type = HashMap::new();

        // Create zone with events out of order
        let zones = vec![create_test_zone(
            0,
            "seg1",
            &["ctx1", "ctx2", "ctx3"],
            &["user1", "user1", "user1"],
            &[3000, 1000, 2000], // Out of order
        )];
        zones_by_type.insert("page_view".to_string(), zones);

        let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
        let groups = grouper.group_zones_by_link_field(&zones_by_type);

        let user1_key = "str:user1";
        let user1_group = &groups[user1_key];
        let row_indices = &user1_group.rows_by_type["page_view"];

        // Should be sorted by timestamp: 1000, 2000, 3000
        assert_eq!(row_indices.len(), 3);
        // Verify sorting by checking timestamps
        let accessor = PreparedAccessor::new(&zones_by_type["page_view"][0].values);
        let ts1 = accessor
            .get_i64_at("timestamp", row_indices[0].row_idx)
            .unwrap();
        let ts2 = accessor
            .get_i64_at("timestamp", row_indices[1].row_idx)
            .unwrap();
        let ts3 = accessor
            .get_i64_at("timestamp", row_indices[2].row_idx)
            .unwrap();

        assert!(ts1 <= ts2);
        assert!(ts2 <= ts3);
    }

    #[test]
    fn test_group_handles_missing_link_field() {
        let mut zones_by_type = HashMap::new();

        // Create zone without user_id field
        use crate::engine::core::column::column_values::ColumnValues;
        use crate::engine::core::read::cache::DecompressedBlock;
        use std::sync::Arc;

        let mut values = HashMap::new();
        let mut bytes: Vec<u8> = b"ctx1".to_vec();
        let ranges = vec![(0, 4)];
        let block = Arc::new(DecompressedBlock::from_bytes(bytes));
        values.insert("context_id".to_string(), ColumnValues::new(block, ranges));
        let mut zone = CandidateZone::new(0, "seg1".to_string());
        zone.set_values(values);

        zones_by_type.insert("page_view".to_string(), vec![zone]);

        let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
        let groups = grouper.group_zones_by_link_field(&zones_by_type);

        // Should have no groups (missing link field)
        assert_eq!(groups.len(), 0);
    }
}
