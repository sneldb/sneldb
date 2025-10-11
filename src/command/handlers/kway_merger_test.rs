use super::kway_merger::KWayMerger;
use serde_json::json;

fn make_row(context: &str, event: &str, timestamp: u64, value: u64) -> Vec<serde_json::Value> {
    vec![
        json!(context),
        json!(event),
        json!(timestamp),
        json!({"value": value}),
    ]
}

#[test]
fn merges_two_sorted_shards_ascending() {
    let shard0 = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 5000, 50),
    ];

    let shard1 = vec![
        make_row("ctx", "click", 2000, 20),
        make_row("ctx", "click", 4000, 40),
        make_row("ctx", "click", 6000, 60),
    ];

    let shards = vec![shard0, shard1];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 6);
    assert_eq!(result[0][2], 1000);
    assert_eq!(result[1][2], 2000);
    assert_eq!(result[2][2], 3000);
    assert_eq!(result[3][2], 4000);
    assert_eq!(result[4][2], 5000);
    assert_eq!(result[5][2], 6000);
}

#[test]
fn merges_two_sorted_shards_descending() {
    let shard0 = vec![
        make_row("ctx", "click", 5000, 50),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 1000, 10),
    ];

    let shard1 = vec![
        make_row("ctx", "click", 6000, 60),
        make_row("ctx", "click", 4000, 40),
        make_row("ctx", "click", 2000, 20),
    ];

    let shards = vec![shard0, shard1];
    let merger = KWayMerger::new(&shards, "timestamp", false, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 6);
    assert_eq!(result[0][2], 6000);
    assert_eq!(result[1][2], 5000);
    assert_eq!(result[2][2], 4000);
    assert_eq!(result[3][2], 3000);
    assert_eq!(result[4][2], 2000);
    assert_eq!(result[5][2], 1000);
}

#[test]
fn respects_limit() {
    let shard0 = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 5000, 50),
    ];

    let shard1 = vec![
        make_row("ctx", "click", 2000, 20),
        make_row("ctx", "click", 4000, 40),
        make_row("ctx", "click", 6000, 60),
    ];

    let shards = vec![shard0, shard1];
    let merger = KWayMerger::new(&shards, "timestamp", true, 3);
    let result = merger.merge();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0][2], 1000);
    assert_eq!(result[1][2], 2000);
    assert_eq!(result[2][2], 3000);
}

#[test]
fn handles_empty_shards() {
    let shard0: Vec<Vec<serde_json::Value>> = vec![];
    let shard1 = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
    ];
    let shard2: Vec<Vec<serde_json::Value>> = vec![];

    let shards = vec![shard0, shard1, shard2];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0][2], 1000);
    assert_eq!(result[1][2], 2000);
}

#[test]
fn handles_all_empty_shards() {
    let shard0: Vec<Vec<serde_json::Value>> = vec![];
    let shard1: Vec<Vec<serde_json::Value>> = vec![];

    let shards = vec![shard0, shard1];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 0);
}

#[test]
fn handles_single_shard() {
    let shard0 = vec![
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
    ];

    let shards = vec![shard0];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    // Should return all rows in original order (already "merged")
    assert_eq!(result.len(), 3);
}

#[test]
fn merges_many_shards() {
    let shard0 = vec![make_row("ctx", "click", 1000, 10)];
    let shard1 = vec![make_row("ctx", "click", 2000, 20)];
    let shard2 = vec![make_row("ctx", "click", 3000, 30)];
    let shard3 = vec![make_row("ctx", "click", 4000, 40)];
    let shard4 = vec![make_row("ctx", "click", 5000, 50)];

    let shards = vec![shard0, shard1, shard2, shard3, shard4];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 5);
    for i in 0..5 {
        assert_eq!(result[i][2], (i as u64 + 1) * 1000);
    }
}

#[test]
fn merges_by_payload_field() {
    let shard0 = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 1000, 30),
    ];

    let shard1 = vec![
        make_row("ctx", "click", 1000, 20),
        make_row("ctx", "click", 1000, 40),
    ];

    let shards = vec![shard0, shard1];
    let merger = KWayMerger::new(&shards, "value", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 4);
    assert_eq!(result[0][3]["value"], 10);
    assert_eq!(result[1][3]["value"], 20);
    assert_eq!(result[2][3]["value"], 30);
    assert_eq!(result[3][3]["value"], 40);
}

#[test]
fn handles_duplicate_values() {
    let shard0 = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
    ];

    let shard1 = vec![
        make_row("ctx", "click", 1000, 10), // duplicate
        make_row("ctx", "click", 2000, 20), // duplicate
    ];

    let shards = vec![shard0, shard1];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 4);
    // Duplicates should be preserved
    assert_eq!(result[0][2], 1000);
    assert_eq!(result[1][2], 1000);
    assert_eq!(result[2][2], 2000);
    assert_eq!(result[3][2], 2000);
}

#[test]
fn apply_pagination_with_offset() {
    let rows = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 4000, 40),
        make_row("ctx", "click", 5000, 50),
    ];

    let result = KWayMerger::apply_pagination(rows, Some(2), None);

    assert_eq!(result.len(), 3);
    assert_eq!(result[0][2], 3000);
    assert_eq!(result[1][2], 4000);
    assert_eq!(result[2][2], 5000);
}

#[test]
fn apply_pagination_with_limit() {
    let rows = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 4000, 40),
        make_row("ctx", "click", 5000, 50),
    ];

    let result = KWayMerger::apply_pagination(rows, None, Some(3));

    assert_eq!(result.len(), 3);
    assert_eq!(result[0][2], 1000);
    assert_eq!(result[1][2], 2000);
    assert_eq!(result[2][2], 3000);
}

#[test]
fn apply_pagination_with_both() {
    let rows = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 4000, 40),
        make_row("ctx", "click", 5000, 50),
    ];

    let result = KWayMerger::apply_pagination(rows, Some(1), Some(2));

    assert_eq!(result.len(), 2);
    assert_eq!(result[0][2], 2000);
    assert_eq!(result[1][2], 3000);
}

#[test]
fn apply_pagination_offset_exceeds_length() {
    let rows = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
    ];

    let result = KWayMerger::apply_pagination(rows, Some(10), None);

    assert_eq!(result.len(), 0);
}

#[test]
fn apply_pagination_limit_exceeds_length() {
    let rows = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
    ];

    let result = KWayMerger::apply_pagination(rows, None, Some(10));

    assert_eq!(result.len(), 2);
}

#[test]
fn zero_limit_returns_empty() {
    let shard0 = vec![
        make_row("ctx", "click", 1000, 10),
        make_row("ctx", "click", 2000, 20),
    ];

    let shards = vec![shard0];
    let merger = KWayMerger::new(&shards, "timestamp", true, 0);
    let result = merger.merge();

    assert_eq!(result.len(), 0);
}

#[test]
fn large_merge_performance() {
    // Create 100 shards with 100 rows each (10,000 total rows)
    let mut shards = Vec::new();
    for shard_id in 0..100 {
        let mut shard_rows = Vec::new();
        for i in 0..100 {
            let timestamp = (shard_id * 100 + i) as u64;
            shard_rows.push(make_row("ctx", "click", timestamp, timestamp));
        }
        shards.push(shard_rows);
    }

    let start = std::time::Instant::now();
    let merger = KWayMerger::new(&shards, "timestamp", true, 1000);
    let result = merger.merge();
    let duration = start.elapsed();

    // Should complete quickly (< 100ms for 100 shards with indices approach)
    assert!(duration.as_millis() < 100);
    assert_eq!(result.len(), 1000);

    // Verify sorted order
    for i in 0..result.len() - 1 {
        let ts_curr = result[i][2].as_u64().unwrap();
        let ts_next = result[i + 1][2].as_u64().unwrap();
        assert!(ts_curr <= ts_next);
    }
}

#[test]
fn merges_with_varying_shard_sizes() {
    let shard0 = vec![make_row("ctx", "click", 1000, 10)]; // 1 row
    let shard1 = vec![
        make_row("ctx", "click", 2000, 20),
        make_row("ctx", "click", 3000, 30),
        make_row("ctx", "click", 4000, 40),
    ]; // 3 rows
    let shard2 = vec![
        make_row("ctx", "click", 5000, 50),
        make_row("ctx", "click", 6000, 60),
    ]; // 2 rows

    let shards = vec![shard0, shard1, shard2];
    let merger = KWayMerger::new(&shards, "timestamp", true, 100);
    let result = merger.merge();

    assert_eq!(result.len(), 6);
    for i in 0..6 {
        assert_eq!(result[i][2], (i as u64 + 1) * 1000);
    }
}

