use crate::engine::core::ZoneMerger;
use crate::test_helpers::factory::Factory;

#[test]
fn test_zone_merger_full_merge_behavior() {
    // Cursor 1: ctx1, ctx4
    let cursor_1 = Factory::zone_cursor()
        .with_zone_id(0)
        .with_segment_id(100)
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx1").create(),
            Factory::zone_row().with_context_id("ctx4").create(),
        ])
        .create();

    // Cursor 2: ctx2
    let cursor_2 = Factory::zone_cursor()
        .with_zone_id(1)
        .with_segment_id(101)
        .with_rows(vec![Factory::zone_row().with_context_id("ctx2").create()])
        .create();

    // Cursor 3: ctx3, ctx5
    let cursor_3 = Factory::zone_cursor()
        .with_zone_id(2)
        .with_segment_id(102)
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx3").create(),
            Factory::zone_row().with_context_id("ctx5").create(),
        ])
        .create();

    let mut merger = ZoneMerger::new(vec![cursor_1, cursor_2, cursor_3]);

    let mut all_rows = Vec::new();
    while let Some(row) = merger.next_row() {
        all_rows.push(row.context_id);
    }

    // Should be merged in context_id order: ctx1..ctx5
    assert_eq!(all_rows, vec!["ctx1", "ctx2", "ctx3", "ctx4", "ctx5"]);
}

#[test]
fn test_zone_merger_interleaved_complex_merge() {
    // Cursor 1: ctx01, ctx05, ctx09
    let cursor1 = Factory::zone_cursor()
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx01").create(),
            Factory::zone_row().with_context_id("ctx05").create(),
            Factory::zone_row().with_context_id("ctx09").create(),
        ])
        .with_zone_id(0)
        .with_segment_id(1)
        .create();

    // Cursor 2: ctx02, ctx06
    let cursor2 = Factory::zone_cursor()
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx02").create(),
            Factory::zone_row().with_context_id("ctx06").create(),
        ])
        .with_zone_id(1)
        .with_segment_id(2)
        .create();

    // Cursor 3: ctx03, ctx04, ctx07, ctx08
    let cursor3 = Factory::zone_cursor()
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx03").create(),
            Factory::zone_row().with_context_id("ctx04").create(),
            Factory::zone_row().with_context_id("ctx07").create(),
            Factory::zone_row().with_context_id("ctx08").create(),
        ])
        .with_zone_id(2)
        .with_segment_id(3)
        .create();

    let mut merger = ZoneMerger::new(vec![cursor1, cursor2, cursor3]);

    // Fetch all rows in batches of 3
    let mut all_rows = Vec::new();
    while let Some((batch, _created_at)) = merger.next_zone(3) {
        for row in batch {
            all_rows.push(row.context_id);
        }
    }

    // Expect all ctx0X values from 1..=9 in sorted order
    let expected = (1..=9).map(|i| format!("ctx{:02}", i)).collect::<Vec<_>>();
    assert_eq!(all_rows, expected);
}

#[test]
fn test_zone_merger_next_zone_respects_batch_size() {
    let cursor = Factory::zone_cursor()
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx1").create(),
            Factory::zone_row().with_context_id("ctx2").create(),
            Factory::zone_row().with_context_id("ctx3").create(),
        ])
        .create();

    let mut merger = ZoneMerger::new(vec![cursor]);

    let (batch, _created_at) = merger.next_zone(2).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].context_id, "ctx1");
    assert_eq!(batch[1].context_id, "ctx2");

    let (remaining, _created_at) = merger.next_zone(2).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].context_id, "ctx3");

    assert!(merger.next_zone(1).is_none());
}

#[test]
fn test_zone_merger_next_zone_interleaved_batching() {
    let cursor1 = Factory::zone_cursor()
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx01").create(),
            Factory::zone_row().with_context_id("ctx05").create(),
        ])
        .create();

    let cursor2 = Factory::zone_cursor()
        .with_rows(vec![
            Factory::zone_row().with_context_id("ctx02").create(),
            Factory::zone_row().with_context_id("ctx04").create(),
        ])
        .create();

    let cursor3 = Factory::zone_cursor()
        .with_rows(vec![Factory::zone_row().with_context_id("ctx03").create()])
        .create();

    let mut merger = ZoneMerger::new(vec![cursor1, cursor2, cursor3]);

    let (batch1, _created_at) = merger.next_zone(2).unwrap();
    assert_eq!(
        batch1
            .iter()
            .map(|r| r.context_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ctx01", "ctx02"]
    );

    let (batch2, _created_at) = merger.next_zone(2).unwrap();
    assert_eq!(
        batch2
            .iter()
            .map(|r| r.context_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ctx03", "ctx04"]
    );

    let (batch3, _created_at) = merger.next_zone(2).unwrap();
    assert_eq!(
        batch3
            .iter()
            .map(|r| r.context_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ctx05"]
    );

    assert!(merger.next_zone(2).is_none());
}
