use crate::command::types::OrderSpec;
use crate::engine::core::{Event, EventSorter};
use crate::test_helpers::factory::Factory;
use serde_json::json;

#[test]
fn creates_new_event_sorter() {
    let sorter = EventSorter::new("timestamp".to_string(), true);
    assert_eq!(sorter.field(), "timestamp");
    assert!(sorter.is_ascending());
}

#[test]
fn creates_from_order_spec_ascending() {
    let order = OrderSpec {
        field: "score".to_string(),
        desc: false,
    };
    let sorter = EventSorter::from_order_spec(&order);
    assert_eq!(sorter.field(), "score");
    assert!(sorter.is_ascending());
}

#[test]
fn creates_from_order_spec_descending() {
    let order = OrderSpec {
        field: "timestamp".to_string(),
        desc: true,
    };
    let sorter = EventSorter::from_order_spec(&order);
    assert_eq!(sorter.field(), "timestamp");
    assert!(!sorter.is_ascending());
}

#[test]
fn sorts_events_by_timestamp_ascending() {
    let mut events = vec![
        Factory::event().with("timestamp", 3000_u64).create(),
        Factory::event().with("timestamp", 1000_u64).create(),
        Factory::event().with("timestamp", 2000_u64).create(),
    ];

    let sorter = EventSorter::new("timestamp".to_string(), true);
    sorter.sort(&mut events);

    assert_eq!(events[0].timestamp, 1000);
    assert_eq!(events[1].timestamp, 2000);
    assert_eq!(events[2].timestamp, 3000);
}

#[test]
fn sorts_events_by_timestamp_descending() {
    let mut events = vec![
        Factory::event().with("timestamp", 1000_u64).create(),
        Factory::event().with("timestamp", 3000_u64).create(),
        Factory::event().with("timestamp", 2000_u64).create(),
    ];

    let sorter = EventSorter::new("timestamp".to_string(), false);
    sorter.sort(&mut events);

    assert_eq!(events[0].timestamp, 3000);
    assert_eq!(events[1].timestamp, 2000);
    assert_eq!(events[2].timestamp, 1000);
}

#[test]
fn sorts_events_by_context_id() {
    let mut events = vec![
        Factory::event().with("context_id", "ctx-z").create(),
        Factory::event().with("context_id", "ctx-a").create(),
        Factory::event().with("context_id", "ctx-m").create(),
    ];

    let sorter = EventSorter::new("context_id".to_string(), true);
    sorter.sort(&mut events);

    assert_eq!(events[0].context_id, "ctx-a");
    assert_eq!(events[1].context_id, "ctx-m");
    assert_eq!(events[2].context_id, "ctx-z");
}

#[test]
fn sorts_events_by_payload_field() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"amount": 500}))
            .create(),
        Factory::event()
            .with("payload", json!({"amount": 100}))
            .create(),
        Factory::event()
            .with("payload", json!({"amount": 300}))
            .create(),
    ];

    let sorter = EventSorter::new("amount".to_string(), true);
    sorter.sort(&mut events);

    assert_eq!(events[0].get_field_value("amount"), "100");
    assert_eq!(events[1].get_field_value("amount"), "300");
    assert_eq!(events[2].get_field_value("amount"), "500");
}

#[test]
fn sorts_events_by_string_field() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"region": "US"}))
            .create(),
        Factory::event()
            .with("payload", json!({"region": "EU"}))
            .create(),
        Factory::event()
            .with("payload", json!({"region": "ASIA"}))
            .create(),
    ];

    let sorter = EventSorter::new("region".to_string(), true);
    sorter.sort(&mut events);

    assert_eq!(events[0].get_field_value("region"), "ASIA");
    assert_eq!(events[1].get_field_value("region"), "EU");
    assert_eq!(events[2].get_field_value("region"), "US");
}

#[test]
fn sorts_empty_list() {
    let mut events: Vec<Event> = vec![];
    let sorter = EventSorter::new("timestamp".to_string(), true);
    sorter.sort(&mut events);
    assert_eq!(events.len(), 0);
}

#[test]
fn sorts_single_element() {
    let mut events = vec![Factory::event().create()];
    let sorter = EventSorter::new("timestamp".to_string(), true);
    sorter.sort(&mut events);
    assert_eq!(events.len(), 1);
}

#[test]
fn sorts_with_duplicate_values() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"priority": 2}))
            .create(),
        Factory::event()
            .with("payload", json!({"priority": 1}))
            .create(),
        Factory::event()
            .with("payload", json!({"priority": 2}))
            .create(),
        Factory::event()
            .with("payload", json!({"priority": 1}))
            .create(),
    ];

    let sorter = EventSorter::new("priority".to_string(), true);
    sorter.sort(&mut events);

    // Should be stable or at least group same values together
    assert_eq!(events[0].get_field_value("priority"), "1");
    assert_eq!(events[1].get_field_value("priority"), "1");
    assert_eq!(events[2].get_field_value("priority"), "2");
    assert_eq!(events[3].get_field_value("priority"), "2");
}

#[test]
fn sort_vec_method_works() {
    let mut events = vec![
        Factory::event().with("timestamp", 3000_u64).create(),
        Factory::event().with("timestamp", 1000_u64).create(),
        Factory::event().with("timestamp", 2000_u64).create(),
    ];

    let sorter = EventSorter::new("timestamp".to_string(), true);
    sorter.sort_vec(&mut events);

    assert_eq!(events[0].timestamp, 1000);
    assert_eq!(events[1].timestamp, 2000);
    assert_eq!(events[2].timestamp, 3000);
}

#[test]
fn clone_works() {
    let sorter = EventSorter::new("field".to_string(), true);
    let cloned = sorter.clone();

    assert_eq!(sorter.field(), cloned.field());
    assert_eq!(sorter.is_ascending(), cloned.is_ascending());
}

#[test]
fn debug_format_is_readable() {
    let sorter = EventSorter::new("timestamp".to_string(), true);
    let debug_str = format!("{:?}", sorter);
    assert!(debug_str.contains("timestamp"));
    assert!(debug_str.contains("true"));
}

#[test]
fn display_format_is_readable() {
    let sorter_asc = EventSorter::new("score".to_string(), true);
    assert_eq!(sorter_asc.to_string(), "EventSorter(score ASC)");

    let sorter_desc = EventSorter::new("timestamp".to_string(), false);
    assert_eq!(sorter_desc.to_string(), "EventSorter(timestamp DESC)");
}

#[test]
fn sorts_large_list_efficiently() {
    // Generate 1000 events with random-ish timestamps
    let mut events: Vec<Event> = (0..1000)
        .map(|i| {
            let timestamp = 1000000 - (i * 997) % 100000; // pseudo-random order
            Factory::event()
                .with("timestamp", timestamp as u64)
                .create()
        })
        .collect();

    let sorter = EventSorter::new("timestamp".to_string(), true);
    sorter.sort(&mut events);

    // Verify sorted order
    for i in 0..events.len() - 1 {
        assert!(events[i].timestamp <= events[i + 1].timestamp);
    }
}

#[test]
fn sorts_by_negative_numbers() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"balance": -100}))
            .create(),
        Factory::event()
            .with("payload", json!({"balance": 50}))
            .create(),
        Factory::event()
            .with("payload", json!({"balance": -200}))
            .create(),
    ];

    let sorter = EventSorter::new("balance".to_string(), true);
    sorter.sort(&mut events);

    assert_eq!(events[0].get_field_value("balance"), "-200");
    assert_eq!(events[1].get_field_value("balance"), "-100");
    assert_eq!(events[2].get_field_value("balance"), "50");
}

#[test]
fn sorts_by_decimal_values() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"rate": 0.5}))
            .create(),
        Factory::event()
            .with("payload", json!({"rate": 0.1}))
            .create(),
        Factory::event()
            .with("payload", json!({"rate": 0.9}))
            .create(),
    ];

    let sorter = EventSorter::new("rate".to_string(), true);
    sorter.sort(&mut events);

    assert_eq!(events[0].get_field_value("rate"), "0.1");
    assert_eq!(events[1].get_field_value("rate"), "0.5");
    assert_eq!(events[2].get_field_value("rate"), "0.9");
}

#[test]
fn ascending_and_descending_are_inverses() {
    let mut events_asc = vec![
        Factory::event().with("timestamp", 3000_u64).create(),
        Factory::event().with("timestamp", 1000_u64).create(),
        Factory::event().with("timestamp", 2000_u64).create(),
    ];

    let mut events_desc = events_asc.clone();

    let sorter_asc = EventSorter::new("timestamp".to_string(), true);
    sorter_asc.sort(&mut events_asc);

    let sorter_desc = EventSorter::new("timestamp".to_string(), false);
    sorter_desc.sort(&mut events_desc);

    // Ascending and descending should be reverse of each other
    assert_eq!(events_asc[0].timestamp, events_desc[2].timestamp);
    assert_eq!(events_asc[1].timestamp, events_desc[1].timestamp);
    assert_eq!(events_asc[2].timestamp, events_desc[0].timestamp);
}
