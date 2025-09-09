use crate::engine::core::MemTable;
use crate::engine::errors::StoreError;
use crate::test_helpers::factories::EventFactory;

#[test]
fn test_memtable_insert_and_len() {
    let mut memtable = MemTable::new(10);
    assert_eq!(memtable.len(), 0);
    assert!(!memtable.is_full());

    let events = EventFactory::new().create_list(5);
    for event in &events {
        memtable.insert(event.clone()).unwrap();
    }

    assert_eq!(memtable.len(), 5);
    assert!(!memtable.is_full());
}

#[test]
fn test_memtable_is_full_behavior() {
    let mut memtable = MemTable::new(3);
    let events = EventFactory::new().create_list(3);

    for event in &events {
        memtable.insert(event.clone()).unwrap();
    }

    assert_eq!(memtable.len(), 3);
    assert!(memtable.is_full());
}

#[test]
fn test_memtable_rejects_invalid_event() {
    let mut memtable = MemTable::new(5);

    let mut bad = EventFactory::new().create();
    bad.context_id = "".into();

    let err = memtable.insert(bad).unwrap_err();
    assert!(matches!(err, StoreError::InvalidContextId));

    let mut bad2 = EventFactory::new().create();
    bad2.event_type = "".into();

    let err2 = memtable.insert(bad2).unwrap_err();
    assert!(matches!(err2, StoreError::InvalidEventType));

    assert_eq!(memtable.len(), 0);
}

#[test]
fn test_memtable_iter_yields_sorted_contexts() {
    let mut memtable = MemTable::new(10);
    let e1 = EventFactory::new().with("context_id", "ctx_b").create();
    let e2 = EventFactory::new().with("context_id", "ctx_a").create();
    let e3 = EventFactory::new().with("context_id", "ctx_c").create();

    // insert in random order
    memtable.insert(e1.clone()).unwrap();
    memtable.insert(e2.clone()).unwrap();
    memtable.insert(e3.clone()).unwrap();

    let result: Vec<_> = memtable.iter().map(|e| e.context_id.clone()).collect();
    assert_eq!(result, vec!["ctx_a", "ctx_b", "ctx_c"]);
}

#[test]
fn test_memtable_take_flushes_and_empties() {
    let mut memtable = MemTable::new(4);
    let events = EventFactory::new().create_list(2);
    for event in &events {
        memtable.insert(event.clone()).unwrap();
    }

    assert_eq!(memtable.len(), 2);
    let taken = memtable.take();
    assert_eq!(taken.len(), 2);
}
