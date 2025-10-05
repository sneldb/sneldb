use super::columns::ProjectionColumns;

#[test]
fn new_is_empty() {
    let cols = ProjectionColumns::new();
    assert!(cols.is_empty());
    assert_eq!(cols.len(), 0);
    assert!(cols.into_vec().is_empty());
}

#[test]
fn add_single_and_contains() {
    let mut cols = ProjectionColumns::new();
    cols.add("timestamp");
    assert!(!cols.is_empty());
    assert_eq!(cols.len(), 1);
    assert!(cols.contains("timestamp"));
    assert!(!cols.contains("event_type"));
}

#[test]
fn add_deduplicates() {
    let mut cols = ProjectionColumns::new();
    cols.add("a");
    cols.add(String::from("a"));
    assert_eq!(cols.len(), 1);
}

#[test]
fn add_many_accumulates_and_dedups() {
    let mut cols = ProjectionColumns::new();
    cols.add("a");
    cols.add_many(vec!["b".to_string(), "a".to_string(), "c".to_string()]);
    assert_eq!(cols.len(), 3);
    assert!(cols.contains("a"));
    assert!(cols.contains("b"));
    assert!(cols.contains("c"));
}

#[test]
fn union_merges_sets() {
    let mut left = ProjectionColumns::new();
    left.add("a");
    left.add("b");

    let mut right = ProjectionColumns::new();
    right.add("b");
    right.add("c");

    let merged = left.union(right);
    assert_eq!(merged.len(), 3);
    assert!(merged.contains("a"));
    assert!(merged.contains("b"));
    assert!(merged.contains("c"));
}

#[test]
fn union_with_empty_identity() {
    let mut left = ProjectionColumns::new();
    left.add("x");
    let right = ProjectionColumns::new();
    let merged = left.union(right);
    assert_eq!(merged.len(), 1);
    assert!(merged.contains("x"));
}

#[test]
fn into_vec_returns_all_items_unordered() {
    let mut cols = ProjectionColumns::new();
    cols.add("a");
    cols.add("b");
    cols.add("c");
    let mut v = cols.into_vec();
    v.sort();
    assert_eq!(v, vec!["a", "b", "c"]);
}

#[test]
fn add_many_empty_is_noop() {
    let mut cols = ProjectionColumns::new();
    let empty: Vec<String> = vec![];
    cols.add_many(empty);
    assert!(cols.is_empty());
}

#[test]
fn large_insertions() {
    let mut cols = ProjectionColumns::new();
    for i in 0..1000 {
        cols.add(format!("col_{}", i));
    }
    assert_eq!(cols.len(), 1000);
    assert!(cols.contains("col_0"));
    assert!(cols.contains("col_999"));
}
