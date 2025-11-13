use super::schema::SchemaCache;

#[test]
fn schema_cache_new_creates_empty_cache() {
    let cache = SchemaCache::new();
    assert!(cache.column_indices().is_none());
}

#[test]
fn schema_cache_initialize_column_indices_creates_mapping() {
    let mut cache = SchemaCache::new();
    let column_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    cache.initialize_column_indices(&column_names);

    let indices = cache.column_indices().unwrap();
    assert_eq!(indices.get("a"), Some(&0));
    assert_eq!(indices.get("b"), Some(&1));
    assert_eq!(indices.get("c"), Some(&2));
    assert_eq!(indices.len(), 3);
}

#[test]
fn schema_cache_initialize_same_schema_reuses_cache() {
    let mut cache = SchemaCache::new();
    let column_names = vec!["a".to_string(), "b".to_string()];

    cache.initialize_column_indices(&column_names);
    let val_a_1 = cache.column_indices().unwrap().get("a").cloned();
    let val_b_1 = cache.column_indices().unwrap().get("b").cloned();

    // Initialize again with same schema
    cache.initialize_column_indices(&column_names);
    let val_a_2 = cache.column_indices().unwrap().get("a").cloned();
    let val_b_2 = cache.column_indices().unwrap().get("b").cloned();

    // Should have the same values (reused)
    assert_eq!(val_a_1, val_a_2);
    assert_eq!(val_b_1, val_b_2);
}

#[test]
fn schema_cache_initialize_different_schema_updates_cache() {
    let mut cache = SchemaCache::new();
    let column_names1 = vec!["a".to_string(), "b".to_string()];
    let column_names2 = vec!["x".to_string(), "y".to_string(), "z".to_string()];

    cache.initialize_column_indices(&column_names1);
    assert_eq!(cache.column_indices().unwrap().get("a"), Some(&0));

    cache.initialize_column_indices(&column_names2);
    let indices = cache.column_indices().unwrap();
    assert_eq!(indices.get("x"), Some(&0));
    assert_eq!(indices.get("y"), Some(&1));
    assert_eq!(indices.get("z"), Some(&2));
    assert!(indices.get("a").is_none()); // Old schema should be gone
}

#[test]
fn schema_cache_initialize_different_length_updates_cache() {
    let mut cache = SchemaCache::new();
    let column_names1 = vec!["a".to_string(), "b".to_string()];
    let column_names2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    cache.initialize_column_indices(&column_names1);
    assert_eq!(cache.column_indices().unwrap().len(), 2);

    cache.initialize_column_indices(&column_names2);
    assert_eq!(cache.column_indices().unwrap().len(), 3);
}

#[test]
fn schema_cache_initialize_different_order_updates_cache() {
    let mut cache = SchemaCache::new();
    let column_names1 = vec!["a".to_string(), "b".to_string()];
    let column_names2 = vec!["b".to_string(), "a".to_string()];

    cache.initialize_column_indices(&column_names1);
    assert_eq!(cache.column_indices().unwrap().get("a"), Some(&0));
    assert_eq!(cache.column_indices().unwrap().get("b"), Some(&1));

    cache.initialize_column_indices(&column_names2);
    let indices = cache.column_indices().unwrap();
    assert_eq!(indices.get("a"), Some(&1)); // Order changed
    assert_eq!(indices.get("b"), Some(&0));
}

#[test]
fn schema_cache_empty_column_names() {
    let mut cache = SchemaCache::new();
    let column_names = vec![];

    cache.initialize_column_indices(&column_names);

    let indices = cache.column_indices().unwrap();
    assert_eq!(indices.len(), 0);
}

#[test]
fn schema_cache_single_column() {
    let mut cache = SchemaCache::new();
    let column_names = vec!["timestamp".to_string()];

    cache.initialize_column_indices(&column_names);

    let indices = cache.column_indices().unwrap();
    assert_eq!(indices.get("timestamp"), Some(&0));
    assert_eq!(indices.len(), 1);
}

#[test]
fn schema_cache_many_columns() {
    let mut cache = SchemaCache::new();
    let column_names: Vec<String> = (0..100).map(|i| format!("col_{}", i)).collect();

    cache.initialize_column_indices(&column_names);

    let indices = cache.column_indices().unwrap();
    assert_eq!(indices.len(), 100);
    for i in 0..100 {
        assert_eq!(indices.get(&format!("col_{}", i)), Some(&i));
    }
}

#[test]
fn schema_cache_duplicate_column_names() {
    let mut cache = SchemaCache::new();
    let column_names = vec!["a".to_string(), "a".to_string(), "b".to_string()];

    cache.initialize_column_indices(&column_names);

    let indices = cache.column_indices().unwrap();
    // Last occurrence wins
    assert_eq!(indices.get("a"), Some(&1));
    assert_eq!(indices.get("b"), Some(&2));
}

