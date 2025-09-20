use std::path::PathBuf;

use crate::engine::core::write::column_paths::ColumnPathResolver;
use crate::test_helpers::factories::WriteJobFactory;

#[test]
fn column_path_for_key_prefers_existing_job_path() {
    let jobs = vec![
        WriteJobFactory::new()
            .with_key("evt", "field")
            .with_path(PathBuf::from("/tmp/segment/uid-field.col"))
            .create_with_value("v1"),
    ];

    let key = ("evt".to_string(), "field".to_string());
    let resolver = ColumnPathResolver::new(&jobs);
    let path = resolver.column_path_for_key(&key);
    assert_eq!(path, PathBuf::from("/tmp/segment/uid-field.col"));
}

#[test]
fn column_path_for_key_falls_back_to_default() {
    let jobs = vec![];
    let key = ("evt".to_string(), "field".to_string());
    let resolver = ColumnPathResolver::new(&jobs);
    let path = resolver.column_path_for_key(&key);
    assert_eq!(path, PathBuf::from("evt_field.col"));
}

#[test]
fn zfc_path_converts_col_to_zfc() {
    let jobs = vec![
        WriteJobFactory::new()
            .with_key("evt", "field")
            .with_path(PathBuf::from("/tmp/segment/uid-field.col"))
            .create_with_value("v1"),
    ];
    let key = ("evt".to_string(), "field".to_string());
    let resolver = ColumnPathResolver::new(&jobs);
    let path = resolver.zfc_path_for_key(&key);
    assert_eq!(path, PathBuf::from("/tmp/segment/uid-field.zfc"));
}

#[test]
fn zfc_path_converts_default_col_to_zfc() {
    let jobs = vec![];
    let key = ("evt".to_string(), "field".to_string());
    let resolver = ColumnPathResolver::new(&jobs);
    let path = resolver.zfc_path_for_key(&key);
    assert_eq!(path, PathBuf::from("evt_field.zfc"));
}
