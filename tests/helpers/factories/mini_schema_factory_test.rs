use crate::engine::schema::{EnumType, FieldType};
use crate::test_helpers::factories::MiniSchemaFactory;

#[test]
fn builds_default_schema_with_predefined_fields() {
    let schema = MiniSchemaFactory::new().create();
    let fields: Vec<_> = schema.field_names().into_iter().collect();

    assert!(fields.contains(&"username".to_string()));
    assert!(fields.contains(&"created_at".to_string()));
    assert_eq!(fields.len(), 2);
}

#[test]
fn adds_fields_with_with_method() {
    let schema = MiniSchemaFactory::new()
        .with("email", "string")
        .with("is_active", "bool")
        .create();

    assert_eq!(schema.fields.get("email"), Some(&FieldType::String));
    assert_eq!(schema.fields.get("is_active"), Some(&FieldType::Bool));
}

#[test]
fn adds_numeric_and_alias_types() {
    let schema = MiniSchemaFactory::new()
        .with("counter", "u64")
        .with("created_on", "datetime")
        .create();

    assert_eq!(schema.fields.get("counter"), Some(&FieldType::U64));
    // alias "datetime" maps to U64 in from_primitive_str
    assert_eq!(schema.fields.get("created_on"), Some(&FieldType::U64));
}

#[test]
fn unknown_type_falls_back_to_string() {
    let schema = MiniSchemaFactory::new().with("mystery", "foobar").create();
    assert_eq!(schema.fields.get("mystery"), Some(&FieldType::String));
}

#[test]
fn adds_enum_with_helper() {
    let schema = MiniSchemaFactory::empty()
        .with_enum("plan", &["pro", "basic"])
        .create();

    assert_eq!(
        schema.fields.get("plan"),
        Some(&FieldType::Enum(EnumType {
            variants: vec!["pro".to_string(), "basic".to_string()],
        }))
    );
}

#[test]
fn adds_optional_with_helper() {
    let schema = MiniSchemaFactory::empty()
        .with_optional("nickname", "string")
        .create();

    assert_eq!(
        schema.fields.get("nickname"),
        Some(&FieldType::Optional(Box::new(FieldType::String)))
    );
}

#[test]
fn removes_fields_with_without_method() {
    let schema = MiniSchemaFactory::new().without("created_at").create();

    assert!(schema.fields.contains_key("username"));
    assert!(!schema.fields.contains_key("created_at"));
}

#[test]
fn builds_empty_schema() {
    let schema = MiniSchemaFactory::empty().create();
    assert!(schema.fields.is_empty());
}
