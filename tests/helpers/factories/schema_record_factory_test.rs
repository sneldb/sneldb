use crate::engine::schema::FieldType;
use crate::test_helpers::factories::SchemaRecordFactory;

#[test]
fn schema_record_factory_creates_expected_record() {
    let record = SchemaRecordFactory::new("user_event")
        .with_uid("custom-uid")
        .with_field("email", "string")
        .without_field("field1")
        .create();

    assert_eq!(record.event_type, "user_event");
    assert_eq!(record.uid, "custom-uid");
    assert_eq!(record.schema.fields.get("email"), Some(&FieldType::String));
    assert!(!record.schema.fields.contains_key("field1"));
    assert!(record.schema.fields.contains_key("field2")); // originally present
}
