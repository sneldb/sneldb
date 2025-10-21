use crate::engine::core::read::catalog::IndexKind;
use crate::engine::core::zone::index_build_policy::{FieldCategory, IndexBuildPolicy};
use crate::engine::schema::types::{EnumType, FieldType};

#[test]
fn categorize_core_fields_and_types() {
    // Explicit override: field name "timestamp" => Temporal regardless of type
    assert_eq!(
        IndexBuildPolicy::categorize("timestamp", &FieldType::String),
        FieldCategory::Temporal
    );

    // Temporal by type
    assert_eq!(
        IndexBuildPolicy::categorize("created_at", &FieldType::Timestamp),
        FieldCategory::Temporal
    );
    assert_eq!(
        IndexBuildPolicy::categorize("day", &FieldType::Date),
        FieldCategory::Temporal
    );

    // Optional temporal types
    assert_eq!(
        IndexBuildPolicy::categorize(
            "maybe_time",
            &FieldType::Optional(Box::new(FieldType::Timestamp)),
        ),
        FieldCategory::Temporal
    );
    assert_eq!(
        IndexBuildPolicy::categorize("maybe_day", &FieldType::Optional(Box::new(FieldType::Date)),),
        FieldCategory::Temporal
    );

    // Enum by type
    let et = FieldType::Enum(EnumType {
        variants: vec!["A".to_string(), "B".to_string()],
    });
    assert_eq!(
        IndexBuildPolicy::categorize("kind", &et),
        FieldCategory::Enum
    );

    // Name-based categories for non-temporal primitives
    assert_eq!(
        IndexBuildPolicy::categorize("context_id", &FieldType::String),
        FieldCategory::Context
    );
    assert_eq!(
        IndexBuildPolicy::categorize("event_type", &FieldType::String),
        FieldCategory::EventType
    );

    // Generic primitive
    assert_eq!(
        IndexBuildPolicy::categorize("score", &FieldType::F64),
        FieldCategory::Primitive
    );
}

#[test]
fn kinds_for_category_temporal_and_enum() {
    let pol = IndexBuildPolicy::default();
    let t = pol.kinds_for_category(FieldCategory::Temporal);
    assert!(t.contains(IndexKind::TS_CALENDAR));
    assert!(t.contains(IndexKind::TS_ZTI));
    assert!(t.contains(IndexKind::FIELD_CALENDAR));
    assert!(t.contains(IndexKind::FIELD_ZTI));

    let e = pol.kinds_for_category(FieldCategory::Enum);
    assert_eq!(e, IndexKind::ENUM_BITMAP);
}

#[test]
fn kinds_for_category_context_and_event_type() {
    let pol = IndexBuildPolicy::default();
    for cat in [FieldCategory::Context, FieldCategory::EventType] {
        let k = pol.kinds_for_category(cat);
        assert_eq!(k, IndexKind::XOR_FIELD_FILTER | IndexKind::ZONE_XOR_INDEX);
    }
}

#[test]
fn kinds_for_category_primitives_respect_rlte_flag() {
    // RLTE enabled
    let pol_on = IndexBuildPolicy {
        enable_rlte_for_primitives: true,
    };
    let k_on = pol_on.kinds_for_category(FieldCategory::Primitive);
    let expected_on = IndexKind::ZONE_SURF
        | IndexKind::ZONE_XOR_INDEX
        | IndexKind::XOR_FIELD_FILTER
        | IndexKind::RLTE;
    assert_eq!(k_on, expected_on);

    // RLTE disabled
    let pol_off = IndexBuildPolicy {
        enable_rlte_for_primitives: false,
    };
    let k_off = pol_off.kinds_for_category(FieldCategory::Primitive);
    let expected_off =
        IndexKind::ZONE_SURF | IndexKind::ZONE_XOR_INDEX | IndexKind::XOR_FIELD_FILTER;
    assert_eq!(k_off, expected_off);
}
