use crate::engine::core::ZonePlan;
use crate::engine::core::zone::rlte_index::RlteIndex;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use tempfile::tempdir;

// Build a single-zone plan with given event payloads for a field
fn build_single_zone_plan(
    uid: &str,
    event_type: &str,
    field: &str,
    values: Vec<serde_json::Value>,
) -> Vec<ZonePlan> {
    // Prepare events with payload { field: value }
    let mut events = Vec::with_capacity(values.len());
    for v in values {
        let payload = serde_json::json!({ field: v });
        events.push(
            EventFactory::new()
                .with("event_type", event_type)
                .with("context_id", "ctx")
                .with("payload", payload)
                .create(),
        );
    }

    // Force exactly one zone
    ZonePlan::build_all(&events, events.len(), uid.to_string(), 1).expect("Failed to build zone")
}

#[tokio::test]
async fn test_rlte_build_ladder_for_string_field() {
    // Schema + UID
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "evt_str";
    schema_factory
        .define_with_fields(event_type, &[("name", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // 16 values; RLTE sorts descending lex and samples ranks 1,2,4,8,16
    let values = vec![
        serde_json::json!("zebra"),
        serde_json::json!("yak"),
        serde_json::json!("wolf"),
        serde_json::json!("viper"),
        serde_json::json!("urchin"),
        serde_json::json!("tiger"),
        serde_json::json!("seal"),
        serde_json::json!("rabbit"),
        serde_json::json!("quail"),
        serde_json::json!("puma"),
        serde_json::json!("otter"),
        serde_json::json!("newt"),
        serde_json::json!("moose"),
        serde_json::json!("lion"),
        serde_json::json!("koala"),
        serde_json::json!("ibis"),
    ];

    let plans = build_single_zone_plan(&uid, event_type, "name", values);
    let rlte = RlteIndex::build_from_zones(&plans);

    // context_id excluded from RLTE
    assert!(!rlte.ladders.contains_key("context_id"));

    let zone_id = plans[0].id;
    let ladder = rlte
        .ladders
        .get("name")
        .and_then(|m| m.get(&zone_id))
        .expect("ladder for name missing");

    // Expected ranks from sorted desc list
    let expected = vec!["zebra", "yak", "viper", "rabbit", "ibis"];
    let got: Vec<&str> = ladder.iter().map(|s| s.as_str()).collect();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn test_rlte_save_and_load_roundtrip_for_int_field() {
    // Schema + UID
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "evt_num";
    schema_factory
        .define_with_fields(event_type, &[("score", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // 16 numeric values that, as strings, sort descending as shown
    // Sorted desc (string) will be: 99, 97, 90, 85, 80, 70, 60, 55, 50, 40, 35, 30, 25, 20, 15, 10
    let values = vec![
        serde_json::json!(99),
        serde_json::json!(97),
        serde_json::json!(90),
        serde_json::json!(85),
        serde_json::json!(80),
        serde_json::json!(70),
        serde_json::json!(60),
        serde_json::json!(55),
        serde_json::json!(50),
        serde_json::json!(40),
        serde_json::json!(35),
        serde_json::json!(30),
        serde_json::json!(25),
        serde_json::json!(20),
        serde_json::json!(15),
        serde_json::json!(10),
    ];

    let plans = build_single_zone_plan(&uid, event_type, "score", values);
    let rlte = RlteIndex::build_from_zones(&plans);

    // Expected ladder per rank 1,2,4,8,16 (sorted descending with sortable format)
    // Sortable format applies bias: n.wrapping_sub(i64::MIN) as u64 -> zero-padded 20 digits
    // For positive integers: bias + value
    // 99 -> 9223372036854775808 + 99 = 9223372036854775907 -> "09223372036854775907"
    // 97 -> 9223372036854775808 + 97 = 9223372036854775905 -> "09223372036854775905"
    // etc.
    let expected = vec![
        "09223372036854775907", // 99
        "09223372036854775905", // 97
        "09223372036854775893", // 85
        "09223372036854775863", // 55
        "09223372036854775818", // 10
    ];
    let zone_id = plans[0].id;
    let ladder = rlte
        .ladders
        .get("score")
        .and_then(|m| m.get(&zone_id))
        .expect("ladder for score missing");
    let got: Vec<&str> = ladder.iter().map(|s| s.as_str()).collect();
    assert_eq!(got, expected);

    // Save and load roundtrip
    let dir = tempdir().unwrap();
    rlte.save(&uid, dir.path()).expect("save RLTE");
    let loaded = RlteIndex::load(&uid, dir.path()).expect("load RLTE");
    let loaded_ladder = loaded
        .ladders
        .get("score")
        .and_then(|m| m.get(&zone_id))
        .expect("loaded ladder for score missing");
    assert_eq!(loaded_ladder, ladder);
}
