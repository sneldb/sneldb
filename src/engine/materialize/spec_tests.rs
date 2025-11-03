use super::spec::MaterializedQuerySpecExt;
use crate::command::types::{Command, MaterializedQuerySpec};
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::test_helpers::factories::CommandFactory;

fn build_spec() -> MaterializedQuerySpec {
    build_spec_with_since(None)
}

fn build_spec_with_since(since: Option<&str>) -> MaterializedQuerySpec {
    let mut factory = CommandFactory::query()
        .with_event_type("orders")
        .with_time_field("timestamp");

    if let Some(s) = since {
        factory = factory.with_since(s);
    }

    MaterializedQuerySpec {
        name: "daily_orders".into(),
        query: Box::new(factory.create()),
    }
}

#[test]
fn plan_hash_is_stable() -> Result<(), MaterializationError> {
    let spec = build_spec();
    let hash1 = spec.plan_hash()?;
    let hash2 = spec.plan_hash()?;
    assert_eq!(hash1, hash2);
    Ok(())
}

#[test]
fn delta_command_no_watermark_returns_original() -> Result<(), MaterializationError> {
    let spec = build_spec();
    let original = spec.query().clone();
    let delta = spec.delta_command(None)?;
    assert_eq!(delta, original);
    Ok(())
}

#[test]
fn delta_command_with_watermark_sets_since() -> Result<(), MaterializationError> {
    let spec = build_spec();
    let watermark = HighWaterMark::new(1_700_000_000, 42);

    let command = spec.delta_command(Some(watermark))?;
    let Command::Query {
        since,
        where_clause,
        ..
    } = command
    else {
        panic!("expected query command");
    };

    assert_eq!(since, Some("1700000000".to_string()));
    assert!(where_clause.is_none());
    Ok(())
}

#[test]
fn delta_command_zero_watermark_returns_original() -> Result<(), MaterializationError> {
    let spec = build_spec();
    let watermark = HighWaterMark::default();
    let delta = spec.delta_command(Some(watermark))?;
    assert_eq!(delta, spec.query().clone());
    Ok(())
}

#[test]
fn delta_command_updates_older_existing_since() -> Result<(), MaterializationError> {
    let spec = build_spec_with_since(Some("1600000000"));
    let watermark = HighWaterMark::new(1_700_000_000, 1);

    let command = spec.delta_command(Some(watermark))?;
    let Command::Query { since, .. } = command else {
        panic!("expected query command");
    };
    assert_eq!(since, Some("1700000000".to_string()));
    Ok(())
}

#[test]
fn delta_command_preserves_newer_existing_since() -> Result<(), MaterializationError> {
    let spec = build_spec_with_since(Some("2024-01-01T00:00:00Z"));
    let watermark = HighWaterMark::new(1_700_000_000, 1); // earlier than 2024-01-01

    let command = spec.delta_command(Some(watermark))?;
    let Command::Query { since, .. } = command else {
        panic!("expected query command");
    };
    assert_eq!(since, Some("2024-01-01T00:00:00Z".to_string()));
    Ok(())
}
