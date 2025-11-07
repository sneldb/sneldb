use crate::command::handlers::query::orchestrator::QueryExecutionPipeline;
use crate::command::types::Command;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use std::sync::Arc;

fn base_command() -> Command {
    CommandFactory::query().with_event_type("evt").create()
}

#[test]
fn new_creates_pipeline_with_planner() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();
    let command = base_command();

    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));

    assert!(pipeline.streaming_supported());
}

#[test]
fn streaming_supported_returns_true_for_simple_query() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let command = base_command();
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(pipeline.streaming_supported());
}

#[test]
fn streaming_supported_returns_true_with_order_by() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let command = CommandFactory::query()
        .with_event_type("evt")
        .with_order_by("timestamp", false)
        .create();
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(pipeline.streaming_supported());
}

#[test]
fn streaming_supported_returns_false_with_aggregations() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let command = CommandFactory::query()
        .with_event_type("evt")
        .add_count()
        .create();
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(!pipeline.streaming_supported());
}

#[test]
fn streaming_supported_returns_false_with_time_bucket() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let command = CommandFactory::query()
        .with_event_type("evt")
        .with_time_bucket(crate::command::types::TimeGranularity::Hour)
        .create();
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(!pipeline.streaming_supported());
}

#[test]
fn streaming_supported_returns_false_with_group_by() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let command = CommandFactory::query()
        .with_event_type("evt")
        .with_group_by(vec!["field1"])
        .create();
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(!pipeline.streaming_supported());
}

#[test]
fn streaming_supported_returns_true_with_event_sequence() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let mut command = base_command();
    if let Command::Query { event_sequence, link_field, .. } = &mut command {
        *event_sequence = Some(crate::command::types::EventSequence {
            head: crate::command::types::EventTarget {
                event: "evt1".to_string(),
                field: None,
            },
            links: vec![(
                crate::command::types::SequenceLink::FollowedBy,
                crate::command::types::EventTarget {
                    event: "evt2".to_string(),
                    field: None,
                },
            )],
        });
        *link_field = Some("user_id".to_string());
    }
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    // Sequence queries now support streaming
    assert!(pipeline.streaming_supported());
}

#[test]
fn is_sequence_query_returns_true_for_sequence_query() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let mut command = base_command();
    if let Command::Query { event_sequence, link_field, .. } = &mut command {
        *event_sequence = Some(crate::command::types::EventSequence {
            head: crate::command::types::EventTarget {
                event: "evt1".to_string(),
                field: None,
            },
            links: vec![(
                crate::command::types::SequenceLink::FollowedBy,
                crate::command::types::EventTarget {
                    event: "evt2".to_string(),
                    field: None,
                },
            )],
        });
        *link_field = Some("user_id".to_string());
    }
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(pipeline.is_sequence_query());
}

#[test]
fn is_sequence_query_returns_false_for_regular_query() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let command = base_command();
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(!pipeline.is_sequence_query());
}

#[test]
fn is_sequence_query_returns_false_when_event_sequence_missing() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let mut command = base_command();
    if let Command::Query { link_field, .. } = &mut command {
        *link_field = Some("user_id".to_string());
    }
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(!pipeline.is_sequence_query());
}

#[test]
fn is_sequence_query_returns_false_when_link_field_missing() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let mut command = base_command();
    if let Command::Query { event_sequence, .. } = &mut command {
        *event_sequence = Some(crate::command::types::EventSequence {
            head: crate::command::types::EventTarget {
                event: "evt1".to_string(),
                field: None,
            },
            links: vec![(
                crate::command::types::SequenceLink::FollowedBy,
                crate::command::types::EventTarget {
                    event: "evt2".to_string(),
                    field: None,
                },
            )],
        });
    }
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(!pipeline.is_sequence_query());
}

#[test]
fn is_sequence_query_returns_true_for_preceded_by_sequence() {
    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let mut command = base_command();
    if let Command::Query { event_sequence, link_field, .. } = &mut command {
        *event_sequence = Some(crate::command::types::EventSequence {
            head: crate::command::types::EventTarget {
                event: "evt1".to_string(),
                field: None,
            },
            links: vec![(
                crate::command::types::SequenceLink::PrecededBy,
                crate::command::types::EventTarget {
                    event: "evt2".to_string(),
                    field: None,
                },
            )],
        });
        *link_field = Some("user_id".to_string());
    }
    let pipeline = QueryExecutionPipeline::new(&command, &manager, Arc::clone(&registry));
    assert!(pipeline.is_sequence_query());
}
