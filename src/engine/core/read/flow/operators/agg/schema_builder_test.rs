use super::schema_builder::SchemaBuilder;
use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};

// Basic schema building tests -------------------------------------------------

#[test]
fn schema_builder_builds_count_all() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "count");
    assert_eq!(schema[0].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_count_field() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountField {
            field: "visits".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "count_visits");
    assert_eq!(schema[0].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_count_unique() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "count_unique_user_id_values");
    assert_eq!(schema[0].logical_type, "String");
}

#[test]
fn schema_builder_builds_total() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Total {
            field: "amount".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "total_amount");
    assert_eq!(schema[0].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_avg() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Avg {
            field: "amount".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name, "avg_amount_sum");
    assert_eq!(schema[0].logical_type, "Integer");
    assert_eq!(schema[1].name, "avg_amount_count");
    assert_eq!(schema[1].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_min() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Min {
            field: "score".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "min_score");
    assert_eq!(schema[0].logical_type, "String");
}

#[test]
fn schema_builder_builds_max() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Max {
            field: "name".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "max_name");
    assert_eq!(schema[0].logical_type, "String");
}

// Group by tests --------------------------------------------------------------

#[test]
fn schema_builder_builds_with_single_group_by() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: Some(vec!["region".to_string()]),
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name, "region");
    assert_eq!(schema[0].logical_type, "String");
    assert_eq!(schema[1].name, "count");
    assert_eq!(schema[1].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_with_multiple_group_by() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: Some(vec!["region".to_string(), "country".to_string()]),
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 3);
    assert_eq!(schema[0].name, "region");
    assert_eq!(schema[0].logical_type, "String");
    assert_eq!(schema[1].name, "country");
    assert_eq!(schema[1].logical_type, "String");
    assert_eq!(schema[2].name, "count");
    assert_eq!(schema[2].logical_type, "Integer");
}

// Time bucket tests -----------------------------------------------------------

#[test]
fn schema_builder_builds_with_time_bucket() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: None,
        time_bucket: Some(TimeGranularity::Hour),
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name, "bucket");
    assert_eq!(schema[0].logical_type, "Timestamp");
    assert_eq!(schema[1].name, "count");
    assert_eq!(schema[1].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_with_time_bucket_and_group_by() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: Some(vec!["region".to_string()]),
        time_bucket: Some(TimeGranularity::Day),
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 3);
    assert_eq!(schema[0].name, "bucket");
    assert_eq!(schema[0].logical_type, "Timestamp");
    assert_eq!(schema[1].name, "region");
    assert_eq!(schema[1].logical_type, "String");
    assert_eq!(schema[2].name, "count");
    assert_eq!(schema[2].logical_type, "Integer");
}

#[test]
fn schema_builder_builds_with_different_time_granularities() {
    let granularities = vec![
        TimeGranularity::Hour,
        TimeGranularity::Day,
        TimeGranularity::Week,
        TimeGranularity::Month,
        TimeGranularity::Year,
    ];

    for granularity in granularities {
        let plan = AggregatePlan {
            ops: vec![AggregateOpSpec::CountAll],
            group_by: None,
            time_bucket: Some(granularity),
        };

        let schema = SchemaBuilder::build(&plan);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "bucket");
        assert_eq!(schema[0].logical_type, "Timestamp");
    }
}

// Multiple aggregations tests -------------------------------------------------

#[test]
fn schema_builder_builds_with_multiple_aggregations() {
    let plan = AggregatePlan {
        ops: vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::CountField {
                field: "visits".to_string(),
            },
            AggregateOpSpec::Total {
                field: "amount".to_string(),
            },
        ],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 3);
    assert_eq!(schema[0].name, "count");
    assert_eq!(schema[1].name, "count_visits");
    assert_eq!(schema[2].name, "total_amount");
}

#[test]
fn schema_builder_builds_with_all_aggregation_types() {
    let plan = AggregatePlan {
        ops: vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::CountField {
                field: "visits".to_string(),
            },
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
            AggregateOpSpec::Total {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Avg {
                field: "quantity".to_string(),
            },
            AggregateOpSpec::Min {
                field: "score".to_string(),
            },
            AggregateOpSpec::Max {
                field: "name".to_string(),
            },
        ],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 8); // count, count_visits, count_unique_user_id_values, total_amount, avg_quantity_sum, avg_quantity_count, min_score, max_name
    assert_eq!(schema[0].name, "count");
    assert_eq!(schema[1].name, "count_visits");
    assert_eq!(schema[2].name, "count_unique_user_id_values");
    assert_eq!(schema[3].name, "total_amount");
    assert_eq!(schema[4].name, "avg_quantity_sum");
    assert_eq!(schema[5].name, "avg_quantity_count");
    assert_eq!(schema[6].name, "min_score");
    assert_eq!(schema[7].name, "max_name");
}

// Complex combination tests ----------------------------------------------------

#[test]
fn schema_builder_builds_complex_schema() {
    let plan = AggregatePlan {
        ops: vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
            AggregateOpSpec::Avg {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Total {
                field: "quantity".to_string(),
            },
        ],
        group_by: Some(vec!["region".to_string(), "country".to_string()]),
        time_bucket: Some(TimeGranularity::Day),
    };

    let schema = SchemaBuilder::build(&plan);
    // bucket, region, country, count, count_unique_user_id_values, avg_amount_sum, avg_amount_count, total_quantity
    assert_eq!(schema.len(), 8);
    assert_eq!(schema[0].name, "bucket");
    assert_eq!(schema[1].name, "region");
    assert_eq!(schema[2].name, "country");
    assert_eq!(schema[3].name, "count");
    assert_eq!(schema[4].name, "count_unique_user_id_values");
    assert_eq!(schema[5].name, "avg_amount_sum");
    assert_eq!(schema[6].name, "avg_amount_count");
    assert_eq!(schema[7].name, "total_quantity");
}

#[test]
fn schema_builder_builds_empty_ops() {
    let plan = AggregatePlan {
        ops: vec![],
        group_by: Some(vec!["region".to_string()]),
        time_bucket: Some(TimeGranularity::Hour),
    };

    let schema = SchemaBuilder::build(&plan);
    // Only bucket and region, no aggregation columns
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name, "bucket");
    assert_eq!(schema[1].name, "region");
}

#[test]
fn schema_builder_builds_only_time_bucket() {
    let plan = AggregatePlan {
        ops: vec![],
        group_by: None,
        time_bucket: Some(TimeGranularity::Hour),
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "bucket");
    assert_eq!(schema[0].logical_type, "Timestamp");
}

#[test]
fn schema_builder_builds_only_group_by() {
    let plan = AggregatePlan {
        ops: vec![],
        group_by: Some(vec!["region".to_string()]),
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "region");
    assert_eq!(schema[0].logical_type, "String");
}

#[test]
fn schema_builder_builds_scalar_only() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: None,
        time_bucket: None,
    };

    let schema = SchemaBuilder::build(&plan);
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "count");
    assert_eq!(schema[0].logical_type, "Integer");
}

// Column ordering tests -------------------------------------------------------

#[test]
fn schema_builder_orders_columns_correctly() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: Some(vec!["region".to_string(), "country".to_string()]),
        time_bucket: Some(TimeGranularity::Hour),
    };

    let schema = SchemaBuilder::build(&plan);
    // Order should be: bucket, region, country, count
    assert_eq!(schema.len(), 4);
    assert_eq!(schema[0].name, "bucket");
    assert_eq!(schema[1].name, "region");
    assert_eq!(schema[2].name, "country");
    assert_eq!(schema[3].name, "count");
}

