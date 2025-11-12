use crate::command::parser::error::ParseError;
use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, OrderSpec, QueryCommand,
    SequenceLink, TimeGranularity,
};
use serde_json::{Number, Value};

peg::parser! {
    grammar plotql_parser() for str {
        // ==========
        // HELPERS
        // ==========

        rule whitespace() = quiet!{ [' ' | '\t' | '\n' | '\r']+ }
        rule _() = quiet!{ [' ' | '\t' | '\n' | '\r']* }

        rule ci(s: &'static str)
            = kw:$(['a'..='z' | 'A'..='Z']+) {? if eq_ci(kw, s) { Ok(()) } else { Err("expected keyword") } }

        // ==========
        // ENTRY POINT
        // ==========

        pub rule plot_query() -> PlotQueryInputs
            = _ ci("PLOT") _ metric:metric_expr()
              _ ci("OF") _ events:event_sequence()
              before_vs_clauses:( _ clause:plot_clause_before_vs() { clause } )*
              comparison_sides:( _ ci("VS") _ side:comparison_side() { side } )*
              after_vs_clauses:( _ clause:plot_clause_after_vs() { clause } )* _ {
                PlotQueryInputs {
                    metric,
                    events,
                    before_vs_clauses,
                    comparison_sides,
                    after_vs_clauses,
                }
            }

        rule comparison_side() -> ComparisonSide
            = metric:metric_expr() _ ci("OF") _ events:event_sequence()
              clauses:( _ clause:plot_clause_before_vs() { clause } )* {
                ComparisonSide {
                    metric,
                    events,
                    clauses,
                }
            }

        rule plot_clause_before_vs() -> PlotClause
            = filter_clause()
            / top_clause()
            // breakdown_clause() removed - breakdown must come after vs to be shared

        rule plot_clause_after_vs() -> PlotClause
            = breakdown_clause()
            / time_clause()
            / top_clause()

        // ==========
        // METRIC EXPRESSION
        // ==========

        rule metric_expr() -> MetricSpec
            = func:agg_func() _ "(" _ field:field() _ ")" {
                match func {
                    AggFunc::Total => MetricSpec::Total(field),
                    AggFunc::Sum => MetricSpec::Total(field),
                    AggFunc::Avg => MetricSpec::Avg(field),
                    AggFunc::Min => MetricSpec::Min(field),
                    AggFunc::Max => MetricSpec::Max(field),
                }
            }
            / ci("COUNT") _ "(" _ field:field() _ ")" { MetricSpec::CountField(field) }
            / ci("COUNT") { MetricSpec::CountAll }
            / ci("UNIQUE") _ "(" _ field:field() _ ")" { MetricSpec::CountUnique(field) }

        rule agg_func() -> AggFunc
            = ci("TOTAL") { AggFunc::Total }
            / ci("SUM") { AggFunc::Sum }
            / ci("AVG") { AggFunc::Avg }
            / ci("MIN") { AggFunc::Min }
            / ci("MAX") { AggFunc::Max }

        // ==========
        // EVENT SEQUENCE
        // ==========

        rule sequence_separator()
            = "->" / ci("THEN")

        rule event_sequence() -> Vec<String>
            = head:identifier() tail:( _? sequence_separator() _? id:identifier() { id } )* {
                let mut events = Vec::with_capacity(1 + tail.len());
                events.push(head.to_string());
                for t in tail {
                    events.push(t.to_string());
                }
                events
            }

        // ==========
        // CLAUSES
        // ==========

        rule plot_clause() -> PlotClause
            = breakdown_clause()
            / time_clause()
            / filter_clause()
            / top_clause()

        rule breakdown_clause() -> PlotClause
            = ci("BREAKDOWN") _ ci("BY") _ fields:field_list() {
                PlotClause::Breakdown(fields)
            }

        rule time_clause() -> PlotClause
            = ci("OVER") _ gran:time_granularity() _ "(" _ field:field() _ ")" {
                PlotClause::Time(gran, field)
            }

        rule filter_clause() -> PlotClause
            = ci("FILTER") _ expr:expression() {
                PlotClause::Filter(expr)
            }

        rule top_clause() -> PlotClause
            = ci("TOP") _ n:integer()
              by:( _ ci("BY") _ target:top_by_target() { target } )? {
                PlotClause::Top { value: n, by }
            }

        rule top_by_target() -> TopByTarget
            = m:metric_expr() { TopByTarget::Metric(m) }
            / f:field() { TopByTarget::Field(f) }


        // ==========
        // EXPRESSIONS
        // ==========

        rule expression() -> Expr = or_expr()

        rule or_expr() -> Expr
            = x:and_expr() _ ci("OR") _ y:or_expr() {
                Expr::Or(Box::new(x), Box::new(y))
            }
            / and_expr()

        rule and_expr() -> Expr
            = x:factor() _ ci("AND") _ y:and_expr() {
                Expr::And(Box::new(x), Box::new(y))
            }
            / factor()

        rule factor() -> Expr
            = ci("NOT") _ f:factor() { Expr::Not(Box::new(f)) }
            / "(" _ e:expression() _ ")" { e }
            / comparison()
            / in_expr()
            / exists_expr()

        rule comparison() -> Expr
            = fld:field() _ op:comparison_op() _ val:value() {
                Expr::Compare { field: fld, op, value: val }
            }

        rule in_expr() -> Expr
            = fld:field() _ ci("IN") _ "(" _ vals:value_list() _ ")" {
                Expr::In { field: fld, values: vals }
            }

        rule exists_expr() -> Expr
            = ci("NOT") _ ci("EXISTS") _ "(" _ id:identifier() _ ")" {
                // For now, convert NOT EXISTS to a comparison that will need special handling
                // TODO: Add Expr::Exists variant to support this properly
                Expr::Compare {
                    field: format!("exists({})", id),
                    op: CompareOp::Eq,
                    value: Value::Bool(false),
                }
            }
            / ci("EXISTS") _ "(" _ id:identifier() _ ")" {
                // For now, convert EXISTS to a comparison that will need special handling
                // TODO: Add Expr::Exists variant to support this properly
                Expr::Compare {
                    field: format!("exists({})", id),
                    op: CompareOp::Eq,
                    value: Value::Bool(true),
                }
            }

        rule comparison_op() -> CompareOp
            = "=" { CompareOp::Eq }
            / "!=" { CompareOp::Neq }
            / ">=" { CompareOp::Gte }
            / "<=" { CompareOp::Lte }
            / ">" { CompareOp::Gt }
            / "<" { CompareOp::Lt }

        // ==========
        // TERMINALS
        // ==========

        rule field_list() -> Vec<String>
            = head:field() tail:( _? "," _? f:field() { f } )* {
                let mut list = Vec::with_capacity(1 + tail.len());
                list.push(head);
                for f in tail {
                    list.push(f);
                }
                list
            }

        rule field() -> String
            = a:identifier() "." b:identifier() { format!("{}.{}", a, b) }
            / id:identifier() { id.to_string() }

        rule value_list() -> Vec<Value>
            = head:value() tail:( _? "," _? v:value() { v } )* {
                let mut vals = Vec::with_capacity(1 + tail.len());
                vals.push(head);
                for v in tail {
                    vals.push(v);
                }
                vals
            }

        rule value() -> Value
            = s:string_literal() { Value::String(s.to_string()) }
            / n:number() { n }
            / id:identifier() { Value::String(id.to_string()) }

        rule number() -> Value
            = n:$(( "-")? ['0'..='9']+ ( "." ['0'..='9']+ )? ) {? parse_json_number(n) }

        rule string_literal() -> &'input str
            = "\"" chars:$((!"\"" [_])*) "\"" { chars }

        rule time_granularity() -> TimeGranularity
            = ci("HOUR")  { TimeGranularity::Hour }
            / ci("DAY")   { TimeGranularity::Day }
            / ci("WEEK")  { TimeGranularity::Week }
            / ci("MONTH") { TimeGranularity::Month }
            / ci("YEAR")  { TimeGranularity::Year }

        rule identifier() -> &'input str
            = quiet!{
                $( ['a'..='z' | 'A'..='Z' | '_']
                        ( ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']+
                   / ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']* "-" ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']+ )*
                   ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']?
                )
              }
            / expected!("identifier")

        rule integer() -> u32
            = n:$(( "-")? ['0'..='9']+ ) {?
                let value: i64 = n.parse().map_err(|_| "integer")?;
                if value < 0 {
                    Err("integer must be non-negative")
                } else {
                    Ok(value as u32)
                }
            }
    }
}

pub fn parse(input: &str) -> Result<Command, ParseError> {
    let inputs = plotql_parser::plot_query(input).map_err(map_peg_error)?;

    // Validate that all metrics are the same
    for side in &inputs.comparison_sides {
        if !inputs.metric.equals(&side.metric) {
            return Err(ParseError::UnexpectedToken(
                "All sides of a comparison query must use the same metric function".to_string(),
            ));
        }
    }

    let mut parts = PlotQueryParts::new(inputs.metric, inputs.events);

    // Apply clauses before vs to main query
    for clause in inputs.before_vs_clauses {
        parts.apply_clause_to_main(clause);
    }

    // Process comparison sides
    for side in inputs.comparison_sides {
        let mut side_parts = ComparisonSideParts {
            metric: side.metric,
            events: side.events,
            filter: None,
            breakdown: None,
            top: None,
            top_by_target: None,
        };

        for clause in side.clauses {
            side_parts.apply_clause(clause);
        }

        parts.comparison_sides.push(side_parts);
    }

    // Apply shared clauses (after vs)
    for clause in inputs.after_vs_clauses {
        parts.apply_shared_clause(clause);
    }

    Ok(parts.into_command())
}

fn map_peg_error(e: peg::error::ParseError<peg::str::LineCol>) -> ParseError {
    ParseError::UnexpectedToken(format!("PlotQL parse error: {}", e))
}

#[derive(Debug)]
struct PlotQueryInputs {
    metric: MetricSpec,
    events: Vec<String>,
    before_vs_clauses: Vec<PlotClause>,
    comparison_sides: Vec<ComparisonSide>,
    after_vs_clauses: Vec<PlotClause>,
}

#[derive(Debug)]
struct ComparisonSide {
    metric: MetricSpec,
    events: Vec<String>,
    clauses: Vec<PlotClause>,
}

#[derive(Debug)]
enum PlotClause {
    Breakdown(Vec<String>),
    Time(TimeGranularity, String),
    Filter(Expr),
    Top { value: u32, by: Option<TopByTarget> },
}

#[derive(Debug, Clone)]
enum TopByTarget {
    Field(String),
    Metric(MetricSpec),
}

#[derive(Debug, Clone)]
enum MetricSpec {
    CountAll,
    CountField(String),
    CountUnique(String),
    Total(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Debug)]
enum AggFunc {
    Total,
    Sum,
    Avg,
    Min,
    Max,
}

struct PlotQueryParts {
    // Main query side
    metric: MetricSpec,
    events: Vec<String>,
    filter: Option<Expr>,
    breakdown: Option<Vec<String>>,
    top: Option<u32>,
    top_by_target: Option<TopByTarget>,

    // Comparison sides (N-way)
    comparison_sides: Vec<ComparisonSideParts>,

    // Shared clauses (apply to all sides)
    shared_time: Option<(TimeGranularity, String)>,
    shared_breakdown: Option<Vec<String>>,
    shared_top: Option<u32>,
    shared_top_by_target: Option<TopByTarget>,
}

struct ComparisonSideParts {
    metric: MetricSpec,
    events: Vec<String>,
    filter: Option<Expr>,
    breakdown: Option<Vec<String>>,
    top: Option<u32>,
    top_by_target: Option<TopByTarget>,
}

impl ComparisonSideParts {
    fn apply_clause(&mut self, clause: PlotClause) {
        match clause {
            PlotClause::Breakdown(fields) => {
                self.breakdown = Some(fields);
            }
            PlotClause::Filter(expr) => {
                self.filter = Some(match self.filter.take() {
                    Some(existing) => Expr::And(Box::new(existing), Box::new(expr)),
                    None => expr,
                });
            }
            PlotClause::Top { value, by } => {
                self.top = Some(value);
                self.top_by_target = by;
            }
            PlotClause::Time(_, _) => {
                // Time clause should not appear before vs
            }
        }
    }
}

impl PlotQueryParts {
    fn new(metric: MetricSpec, events: Vec<String>) -> Self {
        Self {
            metric,
            events,
            filter: None,
            breakdown: None,
            top: None,
            top_by_target: None,
            comparison_sides: Vec::new(),
            shared_time: None,
            shared_breakdown: None,
            shared_top: None,
            shared_top_by_target: None,
        }
    }

    fn apply_clause_to_main(&mut self, clause: PlotClause) {
        match clause {
            PlotClause::Breakdown(fields) => {
                self.breakdown = Some(fields);
            }
            PlotClause::Filter(expr) => {
                self.filter = Some(match self.filter.take() {
                    Some(existing) => Expr::And(Box::new(existing), Box::new(expr)),
                    None => expr,
                });
            }
            PlotClause::Top { value, by } => {
                self.top = Some(value);
                self.top_by_target = by;
            }
            PlotClause::Time(_, _) => {
                // Time clause should not appear before vs
            }
        }
    }

    fn apply_shared_clause(&mut self, clause: PlotClause) {
        match clause {
            PlotClause::Breakdown(fields) => {
                self.shared_breakdown = Some(fields);
            }
            PlotClause::Time(gran, field) => {
                self.shared_time = Some((gran, field));
            }
            PlotClause::Top { value, by } => {
                self.shared_top = Some(value);
                self.shared_top_by_target = by;
            }
            PlotClause::Filter(_) => {
                // Filters are per-side, not shared
            }
        }
    }

    fn into_command(self) -> Command {
        // Extract all values before moving self
        let main_metric = self.metric.clone();
        let main_events = self.events.clone();
        let main_filter = self.filter.clone();
        let main_breakdown = self.breakdown.clone();
        let main_top = self.top;
        let main_top_by_target = self.top_by_target.clone();

        let shared_time = self.shared_time.clone();
        let shared_breakdown = self.shared_breakdown.clone();
        let shared_top = self.shared_top;
        let shared_top_by_target = self.shared_top_by_target.clone();

        let comparison_sides = self.comparison_sides;

        // Build main query as QueryCommand
        let main_query = Self::build_query_command_static(
            main_metric,
            main_events,
            main_filter,
            main_breakdown,
            main_top,
            main_top_by_target,
            &shared_time,
            &shared_breakdown,
            shared_top,
            &shared_top_by_target,
        );

        // If no comparison sides, return as regular Query
        if comparison_sides.is_empty() {
            return Command::from(main_query);
        }

        // Build comparison queries
        let mut queries = Vec::with_capacity(1 + comparison_sides.len());
        queries.push(main_query);

        for side in comparison_sides {
            queries.push(Self::build_query_command_static(
                side.metric,
                side.events,
                side.filter,
                side.breakdown,
                side.top,
                side.top_by_target,
                &shared_time,
                &shared_breakdown,
                shared_top,
                &shared_top_by_target,
            ));
        }

        Command::Compare { queries }
    }

    fn build_query_command_static(
        metric: MetricSpec,
        events: Vec<String>,
        filter: Option<Expr>,
        breakdown: Option<Vec<String>>,
        top: Option<u32>,
        top_by_target: Option<TopByTarget>,
        shared_time: &Option<(TimeGranularity, String)>,
        shared_breakdown: &Option<Vec<String>>,
        shared_top: Option<u32>,
        shared_top_by_target: &Option<TopByTarget>,
    ) -> QueryCommand {
        let (event_type, event_sequence) = Self::build_event_sequence(events);

        let (time_bucket, time_field) = match shared_time {
            Some((gran, field)) => (Some(gran.clone()), Some(field.clone())),
            None => (None, None),
        };

        // Use shared breakdown/time/top if present, otherwise use per-side values
        let breakdown = shared_breakdown.clone().or(breakdown);
        let top = shared_top.or(top);
        let top_by_target = shared_top_by_target.clone().or(top_by_target);

        // Build aggs list: start with metric, add ordering metric if different
        let mut aggs = vec![metric.clone().into()];
        let order_by = Self::build_order_spec_for_metrics(&metric, top, &top_by_target, &mut aggs);

        QueryCommand {
            event_type,
            context_id: None,
            since: None,
            time_field,
            sequence_time_field: None,
            where_clause: filter,
            limit: top,
            offset: None,
            order_by,
            picked_zones: None,
            return_fields: None,
            link_field: None,
            aggs: Some(aggs),
            time_bucket,
            group_by: breakdown,
            event_sequence,
        }
    }

    fn build_event_sequence(events: Vec<String>) -> (String, Option<EventSequence>) {
        let mut events_iter = events.into_iter();
        let event_type = events_iter.next().unwrap_or_else(|| "*".to_string());
        let remaining: Vec<String> = events_iter.collect();

        if remaining.is_empty() {
            (event_type, None)
        } else {
            let links: Vec<(SequenceLink, EventTarget)> = remaining
                .into_iter()
                .map(|evt| {
                    (
                        SequenceLink::FollowedBy,
                        EventTarget {
                            event: evt,
                            field: None,
                        },
                    )
                })
                .collect();

            let event_sequence = EventSequence {
                head: EventTarget {
                    event: event_type.clone(),
                    field: None,
                },
                links,
            };

            (event_type, Some(event_sequence))
        }
    }

    /// Builds the OrderSpec and ensures the ordering metric is in the aggs list.
    /// If ordering by a metric that differs from the main metric, adds it to aggs.
    fn build_order_spec_for_metrics(
        main_metric: &MetricSpec,
        top: Option<u32>,
        top_by_target: &Option<TopByTarget>,
        aggs: &mut Vec<AggSpec>,
    ) -> Option<OrderSpec> {
        if top.is_none() {
            return None;
        }

        let order_by = match top_by_target {
            Some(TopByTarget::Field(field)) => {
                // Ordering by a field (column) - no need to add to aggs
                OrderSpec {
                    field: field.clone(),
                    desc: true,
                }
            }
            Some(TopByTarget::Metric(metric)) => {
                // Ordering by a metric - ensure it's in aggs if different from main metric
                if !main_metric.equals(metric) {
                    aggs.push(metric.clone().into());
                }
                OrderSpec {
                    field: metric.field_name(),
                    desc: true,
                }
            }
            None => {
                // Default: order by main metric descending
                OrderSpec {
                    field: main_metric.field_name(),
                    desc: true,
                }
            }
        };

        Some(order_by)
    }
}

impl From<MetricSpec> for AggSpec {
    fn from(metric: MetricSpec) -> Self {
        match metric {
            MetricSpec::CountAll => AggSpec::Count { unique_field: None },
            MetricSpec::CountField(field) => AggSpec::CountField { field },
            MetricSpec::CountUnique(field) => AggSpec::Count {
                unique_field: Some(field),
            },
            MetricSpec::Total(field) => AggSpec::Total { field },
            MetricSpec::Avg(field) => AggSpec::Avg { field },
            MetricSpec::Min(field) => AggSpec::Min { field },
            MetricSpec::Max(field) => AggSpec::Max { field },
        }
    }
}

impl MetricSpec {
    fn field_name(&self) -> String {
        match self {
            MetricSpec::CountAll => "count".to_string(),
            MetricSpec::CountField(field) => format!("count_{}", field),
            MetricSpec::CountUnique(field) => format!("count_unique_{}", field),
            MetricSpec::Total(field) => format!("total_{}", field),
            MetricSpec::Avg(field) => format!("avg_{}", field),
            MetricSpec::Min(field) => format!("min_{}", field),
            MetricSpec::Max(field) => format!("max_{}", field),
        }
    }

    /// Checks if two MetricSpec values are equal.
    fn equals(&self, other: &MetricSpec) -> bool {
        match (self, other) {
            (MetricSpec::CountAll, MetricSpec::CountAll) => true,
            (MetricSpec::CountField(a), MetricSpec::CountField(b)) => a == b,
            (MetricSpec::CountUnique(a), MetricSpec::CountUnique(b)) => a == b,
            (MetricSpec::Total(a), MetricSpec::Total(b)) => a == b,
            (MetricSpec::Avg(a), MetricSpec::Avg(b)) => a == b,
            (MetricSpec::Min(a), MetricSpec::Min(b)) => a == b,
            (MetricSpec::Max(a), MetricSpec::Max(b)) => a == b,
            _ => false,
        }
    }
}

fn eq_ci(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn parse_json_number(raw: &str) -> Result<Value, &'static str> {
    if raw.contains('.') {
        let f: f64 = raw.parse().map_err(|_| "number")?;
        Number::from_f64(f).map(Value::Number).ok_or("number")
    } else {
        let i: i64 = raw.parse().map_err(|_| "number")?;
        Ok(Value::Number(i.into()))
    }
}
