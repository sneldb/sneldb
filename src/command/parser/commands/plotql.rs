use crate::command::parser::error::ParseError;
use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, OrderSpec, SequenceLink,
    TimeGranularity,
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
              _ of_clause:(ci("OF") _ events:event_sequence() { events })?
              clauses:( _ clause:plot_clause() { clause } )* _ {
                PlotQueryInputs {
                    metric,
                    events: of_clause.unwrap_or_else(|| vec![]),
                    clauses,
                }
            }

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

        rule event_sequence() -> Vec<String>
            = head:identifier() tail:( _? "->" _? id:identifier() { id } )* {
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
            / compare_clause()

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

        rule compare_clause() -> PlotClause
            = ci("VS") _ metric:metric_expr() _ ci("OF") _ events:event_sequence() {
                PlotClause::Compare { metric, events }
            }

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
    let mut parts = PlotQueryParts::new(inputs.metric, inputs.events);
    for clause in inputs.clauses {
        parts.apply_clause(clause);
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
    clauses: Vec<PlotClause>,
}

#[derive(Debug)]
enum PlotClause {
    Breakdown(Vec<String>),
    Time(TimeGranularity, String),
    Filter(Expr),
    Top {
        value: u32,
        by: Option<TopByTarget>,
    },
    Compare {
        metric: MetricSpec,
        events: Vec<String>,
    },
}

#[derive(Debug)]
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
    metric: MetricSpec,
    events: Vec<String>,
    filter: Option<Expr>,
    breakdown: Option<Vec<String>>,
    time: Option<(TimeGranularity, String)>,
    top: Option<u32>,
    order_by: Option<OrderSpec>,
    compare: Option<(MetricSpec, Vec<String>)>,
}

impl PlotQueryParts {
    fn new(metric: MetricSpec, events: Vec<String>) -> Self {
        Self {
            metric,
            events,
            filter: None,
            breakdown: None,
            time: None,
            top: None,
            order_by: None,
            compare: None,
        }
    }

    fn apply_clause(&mut self, clause: PlotClause) {
        match clause {
            PlotClause::Breakdown(fields) => {
                self.breakdown = Some(fields);
            }
            PlotClause::Time(gran, field) => {
                self.time = Some((gran, field));
            }
            PlotClause::Filter(expr) => {
                self.filter = Some(match self.filter.take() {
                    Some(existing) => Expr::And(Box::new(existing), Box::new(expr)),
                    None => expr,
                });
            }
            PlotClause::Top { value, by } => {
                self.top = Some(value);
                // Default ordering: by main metric descending, unless overridden
                if let Some(target) = by {
                    self.order_by = Some(OrderSpec {
                        field: target.into_field_name(&self.metric),
                        desc: true,
                    });
                } else {
                    // Default: order by main metric descending
                    self.order_by = Some(OrderSpec {
                        field: self.metric.field_name(),
                        desc: true,
                    });
                }
            }
            PlotClause::Compare { metric, events } => {
                self.compare = Some((metric, events));
                // Note: Comparison queries will need special handling in the executor
                // For now, we store it but the executor will need to handle it
            }
        }
    }

    fn into_command(self) -> Command {
        let mut events_iter = self.events.into_iter();
        let event_type = events_iter.next().unwrap_or_else(|| "*".to_string());
        let remaining: Vec<String> = events_iter.collect();
        let event_sequence = if remaining.is_empty() {
            None
        } else {
            let mut links = Vec::with_capacity(remaining.len());
            for evt in remaining {
                links.push((
                    SequenceLink::FollowedBy,
                    EventTarget {
                        event: evt,
                        field: None,
                    },
                ));
            }
            Some(EventSequence {
                head: EventTarget {
                    event: event_type.clone(),
                    field: None,
                },
                links,
            })
        };

        let (time_bucket, time_field) = match self.time {
            Some((gran, field)) => (Some(gran), Some(field)),
            None => (None, None),
        };

        // If no explicit order_by but we have a top clause, default to ordering by metric
        let order_by = if self.order_by.is_none() && self.top.is_some() {
            Some(OrderSpec {
                field: self.metric.field_name(),
                desc: true,
            })
        } else {
            self.order_by
        };

        Command::Query {
            event_type,
            context_id: None,
            since: None,
            time_field,
            sequence_time_field: None,
            where_clause: self.filter,
            limit: self.top,
            offset: None,
            order_by,
            picked_zones: None,
            return_fields: None,
            link_field: None,
            aggs: Some(vec![self.metric.into()]),
            time_bucket,
            group_by: self.breakdown,
            event_sequence,
        }
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

impl TopByTarget {
    fn into_field_name(self, _main_metric: &MetricSpec) -> String {
        match self {
            TopByTarget::Field(f) => f,
            TopByTarget::Metric(metric) => metric.field_name(),
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
