use crate::command::parser::error::ParseError;
use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, SequenceLink, TimeGranularity,
};
use serde_json::{Number, Value};

peg::parser! {
    grammar sneldb_query() for str {

        // ==========
        // HELPERS
        // ==========

        // Whitespace (accepts newlines, tabs, etc.)
        rule _() = quiet!{ [' ' | '\t' | '\n' | '\r']* }

        // Case-insensitive keyword matcher
        rule ci(s: &'static str)
            = kw:$(['a'..='z' | 'A'..='Z']+) {? if eq_ci(kw, s) { Ok(()) } else { Err("expected keyword") } }

        // ==========
        // ENTRY POINT
        // ==========

        pub rule query() -> Command
            = _ query_kw() _ head:event_sequence() _ clauses:( _ c:clause() { c } )* _ {
                build_command(head, clauses)
            }

        rule query_kw() = ci("QUERY") / ci("FIND")

        // ==========
        // EVENT SEQUENCE
        // ==========

        rule event_sequence() -> EventSequence
            = head:ident() tail:( _ l:seq_link() _ t:ident() { (l, t) } )* {
                let mut links = Vec::new();
                for (l, t) in tail {
                    links.push((l, EventTarget { event: t.to_string(), field: None }));
                }
                EventSequence {
                    head: EventTarget { event: head.to_string(), field: None },
                    links,
                }
            }

        rule seq_link() -> SequenceLink
            = ci("FOLLOWED") _ ci("BY") { SequenceLink::FollowedBy }
            / ci("PRECEDED") _ ci("BY") { SequenceLink::PrecededBy }

        // ==========
        // CLAUSES
        // ==========

        rule clause() -> Clause
            = for_clause()
            / since_clause()
            / return_clause()
            / linked_clause()
            / where_clause()
            / using_clause()
            / agg_clause()
            / time_clause()
            / group_clause()
            / offset_clause()
            / limit_clause()

        rule clause_start()
            = ci("PER") / ci("BY") / ci("USING") / ci("SINCE") / ci("LIMIT") / ci("OFFSET")
            / ci("RETURN") / ci("LINKED") / ci("WHERE") / ci("FOR")
            / ci("FOLLOWED") / ci("PRECEDED")

        rule for_clause() -> Clause
            = ci("FOR") _ id:(ident() / string_literal()) {
                Clause::For(id.to_string())
            }

        rule since_clause() -> Clause
            = ci("SINCE") _ ts:string_literal() {
                Clause::Since(ts.to_string())
            }

        rule return_clause() -> Clause
            = ci("RETURN") _ "[" _ fields:( return_item() ** (_ "," _) )? _ "]" {
                Clause::Return(fields.unwrap_or_default())
            }

        rule return_item() -> String
            = f:field() { f }
            / s:string_literal() { s.to_string() }

        rule linked_clause() -> Clause
            = ci("LINKED") _ ci("BY") _ id:ident() {
                Clause::Link(id.to_string())
            }

        rule where_clause() -> Clause
            = ci("WHERE") _ e:expr() {
                Clause::Where(e)
            }

        rule using_clause() -> Clause
            = ci("USING") _ fld:field() {
                Clause::Using(fld)
            }

        // ==========
        // AGGREGATIONS
        // ==========

        rule agg_clause() -> Clause
            = specs:( agg_spec() ++ (_ "," _) ) {
                Clause::Aggs(specs)
            }

        rule agg_spec() -> AggSpec
            = ci("COUNT") _ ci("UNIQUE") _ !(clause_start()) fld:field() {
                AggSpec::Count { unique_field: Some(fld) }
            }
            / ci("COUNT") _ !(clause_start()) fld:field() {
                AggSpec::CountField { field: fld }
            }
            / ci("COUNT") { AggSpec::Count { unique_field: None } }
            / ci("TOTAL") _ !(clause_start()) fld:field() {
                AggSpec::Total { field: fld }
            }
            / ci("AVG") _ !(clause_start()) fld:field() {
                AggSpec::Avg { field: fld }
            }
            / ci("MIN") _ !(clause_start()) fld:field() {
                AggSpec::Min { field: fld }
            }
            / ci("MAX") _ !(clause_start()) fld:field() {
                AggSpec::Max { field: fld }
            }

        // ==========
        // TIME & GROUPING
        // ==========

        rule time_clause() -> Clause
            = ci("PER") _ tg:(
                  ci("HOUR")  { TimeGranularity::Hour }
                / ci("DAY")   { TimeGranularity::Day }
                / ci("WEEK")  { TimeGranularity::Week }
                / ci("MONTH") { TimeGranularity::Month }
              )
              _ using:(ci("USING") _ f:field() { f })? {
                Clause::Time(tg, using)
            }

        rule group_clause() -> Clause
            = ci("BY") _ first:field()
              rest:( _ "," _ f:field() { f } )*
              using:(ci("USING") _ f:field() { f })? {
                let mut v = Vec::new();
                v.push(first);
                for fld in rest { v.push(fld); }
                Clause::Group(v, using)
            }

        rule offset_clause() -> Clause
            = ci("OFFSET") _ n:integer() {
                Clause::Offset(n.parse::<u32>().unwrap())
            }

        rule limit_clause() -> Clause
            = ci("LIMIT") _ n:integer() {
                Clause::Limit(n.parse::<u32>().unwrap())
            }

        // ==========
        // EXPRESSIONS
        // ==========

        rule expr() -> Expr = or_expr()

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
            = ci("NOT") _ x:factor() { Expr::Not(Box::new(x)) }
            / comparison()
            / atom()

        rule atom() -> Expr
            = f:field() {
                Expr::Compare {
                    field: f,
                    op: CompareOp::Eq,
                    value: Value::Bool(true)
                }
            }

        rule comparison() -> Expr
            = f:field() _ op:cmp_op() _ v:value() {
                Expr::Compare { field: f, op, value: v }
            }

        rule cmp_op() -> CompareOp
            = "!=" { CompareOp::Neq }
            / ">=" { CompareOp::Gte }
            / "<=" { CompareOp::Lte }
            / "=" { CompareOp::Eq }
            / ">" { CompareOp::Gt }
            / "<" { CompareOp::Lt }

        // ==========
        // TERMINALS
        // ==========

        rule value() -> Value
            = s:string_literal() { Value::String(s.to_string()) }
            / n:number() { n }
            / id:ident() { Value::String(id.to_string()) }

        // Accept integers and decimals; choose int when no dot for stable equality in tests
        rule number() -> Value
            = n:$( ("-")? ['0'..='9']+ ( "." ['0'..='9']+ )? ) {
                if n.contains('.') {
                    let f: f64 = n.parse::<f64>().unwrap();
                    Value::Number(Number::from_f64(f).unwrap())
                } else {
                    let i: i64 = n.parse::<i64>().unwrap();
                    Value::Number(i.into())
                }
            }

        rule field() -> String
            = i:ident() "." j:ident() { format!("{}.{}", i, j) }
            / i:ident() { i.to_string() }

        rule ident() -> &'input str
            = quiet!{
                $( ['a'..='z' | 'A'..='Z' | '_']
                   ['a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-']* )
            }
            / expected!("identifier")

        rule string_literal() -> &'input str
            = "\"" chars:$((!"\"" [_])*) "\"" { chars }

        rule integer() -> &'input str
            = quiet!{ $(("-")? ['0'..='9']+) } / expected!("integer")
    }
}

// =========
// HELPERS
// =========

fn eq_ci(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

// =========
// STATE/BUILDERS
// =========

#[derive(Default)]
struct QueryParts {
    context_id: Option<String>,
    since: Option<String>,
    return_fields: Option<Vec<String>>,
    link_field: Option<String>,
    where_clause: Option<Expr>,
    using_field: Option<String>,
    aggs: Option<Vec<AggSpec>>,
    time_bucket: Option<TimeGranularity>,
    group_by: Option<Vec<String>>,
    offset: Option<u32>,
    limit: Option<u32>,
}

impl QueryParts {
    fn apply_clause(&mut self, clause: Clause) {
        match clause {
            Clause::For(v) => self.context_id = Some(v),
            Clause::Since(v) => self.since = Some(v),
            Clause::Return(v) => self.return_fields = Some(v),
            Clause::Link(v) => self.link_field = Some(v),
            Clause::Where(e) => self.where_clause = Some(e),
            Clause::Using(f) => self.using_field = Some(f),
            Clause::Aggs(a) => self.aggs = Some(a),
            Clause::Time(tg, uf) => {
                self.time_bucket = Some(tg);
                if let Some(fu) = uf {
                    self.using_field = Some(fu);
                }
            }
            Clause::Group(g, uf) => {
                self.group_by = Some(g);
                if let Some(fu) = uf {
                    self.using_field = Some(fu);
                }
            }
            Clause::Offset(n) => self.offset = Some(n),
            Clause::Limit(n) => self.limit = Some(n),
        }
    }

    fn into_command(self, event_type: String, event_sequence: Option<EventSequence>) -> Command {
        Command::Query {
            event_type,
            context_id: self.context_id,
            since: self.since,
            time_field: self.using_field,
            where_clause: self.where_clause,
            offset: self.offset,
            limit: self.limit,
            return_fields: self.return_fields,
            link_field: self.link_field,
            aggs: self.aggs,
            time_bucket: self.time_bucket,
            group_by: self.group_by,
            event_sequence,
        }
    }
}

fn build_command(head: EventSequence, clauses: Vec<Clause>) -> Command {
    let mut parts = QueryParts::default();
    for c in clauses {
        parts.apply_clause(c);
    }

    let event_type = head.head.event.clone();
    let event_sequence = if head.links.is_empty() {
        None
    } else {
        Some(head)
    };

    parts.into_command(event_type, event_sequence)
}

#[derive(Debug)]
enum Clause {
    For(String),
    Since(String),
    Return(Vec<String>),
    Link(String),
    Where(Expr),
    Using(String),
    Aggs(Vec<AggSpec>),
    Time(TimeGranularity, Option<String>),
    Group(Vec<String>, Option<String>),
    Offset(u32),
    Limit(u32),
}

pub fn parse(input: &str) -> Result<Command, ParseError> {
    sneldb_query::query(input).map_err(map_peg_error)
}

fn map_peg_error(e: peg::error::ParseError<peg::str::LineCol>) -> ParseError {
    // Preserve old dispatcher_tests expectations (prefix and details)
    ParseError::UnexpectedToken(format!("PEG parse error: {}", e))
}
