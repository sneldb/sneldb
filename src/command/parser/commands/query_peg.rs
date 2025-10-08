use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, SequenceLink, TimeGranularity,
};
use serde_json::Value;

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
            / limit_clause()

        rule clause_start()
            = ci("PER") / ci("BY") / ci("USING") / ci("SINCE") / ci("LIMIT")
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
            = ci("PER") _ unit:$("hour" / "day" / "week" / "month")
              _ using:(ci("USING") _ f:field() { f })? {
                let tg = match unit {
                    "hour" => TimeGranularity::Hour,
                    "day" => TimeGranularity::Day,
                    "week" => TimeGranularity::Week,
                    "month" => TimeGranularity::Month,
                    _ => unreachable!(),
                };
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
            / i:integer() { Value::Number(i.parse::<i64>().unwrap().into()) }
            / id:ident() { Value::String(id.to_string()) }

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
// BUILDERS
// =========

fn build_command(head: EventSequence, clauses: Vec<Clause>) -> Command {
    let mut context_id = None;
    let mut since = None;
    let mut return_fields = None;
    let mut link_field = None;
    let mut where_clause = None;
    let mut using_field = None;
    let mut aggs = None;
    let mut time_bucket = None;
    let mut group_by = None;
    let mut limit = None;

    for c in clauses {
        match c {
            Clause::For(v) => context_id = Some(v),
            Clause::Since(v) => since = Some(v),
            Clause::Return(v) => return_fields = Some(v),
            Clause::Link(v) => link_field = Some(v),
            Clause::Where(e) => where_clause = Some(e),
            Clause::Using(f) => using_field = Some(f),
            Clause::Aggs(a) => aggs = Some(a),
            Clause::Time(tg, uf) => {
                time_bucket = Some(tg);
                if let Some(fu) = uf {
                    using_field = Some(fu);
                }
            }
            Clause::Group(g, uf) => {
                group_by = Some(g);
                if let Some(fu) = uf {
                    using_field = Some(fu);
                }
            }
            Clause::Limit(n) => limit = Some(n),
        }
    }

    let event_type = head.head.event.clone();
    let event_sequence = if head.links.is_empty() {
        None
    } else {
        Some(head)
    };

    Command::Query {
        event_type,
        context_id,
        since,
        time_field: using_field,
        where_clause,
        limit,
        return_fields,
        link_field,
        aggs,
        time_bucket,
        group_by,
        event_sequence,
    }
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
    Limit(u32),
}

pub fn parse_query_peg(input: &str) -> Result<Command, peg::error::ParseError<peg::str::LineCol>> {
    sneldb_query::query(input)
}
