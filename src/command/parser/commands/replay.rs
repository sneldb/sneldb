use crate::command::parser::error::ParseError;
use crate::command::types::Command;

peg::parser! {
    grammar sneldb_replay() for str {

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

        pub rule replay() -> Command
            = _ ci("REPLAY") _ event_type:event_type_opt() ci("FOR") _ context_id:context_id_val()
              clauses:( _ c:clause() { c } )* _ {
                build_command(event_type, context_id, clauses)
            }

        // Optional event_type, but NOT if it's "FOR"
        rule event_type_opt() -> Option<&'input str>
            = !ci("FOR") i:ident() _ { Some(i) }
            / { None }

        // ==========
        // CLAUSES
        // ==========

        rule clause() -> Clause
            = since_clause()
            / return_clause()
            / using_clause()

        rule since_clause() -> Clause
            = ci("SINCE") _ ts:string_literal() {
                Clause::Since(ts.to_string())
            }

        rule return_clause() -> Clause
            = ci("RETURN") _ "[" _ fields:( return_item() ** (_ "," _) )? _ "]" {
                Clause::Return(fields.unwrap_or_default())
            }

        rule return_item() -> String
            = i:ident() { i.to_string() }
            / s:string_literal() { s.to_string() }

        rule using_clause() -> Clause
            = ci("USING") _ fld:ident() {
                Clause::Using(fld.to_string())
            }

        // ==========
        // TERMINALS
        // ==========

        rule context_id_val() -> String
            = s:string_literal() { s.to_string() }
            / i:ident() { i.to_string() }

        rule ident() -> &'input str
            = quiet!{
                $( ['a'..='z' | 'A'..='Z' | '_']
                   ['a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | ':']* )
            }
            / expected!("identifier")

        rule string_literal() -> &'input str
            = "\"" chars:$((!"\"" [_])*) "\"" { chars }
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

#[derive(Debug)]
enum Clause {
    Since(String),
    Return(Vec<String>),
    Using(String),
}

fn build_command(
    event_type: Option<&str>,
    context_id: String,
    clauses: Vec<Clause>,
) -> Command {
    let mut since = None;
    let mut return_fields = None;
    let mut time_field = None;

    for clause in clauses {
        match clause {
            Clause::Since(v) => since = Some(v),
            Clause::Return(v) => return_fields = Some(v),
            Clause::Using(v) => time_field = Some(v),
        }
    }

    Command::Replay {
        event_type: event_type.map(|s| s.to_string()),
        context_id,
        since,
        time_field,
        return_fields,
    }
}

pub fn parse(input: &str) -> Result<Command, ParseError> {
    sneldb_replay::replay(input).map_err(map_peg_error)
}

fn map_peg_error(e: peg::error::ParseError<peg::str::LineCol>) -> ParseError {
    ParseError::UnexpectedToken(format!("PEG parse error: {}", e))
}
