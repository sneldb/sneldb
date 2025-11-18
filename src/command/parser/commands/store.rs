use crate::command::parser::error::ParseError;
use crate::command::types::Command;
use serde_json::Value;

// Fast PEG-based parser for STORE commands (extracts JSON directly without tokenization)
peg::parser! {
    grammar sneldb_store() for str {
        rule _() = quiet!{ [' ' | '\t' | '\n' | '\r']* }

        rule ci(s: &'static str) -> ()
            = kw:$(['a'..='z' | 'A'..='Z']+) {?
                if kw.eq_ignore_ascii_case(s) { Ok(()) }
                else { Err("expected keyword") }
            }

        rule ident() -> &'input str
            = quiet!{
                $( ['a'..='z' | 'A'..='Z' | '_']
                   ['a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-']* )
            }
            / expected!("identifier")

        rule string_literal() -> &'input str
            = "\"" chars:$((!"\"" [_])*) "\"" { chars }

        // Extract JSON block directly as string slice (much faster than tokenizing and rebuilding)
        // Matches balanced braces and captures the entire JSON object as a string slice
        rule json_block() -> &'input str
            = _ json:$(balanced_braces()) {
                json
            }

        // Match balanced braces and capture everything including the braces
        rule balanced_braces() -> &'input str
            = json:$( "{" (balanced_braces() / (!"}" [_]))* "}" ) {
                json
            }

        pub rule store() -> (&'input str, &'input str, &'input str)
            = _ ci("STORE") _
              event_type:ident() _
              ci("FOR") _
              context_id:(ident() / string_literal()) _
              ci("PAYLOAD") _
              json:json_block()
              _ // Allow trailing whitespace only
              {
                  (event_type, context_id, json)
              }
    }
}

/// Fast PEG-based parser that extracts JSON directly from string
pub fn parse_peg(input: &str) -> Result<Command, ParseError> {
    let (event_type, context_id, json_str) = sneldb_store::store(input)
        .map_err(|e| ParseError::UnexpectedToken(format!("PEG parse error: {}", e)))?;

    // Parse JSON directly using sonic-rs (faster than serde_json)
    let json_value: Value =
        sonic_rs::from_str(json_str).map_err(|_| ParseError::InvalidJson(json_str.to_string()))?;

    Ok(Command::Store {
        event_type: event_type.to_string(),
        context_id: context_id.to_string(),
        payload: json_value,
    })
}
