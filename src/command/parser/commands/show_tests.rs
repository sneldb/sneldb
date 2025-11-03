use super::show;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[test]
fn parse_show_basic() {
    let tokens = tokenize("SHOW cached_view");
    let cmd = show::parse(&tokens).expect("failed to parse SHOW");
    match cmd {
        Command::ShowMaterialized { name } => assert_eq!(name, "cached_view"),
        other => panic!("expected show command, got {:?}", other),
    }
}

#[test]
fn parse_show_rejects_extra_tokens() {
    let tokens = tokenize("SHOW cached_view extra");
    let err = show::parse(&tokens).unwrap_err();
    assert!(matches!(err, ParseError::UnexpectedToken(_)));
}
