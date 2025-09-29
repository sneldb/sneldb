use crate::command::parser::tokenizer::{Token, tokenize};

#[cfg(test)]
mod tokenizer_tests {
    use super::*;

    #[test]
    fn test_tokenize_simple_store() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "status": "pending" }"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("order_created".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-9".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("id".to_string()),
                Token::Symbol(':'),
                Token::Number(9.0),
                Token::Symbol(','),
                Token::StringLiteral("status".to_string()),
                Token::Symbol(':'),
                Token::StringLiteral("pending".to_string()),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_negative_number() {
        let input = r#"STORE my_event FOR user-1 PAYLOAD { "balance": -123.45 }"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("my_event".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-1".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("balance".to_string()),
                Token::Symbol(':'),
                Token::Number(-123.45),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_empty() {
        let tokens = tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenize_extra_spaces() {
        let input = r#"  STORE   order_created   FOR    user-9   PAYLOAD  { "id" : 9 }  "#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("order_created".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-9".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("id".to_string()),
                Token::Symbol(':'),
                Token::Number(9.0),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_string_with_spaces() {
        let input = r#"STORE my_event FOR "user 9" PAYLOAD { "name": "John Doe" }"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("my_event".to_string()),
                Token::Word("FOR".to_string()),
                Token::StringLiteral("user 9".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("name".to_string()),
                Token::Symbol(':'),
                Token::StringLiteral("John Doe".to_string()),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_multiple_fields_payload() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "status": "pending", "amount": 100.5 }"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("order_created".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-9".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("id".to_string()),
                Token::Symbol(':'),
                Token::Number(9.0),
                Token::Symbol(','),
                Token::StringLiteral("status".to_string()),
                Token::Symbol(':'),
                Token::StringLiteral("pending".to_string()),
                Token::Symbol(','),
                Token::StringLiteral("amount".to_string()),
                Token::Symbol(':'),
                Token::Number(100.5),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_missing_payload() {
        let input = "STORE order_created FOR user-9";
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("order_created".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-9".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_non_alphanumeric_characters() {
        let input = "STORE order_created FOR user-9 PAYLOAD { 🚀 }";
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("order_created".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-9".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::Word("<INVALID>".to_string()),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_nested_json_payload() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "details": { "product": "book", "price": 12.99 } }"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("order_created".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user-9".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("id".to_string()),
                Token::Symbol(':'),
                Token::Number(9.0),
                Token::Symbol(','),
                Token::StringLiteral("details".to_string()),
                Token::Symbol(':'),
                Token::LeftBrace,
                Token::StringLiteral("product".to_string()),
                Token::Symbol(':'),
                Token::StringLiteral("book".to_string()),
                Token::Symbol(','),
                Token::StringLiteral("price".to_string()),
                Token::Symbol(':'),
                Token::Number(12.99),
                Token::RightBrace,
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_weird_spacing() {
        let input = r#"STORE   event FOR  user PAYLOAD {  "id" :   9 , "ok" : true }"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("STORE".to_string()),
                Token::Word("event".to_string()),
                Token::Word("FOR".to_string()),
                Token::Word("user".to_string()),
                Token::Word("PAYLOAD".to_string()),
                Token::LeftBrace,
                Token::StringLiteral("id".to_string()),
                Token::Symbol(':'),
                Token::Number(9.0),
                Token::Symbol(','),
                Token::StringLiteral("ok".to_string()),
                Token::Symbol(':'),
                Token::Word("true".to_string()), // `true` remains a word
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_array_brackets() {
        let input = r#"QUERY orders WHERE status = [ "pending", "shipped" ]"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("QUERY".to_string()),
                Token::Word("orders".to_string()),
                Token::Word("WHERE".to_string()),
                Token::Word("status".to_string()),
                Token::Symbol('='),
                Token::LeftSquareBracket,
                Token::StringLiteral("pending".to_string()),
                Token::Symbol(','),
                Token::StringLiteral("shipped".to_string()),
                Token::RightSquareBracket,
            ]
        );
    }

    #[test]
    fn test_tokenize_dotted_identifier_in_where() {
        let input = r#"QUERY orders WHERE order.status = "ok""#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("QUERY".to_string()),
                Token::Word("orders".to_string()),
                Token::Word("WHERE".to_string()),
                Token::Word("order".to_string()),
                Token::Symbol('.'),
                Token::Word("status".to_string()),
                Token::Symbol('='),
                Token::StringLiteral("ok".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_dotted_identifier_in_return_and_by() {
        let input = r#"QUERY orders RETURN [order.status] BY region.name"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("QUERY".to_string()),
                Token::Word("orders".to_string()),
                Token::Word("RETURN".to_string()),
                Token::LeftSquareBracket,
                Token::Word("order".to_string()),
                Token::Symbol('.'),
                Token::Word("status".to_string()),
                Token::RightSquareBracket,
                Token::Word("BY".to_string()),
                Token::Word("region".to_string()),
                Token::Symbol('.'),
                Token::Word("name".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_find_and_linked_by_with_dotted() {
        let input = r#"FIND orders LINKED BY account.id"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("FIND".to_string()),
                Token::Word("orders".to_string()),
                Token::Word("LINKED".to_string()),
                Token::Word("BY".to_string()),
                Token::Word("account".to_string()),
                Token::Symbol('.'),
                Token::Word("id".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_group_by_multiple_dotted_fields() {
        let input = r#"QUERY orders COUNT BY a.b, c.d"#;
        let tokens = tokenize(input);

        assert_eq!(
            tokens,
            vec![
                Token::Word("QUERY".to_string()),
                Token::Word("orders".to_string()),
                Token::Word("COUNT".to_string()),
                Token::Word("BY".to_string()),
                Token::Word("a".to_string()),
                Token::Symbol('.'),
                Token::Word("b".to_string()),
                Token::Symbol(','),
                Token::Word("c".to_string()),
                Token::Symbol('.'),
                Token::Word("d".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_with_invalid_characters() {
        let input = r#"🧠"#;
        let tokens = tokenize(input);

        assert_eq!(tokens, vec![Token::Word("<INVALID>".to_string()),]);
    }
}
