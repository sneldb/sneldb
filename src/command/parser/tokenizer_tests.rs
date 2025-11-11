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

    // ─────────────────────────────
    // Individual Token Type Tests
    // ─────────────────────────────

    #[test]
    fn test_tokenize_left_brace() {
        let tokens = tokenize("{");
        assert_eq!(tokens, vec![Token::LeftBrace]);
    }

    #[test]
    fn test_tokenize_right_brace() {
        let tokens = tokenize("}");
        assert_eq!(tokens, vec![Token::RightBrace]);
    }

    #[test]
    fn test_tokenize_braces_pair() {
        let tokens = tokenize("{}");
        assert_eq!(tokens, vec![Token::LeftBrace, Token::RightBrace]);
    }

    #[test]
    fn test_tokenize_semicolon() {
        let tokens = tokenize(";");
        assert_eq!(tokens, vec![Token::Semicolon]);
    }

    #[test]
    fn test_tokenize_left_square_bracket() {
        let tokens = tokenize("[");
        assert_eq!(tokens, vec![Token::LeftSquareBracket]);
    }

    #[test]
    fn test_tokenize_right_square_bracket() {
        let tokens = tokenize("]");
        assert_eq!(tokens, vec![Token::RightSquareBracket]);
    }

    #[test]
    fn test_tokenize_square_brackets_pair() {
        let tokens = tokenize("[]");
        assert_eq!(
            tokens,
            vec![Token::LeftSquareBracket, Token::RightSquareBracket]
        );
    }

    #[test]
    fn test_tokenize_left_paren() {
        let tokens = tokenize("(");
        assert_eq!(tokens, vec![Token::LeftParen]);
    }

    #[test]
    fn test_tokenize_right_paren() {
        let tokens = tokenize(")");
        assert_eq!(tokens, vec![Token::RightParen]);
    }

    #[test]
    fn test_tokenize_parens_pair() {
        let tokens = tokenize("()");
        assert_eq!(tokens, vec![Token::LeftParen, Token::RightParen]);
    }

    #[test]
    fn test_tokenize_all_symbols() {
        let tokens = tokenize(":,=><!.");
        assert_eq!(
            tokens,
            vec![
                Token::Symbol(':'),
                Token::Symbol(','),
                Token::Symbol('='),
                Token::Symbol('>'),
                Token::Symbol('<'),
                Token::Symbol('!'),
                Token::Symbol('.'),
            ]
        );
    }

    #[test]
    fn test_tokenize_colon() {
        let tokens = tokenize(":");
        assert_eq!(tokens, vec![Token::Symbol(':')]);
    }

    #[test]
    fn test_tokenize_comma() {
        let tokens = tokenize(",");
        assert_eq!(tokens, vec![Token::Symbol(',')]);
    }

    #[test]
    fn test_tokenize_equals() {
        let tokens = tokenize("=");
        assert_eq!(tokens, vec![Token::Symbol('=')]);
    }

    #[test]
    fn test_tokenize_greater_than() {
        let tokens = tokenize(">");
        assert_eq!(tokens, vec![Token::Symbol('>')]);
    }

    #[test]
    fn test_tokenize_less_than() {
        let tokens = tokenize("<");
        assert_eq!(tokens, vec![Token::Symbol('<')]);
    }

    #[test]
    fn test_tokenize_exclamation() {
        let tokens = tokenize("!");
        assert_eq!(tokens, vec![Token::Symbol('!')]);
    }

    #[test]
    fn test_tokenize_dot() {
        let tokens = tokenize(".");
        assert_eq!(tokens, vec![Token::Symbol('.')]);
    }

    // ─────────────────────────────
    // String Literal Tests
    // ─────────────────────────────

    #[test]
    fn test_tokenize_empty_string() {
        let tokens = tokenize(r#""""#);
        assert_eq!(tokens, vec![Token::StringLiteral("".to_string())]);
    }

    #[test]
    fn test_tokenize_string_with_escape_sequence() {
        let tokens = tokenize(r#""hello\nworld""#);
        assert_eq!(
            tokens,
            vec![Token::StringLiteral("hello\nworld".to_string())]
        );
    }

    #[test]
    fn test_tokenize_string_with_backslash() {
        let tokens = tokenize(r#""path\\to\\file""#);
        assert_eq!(
            tokens,
            vec![Token::StringLiteral("path\\to\\file".to_string())]
        );
    }

    #[test]
    fn test_tokenize_string_with_escaped_quote() {
        let tokens = tokenize(r#""say \"hello\"""#);
        assert_eq!(
            tokens,
            vec![Token::StringLiteral("say \"hello\"".to_string())]
        );
    }

    #[test]
    fn test_tokenize_string_with_tab() {
        let tokens = tokenize(r#""hello\tworld""#);
        assert_eq!(
            tokens,
            vec![Token::StringLiteral("hello\tworld".to_string())]
        );
    }

    #[test]
    fn test_tokenize_string_with_multiple_escapes() {
        let tokens = tokenize(r#""a\\b\"c""#);
        assert_eq!(tokens, vec![Token::StringLiteral("a\\b\"c".to_string())]);
    }

    #[test]
    fn test_tokenize_string_at_end_of_input() {
        let tokens = tokenize(r#""end""#);
        assert_eq!(tokens, vec![Token::StringLiteral("end".to_string())]);
    }

    #[test]
    fn test_tokenize_string_followed_by_word() {
        let tokens = tokenize(r#""hello"world"#);
        assert_eq!(
            tokens,
            vec![
                Token::StringLiteral("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_string_with_unicode() {
        let tokens = tokenize(r#""café""#);
        assert_eq!(tokens, vec![Token::StringLiteral("café".to_string())]);
    }

    #[test]
    fn test_tokenize_string_with_newline_char() {
        let tokens = tokenize(r#""line1\nline2""#);
        assert_eq!(
            tokens,
            vec![Token::StringLiteral("line1\nline2".to_string())]
        );
    }

    // ─────────────────────────────
    // Number Tests
    // ─────────────────────────────

    #[test]
    fn test_tokenize_zero() {
        let tokens = tokenize("0");
        assert_eq!(tokens, vec![Token::Number(0.0)]);
    }

    #[test]
    fn test_tokenize_negative_zero() {
        let tokens = tokenize("-0");
        assert_eq!(tokens, vec![Token::Number(-0.0)]);
    }

    #[test]
    fn test_tokenize_positive_integer() {
        let tokens = tokenize("42");
        assert_eq!(tokens, vec![Token::Number(42.0)]);
    }

    #[test]
    fn test_tokenize_decimal_number() {
        let tokens = tokenize("3.14");
        assert_eq!(tokens, vec![Token::Number(3.14)]);
    }

    #[test]
    fn test_tokenize_negative_decimal() {
        let tokens = tokenize("-99.99");
        assert_eq!(tokens, vec![Token::Number(-99.99)]);
    }

    #[test]
    fn test_tokenize_large_number() {
        let tokens = tokenize("1234567890");
        assert_eq!(tokens, vec![Token::Number(1234567890.0)]);
    }

    #[test]
    fn test_tokenize_small_decimal() {
        let tokens = tokenize("0.001");
        assert_eq!(tokens, vec![Token::Number(0.001)]);
    }

    #[test]
    fn test_tokenize_number_followed_by_word() {
        let tokens = tokenize("123abc");
        assert_eq!(
            tokens,
            vec![Token::Number(123.0), Token::Word("abc".to_string()),]
        );
    }

    #[test]
    fn test_tokenize_number_followed_by_symbol() {
        let tokens = tokenize("42=");
        assert_eq!(tokens, vec![Token::Number(42.0), Token::Symbol('='),]);
    }

    #[test]
    fn test_tokenize_multiple_numbers() {
        let tokens = tokenize("1 2 3");
        assert_eq!(
            tokens,
            vec![Token::Number(1.0), Token::Number(2.0), Token::Number(3.0),]
        );
    }

    #[test]
    fn test_tokenize_number_with_multiple_decimals() {
        // The parser consumes all digits and dots as a single number token
        // "1.2.3" is consumed entirely, then parsed (which fails and returns 0.0)
        let tokens = tokenize("1.2.3");
        assert_eq!(tokens.len(), 1);
        // Since "1.2.3" can't be parsed as a float, it returns 0.0
        assert_eq!(tokens[0], Token::Number(0.0));
    }

    #[test]
    fn test_tokenize_standalone_minus() {
        let tokens = tokenize("-");
        // Minus alone should be treated as a symbol or invalid
        // Based on the code, it will try to parse as number and get 0.0
        assert_eq!(tokens, vec![Token::Number(0.0)]);
    }

    // ─────────────────────────────
    // Word Tests
    // ─────────────────────────────

    #[test]
    fn test_tokenize_simple_word() {
        let tokens = tokenize("hello");
        assert_eq!(tokens, vec![Token::Word("hello".to_string())]);
    }

    #[test]
    fn test_tokenize_word_with_underscore() {
        let tokens = tokenize("hello_world");
        assert_eq!(tokens, vec![Token::Word("hello_world".to_string())]);
    }

    #[test]
    fn test_tokenize_word_with_hyphen() {
        let tokens = tokenize("hello-world");
        assert_eq!(tokens, vec![Token::Word("hello-world".to_string())]);
    }

    #[test]
    fn test_tokenize_word_with_numbers() {
        let tokens = tokenize("hello123");
        assert_eq!(tokens, vec![Token::Word("hello123".to_string())]);
    }

    #[test]
    fn test_tokenize_mixed_case_word() {
        let tokens = tokenize("HelloWorld");
        assert_eq!(tokens, vec![Token::Word("HelloWorld".to_string())]);
    }

    #[test]
    fn test_tokenize_word_starting_with_number() {
        let tokens = tokenize("123abc");
        // Numbers are parsed first, so this becomes Number(123) + Word("abc")
        assert_eq!(
            tokens,
            vec![Token::Number(123.0), Token::Word("abc".to_string()),]
        );
    }

    #[test]
    fn test_tokenize_word_with_multiple_underscores() {
        let tokens = tokenize("hello__world");
        assert_eq!(tokens, vec![Token::Word("hello__world".to_string())]);
    }

    #[test]
    fn test_tokenize_word_with_multiple_hyphens() {
        let tokens = tokenize("hello--world");
        assert_eq!(tokens, vec![Token::Word("hello--world".to_string())]);
    }

    #[test]
    fn test_tokenize_single_letter_word() {
        let tokens = tokenize("a");
        assert_eq!(tokens, vec![Token::Word("a".to_string())]);
    }

    #[test]
    fn test_tokenize_single_digit_as_word() {
        // Single digit is parsed as number, not word
        let tokens = tokenize("5");
        assert_eq!(tokens, vec![Token::Number(5.0)]);
    }

    #[test]
    fn test_tokenize_word_followed_by_symbol() {
        let tokens = tokenize("hello=");
        assert_eq!(
            tokens,
            vec![Token::Word("hello".to_string()), Token::Symbol('='),]
        );
    }

    // ─────────────────────────────
    // Whitespace Tests
    // ─────────────────────────────

    #[test]
    fn test_tokenize_tabs() {
        let tokens = tokenize("\thello\tworld\t");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_newlines() {
        let tokens = tokenize("hello\nworld");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_carriage_returns() {
        let tokens = tokenize("hello\rworld");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_windows_line_endings() {
        let tokens = tokenize("hello\r\nworld");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_multiple_spaces() {
        let tokens = tokenize("hello    world");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_mixed_whitespace() {
        let tokens = tokenize("hello\t\n\r world");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_only_whitespace() {
        let tokens = tokenize("   \t\n\r  ");
        assert!(tokens.is_empty());
    }

    // ─────────────────────────────
    // Edge Cases and Boundary Conditions
    // ─────────────────────────────

    #[test]
    fn test_tokenize_consecutive_symbols() {
        let tokens = tokenize("====");
        assert_eq!(
            tokens,
            vec![
                Token::Symbol('='),
                Token::Symbol('='),
                Token::Symbol('='),
                Token::Symbol('='),
            ]
        );
    }

    #[test]
    fn test_tokenize_consecutive_braces() {
        let tokens = tokenize("{{}}");
        assert_eq!(
            tokens,
            vec![
                Token::LeftBrace,
                Token::LeftBrace,
                Token::RightBrace,
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn test_tokenize_mixed_tokens() {
        let tokens = tokenize("hello{world}[test](123)");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::LeftBrace,
                Token::Word("world".to_string()),
                Token::RightBrace,
                Token::LeftSquareBracket,
                Token::Word("test".to_string()),
                Token::RightSquareBracket,
                Token::LeftParen,
                Token::Number(123.0),
                Token::RightParen,
            ]
        );
    }

    #[test]
    fn test_tokenize_complex_expression() {
        let tokens = tokenize("QUERY orders WHERE status = \"active\" AND price > 100");
        assert_eq!(
            tokens,
            vec![
                Token::Word("QUERY".to_string()),
                Token::Word("orders".to_string()),
                Token::Word("WHERE".to_string()),
                Token::Word("status".to_string()),
                Token::Symbol('='),
                Token::StringLiteral("active".to_string()),
                Token::Word("AND".to_string()),
                Token::Word("price".to_string()),
                Token::Symbol('>'),
                Token::Number(100.0),
            ]
        );
    }

    #[test]
    fn test_tokenize_with_parentheses() {
        let tokens = tokenize("(hello world)");
        assert_eq!(
            tokens,
            vec![
                Token::LeftParen,
                Token::Word("hello".to_string()),
                Token::Word("world".to_string()),
                Token::RightParen,
            ]
        );
    }

    #[test]
    fn test_tokenize_with_semicolon() {
        let tokens = tokenize("hello;world");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Semicolon,
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_unicode_in_word() {
        // Unicode characters that aren't alphanumeric/underscore/hyphen should be invalid
        let tokens = tokenize("hello🚀world");
        assert_eq!(
            tokens,
            vec![
                Token::Word("hello".to_string()),
                Token::Word("<INVALID>".to_string()),
                Token::Word("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_special_characters_as_invalid() {
        let tokens = tokenize("@#$%");
        assert_eq!(
            tokens,
            vec![
                Token::Word("<INVALID>".to_string()),
                Token::Word("<INVALID>".to_string()),
                Token::Word("<INVALID>".to_string()),
                Token::Word("<INVALID>".to_string()),
            ]
        );
    }

    #[test]
    fn test_tokenize_number_immediately_followed_by_dot() {
        let tokens = tokenize("123.456");
        assert_eq!(tokens, vec![Token::Number(123.456)]);
    }

    #[test]
    fn test_tokenize_dot_followed_by_number() {
        // Dot is a symbol, so this becomes Symbol('.') + Number(5)
        let tokens = tokenize(".5");
        assert_eq!(tokens, vec![Token::Symbol('.'), Token::Number(5.0),]);
    }

    #[test]
    fn test_tokenize_negative_number_in_expression() {
        let tokens = tokenize("x = -42");
        assert_eq!(
            tokens,
            vec![
                Token::Word("x".to_string()),
                Token::Symbol('='),
                Token::Number(-42.0),
            ]
        );
    }

    #[test]
    fn test_tokenize_string_with_only_escapes() {
        let tokens = tokenize(r#""\\""#);
        assert_eq!(tokens, vec![Token::StringLiteral("\\".to_string())]);
    }

    #[test]
    fn test_tokenize_very_long_word() {
        let long_word = "a".repeat(1000);
        let tokens = tokenize(&long_word);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], Token::Word(long_word));
    }

    #[test]
    fn test_tokenize_all_token_types_together() {
        let tokens = tokenize(r#"word "string" 123 { } [ ] ( ) ; : , = > < ! ."#);
        assert_eq!(
            tokens,
            vec![
                Token::Word("word".to_string()),
                Token::StringLiteral("string".to_string()),
                Token::Number(123.0),
                Token::LeftBrace,
                Token::RightBrace,
                Token::LeftSquareBracket,
                Token::RightSquareBracket,
                Token::LeftParen,
                Token::RightParen,
                Token::Semicolon,
                Token::Symbol(':'),
                Token::Symbol(','),
                Token::Symbol('='),
                Token::Symbol('>'),
                Token::Symbol('<'),
                Token::Symbol('!'),
                Token::Symbol('.'),
            ]
        );
    }

    #[test]
    fn test_tokenize_comparison_operators() {
        let tokens = tokenize("x > 5 AND y < 10");
        assert_eq!(
            tokens,
            vec![
                Token::Word("x".to_string()),
                Token::Symbol('>'),
                Token::Number(5.0),
                Token::Word("AND".to_string()),
                Token::Word("y".to_string()),
                Token::Symbol('<'),
                Token::Number(10.0),
            ]
        );
    }

    #[test]
    fn test_tokenize_inequality_operators() {
        let tokens = tokenize("x != 5 AND y == 10");
        assert_eq!(
            tokens,
            vec![
                Token::Word("x".to_string()),
                Token::Symbol('!'),
                Token::Symbol('='),
                Token::Number(5.0),
                Token::Word("AND".to_string()),
                Token::Word("y".to_string()),
                Token::Symbol('='),
                Token::Symbol('='),
                Token::Number(10.0),
            ]
        );
    }
}
