/// Token represents different types of parsed items in the DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Word(String),
    Number(f64),
    StringLiteral(String),
    Symbol(char),
    LeftBrace,          // {
    RightBrace,         // }
    Semicolon,          // ;
    LeftSquareBracket,  // [
    RightSquareBracket, // ]
    LeftParen,          // (
    RightParen,         // )
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '{' => {
                chars.next();
                tokens.push(Token::LeftBrace);
            }
            '}' => {
                chars.next();
                tokens.push(Token::RightBrace);
            }
            ';' => {
                chars.next();
                tokens.push(Token::Semicolon);
            }
            '"' => {
                tokens.push(parse_string_literal(&mut chars));
            }
            '0'..='9' | '-' => {
                tokens.push(parse_number(&mut chars));
            }
            ':' | ',' | '=' | '>' | '<' | '!' | '.' => {
                tokens.push(Token::Symbol(chars.next().unwrap()));
            }
            '[' => {
                chars.next();
                tokens.push(Token::LeftSquareBracket);
            }
            ']' => {
                chars.next();
                tokens.push(Token::RightSquareBracket);
            }
            '(' => {
                chars.next();
                tokens.push(Token::LeftParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RightParen);
            }
            _ => {
                let word = parse_word(&mut chars);
                tokens.push(word);
            }
        }
    }

    tokens
}

fn parse_string_literal<I>(chars: &mut std::iter::Peekable<I>) -> Token
where
    I: Iterator<Item = char>,
{
    let mut string = String::new();
    chars.next(); // consume opening quote

    while let Some(&c) = chars.peek() {
        match c {
            '"' => {
                chars.next(); // consume closing quote
                break;
            }
            '\\' => {
                chars.next(); // consume '\'
                if let Some(&escaped) = chars.peek() {
                    chars.next();
                    match escaped {
                        'n' => string.push('\n'),
                        't' => string.push('\t'),
                        'r' => string.push('\r'),
                        '\\' => string.push('\\'),
                        '"' => string.push('"'),
                        _ => string.push(escaped), // Unknown escape, keep as-is
                    }
                }
            }
            _ => {
                string.push(c);
                chars.next();
            }
        }
    }

    Token::StringLiteral(string)
}

fn parse_number<I>(chars: &mut std::iter::Peekable<I>) -> Token
where
    I: Iterator<Item = char>,
{
    let mut number = String::new();

    while let Some(&c) = chars.peek() {
        if c.is_numeric() || c == '.' || c == '-' {
            number.push(c);
            chars.next();
        } else {
            break;
        }
    }

    let value: f64 = number.parse().unwrap_or(0.0);
    Token::Number(value)
}

fn parse_word<I>(chars: &mut std::iter::Peekable<I>) -> Token
where
    I: Iterator<Item = char>,
{
    let mut word = String::new();

    while let Some(&c) = chars.peek() {
        if c.is_alphanumeric() || c == '_' || c == '-' {
            word.push(c);
            chars.next();
        } else {
            break;
        }
    }

    if word.is_empty() {
        // If no valid word characters, consume 1 invalid char to prevent hang
        chars.next(); // consume invalid char (like 🚀)
        Token::Word("<INVALID>".to_string())
    } else {
        Token::Word(word)
    }
}
