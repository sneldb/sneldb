#[derive(Debug)]
pub enum ParseError {
    /// Unrecognized top-level command
    UnknownCommand(String),

    /// A required argument was missing
    MissingArgument(String),

    /// Failed to parse JSON
    InvalidJson(String),

    /// Failed to parse a date (expected format: YYYY-MM-DD)
    InvalidDate(String),

    /// WHERE clause is invalid (e.g., missing =, or invalid structure)
    InvalidWhereClause,

    /// Unexpected token found while parsing
    UnexpectedToken(String),

    /// Expected a specific keyword but found something else
    ExpectedKeyword(String, String),

    /// Expected a JSON block but found something else
    ExpectedJsonBlock,

    /// Nested JSON blocks are not allowed
    NestedJsonNotAllowed,

    /// Invalid event type
    InvalidEventType(String),

    /// Invalid character
    InvalidCharacter(char),

    /// Empty schema
    EmptySchema,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnknownCommand(cmd) => write!(f, "Unknown command: {}", cmd),
            ParseError::MissingArgument(arg) => write!(f, "Missing argument: {}", arg),
            ParseError::InvalidJson(arg) => write!(f, "Invalid JSON: {}", arg),
            ParseError::InvalidDate(arg) => {
                write!(f, "Invalid date: '{}'. Expected format: YYYY-MM-DD", arg)
            }
            ParseError::InvalidWhereClause => {
                write!(f, "Invalid WHERE clause. Use: WHERE field = value")
            }
            ParseError::UnexpectedToken(tok) => write!(f, "Unexpected token: {}", tok),
            ParseError::ExpectedKeyword(expected, found) => {
                write!(f, "Expected keyword '{}', but found '{}'", expected, found)
            }
            ParseError::ExpectedJsonBlock => {
                write!(f, "Expected JSON block, but found something else")
            }
            ParseError::NestedJsonNotAllowed => {
                write!(f, "Nested JSON blocks are not allowed")
            }
            ParseError::InvalidEventType(event_type) => {
                write!(f, "Invalid event type: {}", event_type)
            }
            ParseError::InvalidCharacter(c) => {
                write!(f, "Invalid character: '{}'", c)
            }
            ParseError::EmptySchema => {
                write!(f, "Schema cannot be empty")
            }
        }
    }
}

impl std::error::Error for ParseError {}
