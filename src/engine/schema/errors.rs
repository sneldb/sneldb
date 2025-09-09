use std::fmt;

#[derive(Debug)]
pub enum SchemaError {
    /// Tried to define a schema that already exists
    AlreadyDefined(String),

    /// Schema is empty
    EmptySchema,

    /// Failed to serialize schema to disk
    SerializationFailed(String),

    /// Failed to write or append schema to disk
    IoWriteFailed(String),

    /// Failed to read from disk
    IoReadFailed(String),

    /// Failed to deserialize a record from disk
    DeserializationFailed(String),

    /// General schema registry error
    Other(String),

    /// Record length too large
    CorruptedRecord(String),
}

impl From<std::io::Error> for SchemaError {
    fn from(err: std::io::Error) -> Self {
        SchemaError::IoWriteFailed(err.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for SchemaError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        SchemaError::SerializationFailed(err.to_string())
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::AlreadyDefined(name) => {
                write!(f, "Schema for event_type '{}' already defined", name)
            }
            SchemaError::EmptySchema => write!(f, "Schema cannot be empty"),
            SchemaError::SerializationFailed(e) => write!(f, "Serialization error: {}", e),
            SchemaError::IoWriteFailed(e) => write!(f, "I/O write error: {}", e),
            SchemaError::IoReadFailed(e) => write!(f, "I/O read error: {}", e),
            SchemaError::DeserializationFailed(e) => {
                write!(f, "Deserialization error: {}", e)
            }
            SchemaError::Other(e) => write!(f, "Schema registry error: {}", e),
            SchemaError::CorruptedRecord(e) => write!(f, "Corrupted record: {}", e),
        }
    }
}

impl std::error::Error for SchemaError {}
