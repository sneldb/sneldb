use thiserror::Error;

#[derive(Debug, Error)]
pub enum MaterializationError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid storage header: {0}")]
    Header(String),

    #[error("Duplicate materialization name: {0}")]
    Duplicate(String),

    #[error("Materialization not found: {0}")]
    NotFound(String),

    #[error("Catalog corruption: {0}")]
    Corrupt(String),

    #[error("Batch error: {0}")]
    Batch(String),
}
