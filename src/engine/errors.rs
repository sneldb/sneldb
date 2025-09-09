use std::io;
use thiserror::Error;
use tracing::{debug, error};

/// Errors that can occur during query execution.
#[derive(Debug, Error)]
pub enum QueryExecutionError {
    #[error("Zone metadata load failed: {0}")]
    ZoneLoad(#[from] ZoneMetaError),

    #[error("Column load failed: {0}")]
    ColLoad(#[from] ColumnLoadError),

    #[error("Row loading failed: {0}")]
    RowLoad(ColumnLoadError), // Manual `.map_err(QueryExecutionError::RowLoad)?`

    #[error("In-memory expression evaluation failed: {0}")]
    ExprEval(String),

    #[error("Query aborted due to internal limit or planning error")]
    Aborted,

    #[error("data load error: {0}")]
    Load(ColumnLoadError),

    #[error("Column read error: {0}")]
    ColRead(String),

    #[error("Offset load error: {0}")]
    OffsetLoad(String),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Invalid segment ID: {0}")]
    InvalidSegmentId(String),
}

#[derive(Debug, Error)]
pub enum ColumnLoadError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("UTF-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Unexpected EOF while reading column")]
    UnexpectedEOF,

    #[error("Other error: {0}")]
    Other(String),
}

impl QueryExecutionError {
    pub fn log_error(&self) {
        match self {
            QueryExecutionError::ZoneLoad(e) => {
                error!("Zone metadata load failed: {}", e);
                debug!("Zone metadata error details: {:?}", e);
            }
            QueryExecutionError::ColLoad(e) => {
                error!("Column load failed: {}", e);
                debug!("Column load error details: {:?}", e);
            }
            QueryExecutionError::RowLoad(e) => {
                error!("Row loading failed: {}", e);
                debug!("Row load error details: {:?}", e);
            }
            QueryExecutionError::ExprEval(e) => {
                error!("Expression evaluation failed: {}", e);
                debug!("Expression evaluation error details: {}", e);
            }
            QueryExecutionError::Aborted => {
                error!("Query was aborted");
                debug!("Query aborted due to internal limit or planning error");
            }
            QueryExecutionError::Load(e) => {
                error!("Data load error: {}", e);
                debug!("Data load error details: {:?}", e);
            }
            QueryExecutionError::ColRead(e) => {
                error!("Column read error: {}", e);
                debug!("Column read error details: {}", e);
            }
            QueryExecutionError::OffsetLoad(e) => {
                error!("Offset load error: {}", e);
                debug!("Offset load error details: {}", e);
            }
            QueryExecutionError::SchemaNotFound(e) => {
                error!("Schema not found: {}", e);
                debug!("Schema not found error details: {}", e);
            }
            QueryExecutionError::InvalidSegmentId(e) => {
                error!("Invalid segment ID: {}", e);
                debug!("Invalid segment ID error details: {}", e);
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ZoneMetaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Failed to deserialize: {0}")]
    Deserialize(#[from] bincode::Error),

    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum CompactorError {
    #[error("Zone cursor load error: {0}")]
    ZoneCursorLoad(String),

    #[error("Zone writer error: {0}")]
    ZoneWriter(String),

    #[error("Segment index error: {0}")]
    SegmentIndex(String),

    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Zone serialization error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Invalid context ID")]
    InvalidContextId,

    #[error("Invalid event type")]
    InvalidEventType,

    #[error("Flush error: {0}")]
    FlushFailed(String),

    #[error("Flush called on empty event list")]
    EmptyFlush,

    #[error("No UID for event type: {0}")]
    NoUidForEventType(String),

    #[error("Empty zone")]
    EmptyZone,

    #[error("Zone meta error: {0}")]
    ZoneMeta(#[from] ZoneMetaError),

    #[error("WAL error: {0}")]
    WALError(String),

    #[error("Invalid UID: {0}")]
    InvalidUid(String),
}
