use std::fmt;

/// HTTP-style status codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    Ok,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    InternalError,
    ServiceUnavailable,
}

impl StatusCode {
    pub fn code(&self) -> u16 {
        match self {
            StatusCode::Ok => 200,
            StatusCode::BadRequest => 400,
            StatusCode::Unauthorized => 401,
            StatusCode::Forbidden => 403,
            StatusCode::NotFound => 404,
            StatusCode::InternalError => 500,
            StatusCode::ServiceUnavailable => 503,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            StatusCode::Ok => "OK",
            StatusCode::BadRequest => "Bad Request",
            StatusCode::Unauthorized => "Unauthorized",
            StatusCode::Forbidden => "Forbidden",
            StatusCode::NotFound => "Not Found",
            StatusCode::InternalError => "Internal Error",
            StatusCode::ServiceUnavailable => "Service Unavailable",
        }
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.code(), self.message())
    }
}

impl From<hyper::StatusCode> for StatusCode {
    fn from(status: hyper::StatusCode) -> Self {
        match status.as_u16() {
            200 => StatusCode::Ok,
            400 => StatusCode::BadRequest,
            404 => StatusCode::NotFound,
            503 => StatusCode::ServiceUnavailable,
            401 => StatusCode::Unauthorized,
            403 => StatusCode::Forbidden,
            405 => StatusCode::BadRequest,  // Method not allowed
            _ => StatusCode::InternalError, // Default to internal error for other codes
        }
    }
}
use crate::engine::types::ScalarValue;

#[derive(Debug, Clone)]
pub enum ResponseBody {
    Lines(Vec<String>),
    ScalarArray(Vec<ScalarValue>),
    Table {
        columns: Vec<(String, String)>,
        rows: Vec<Vec<ScalarValue>>,
    },
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: StatusCode,
    pub message: String,
    pub body: ResponseBody,
    pub count: usize,
}

impl Response {
    pub fn ok_lines(lines: impl IntoIterator<Item = String>) -> Self {
        Self {
            status: StatusCode::Ok,
            count: 1,
            message: "OK".to_string(),
            body: ResponseBody::Lines(lines.into_iter().collect()),
        }
    }

    pub fn ok_scalar_array(rows: Vec<ScalarValue>, count: usize) -> Self {
        Self {
            status: StatusCode::Ok,
            count: count,
            message: "OK".to_string(),
            body: ResponseBody::ScalarArray(rows),
        }
    }

    pub fn ok_table(
        columns: Vec<(String, String)>,
        rows: Vec<Vec<ScalarValue>>,
        count: usize,
    ) -> Self {
        Self {
            status: StatusCode::Ok,
            count,
            message: "OK".to_string(),
            body: ResponseBody::Table { columns, rows },
        }
    }

    pub fn error(code: StatusCode, message: impl ToString) -> Self {
        Self {
            status: code,
            count: 0,
            message: message.to_string(),
            body: ResponseBody::Lines(vec![]),
        }
    }
}
