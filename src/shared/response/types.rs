use std::fmt;

/// HTTP-style status codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    Ok,
    BadRequest,
    NotFound,
    InternalError,
}

impl StatusCode {
    pub fn code(&self) -> u16 {
        match self {
            StatusCode::Ok => 200,
            StatusCode::BadRequest => 400,
            StatusCode::NotFound => 404,
            StatusCode::InternalError => 500,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            StatusCode::Ok => "OK",
            StatusCode::BadRequest => "Bad Request",
            StatusCode::NotFound => "Not Found",
            StatusCode::InternalError => "Internal Error",
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
            401 | 403 | 405 => StatusCode::BadRequest, // Map unauthorized, forbidden, method not allowed to bad request
            _ => StatusCode::InternalError,            // Default to internal error for other codes
        }
    }
}
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum ResponseBody {
    Lines(Vec<String>),
    JsonArray(Vec<Value>),
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

    pub fn ok_json(rows: Vec<Value>, count: usize) -> Self {
        Self {
            status: StatusCode::Ok,
            count: count,
            message: "OK".to_string(),
            body: ResponseBody::JsonArray(rows),
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
