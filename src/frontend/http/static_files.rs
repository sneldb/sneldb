use hyper::{Response, StatusCode};

// Serve the main SPA index.html
pub fn serve_index() -> Response<String> {
    let body = include_str!("static/index.html");
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(body.to_string())
        .unwrap()
}

// Serve files from the embedded ./static directory by simple name matching
pub fn serve_asset(path: &str) -> Response<String> {
    match path {
        "app.js" => Response::builder()
            .status(StatusCode::OK)
            .header(
                hyper::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            )
            .body(include_str!("static/app.js").to_string())
            .unwrap(),
        "styles.css" => Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "text/css; charset=utf-8")
            .body(include_str!("static/styles.css").to_string())
            .unwrap(),
        "highlight.js" => Response::builder()
            .status(StatusCode::OK)
            .header(
                hyper::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            )
            .body(include_str!("static/highlight.js").to_string())
            .unwrap(),
        "sneldb.js" => Response::builder()
            .status(StatusCode::OK)
            .header(
                hyper::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            )
            .body(include_str!("static/sneldb.js").to_string())
            .unwrap(),
        "highlight.css" => Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "text/css; charset=utf-8")
            .body(include_str!("static/highlight.css").to_string())
            .unwrap(),
        // No external docs assets; playground is self-contained
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body("Not Found".to_string())
            .unwrap(),
    }
}
