use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::command::types::Command;
use crate::engine::auth::AuthManager;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::http::json_command::JsonCommand;
use crate::frontend::server_state::ServerState;
use crate::shared::config::CONFIG;
use crate::shared::response::{
    ArrowRenderer, JsonRenderer, Response as ResponseType, StatusCode as ResponseStatusCode,
    render::Renderer, unix::UnixRenderer,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode, body::Incoming, header};
use std::{convert::Infallible, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::info;

fn is_authorized(req: &Request<Incoming>) -> bool {
    // Allow unauthenticated on loopback if playground says so
    if CONFIG.playground.allow_unauthenticated {
        if let Some(addr) = req
            .headers()
            .get("X-Forwarded-For")
            .and_then(|h| h.to_str().ok())
        {
            if addr == "127.0.0.1" || addr == "::1" {
                return true;
            }
        }
        if let Some(host) = req
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
        {
            if host.starts_with("127.0.0.1") || host.starts_with("localhost") {
                return true;
            }
        }
    }

    // Otherwise require bearer token
    req.headers().get(header::AUTHORIZATION)
        == Some(
            &format!("Bearer {}", CONFIG.server.auth_token)
                .parse::<hyper::header::HeaderValue>()
                .unwrap(),
        )
}

/// Extract authentication from HTTP headers
/// Returns (user_id, signature) if found in headers, None otherwise
fn extract_auth_from_headers(req: &Request<Incoming>) -> Option<(String, String)> {
    let user_id = req
        .headers()
        .get("X-Auth-User")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())?;

    let signature = req
        .headers()
        .get("X-Auth-Signature")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())?;

    if user_id.is_empty() || signature.is_empty() {
        return None;
    }

    Some((user_id, signature))
}

/// Check authentication before parsing command
/// Supports both header-based (X-Auth-User, X-Auth-Signature) and inline format (user_id:signature:command)
/// Returns (command, user_id) if authenticated, or None if auth check failed
async fn check_auth_with_headers<'a>(
    input: &'a str,
    auth_from_headers: Option<(String, String)>,
    auth_manager: Option<&'a Arc<AuthManager>>,
) -> Option<(&'a str, String)> {
    // Check if authentication is bypassed via config - do this first for performance
    if CONFIG.auth.as_ref().map(|a| a.bypass_auth).unwrap_or(false) {
        return Some((input, "bypass".to_string()));
    }

    // Cache trimmed input
    let trimmed = input.trim();

    // User management commands now require authentication

    // If auth manager is not configured, allow all commands
    let auth_mgr = match auth_manager {
        Some(am) => am,
        None => return Some((input, "no-auth".to_string())),
    };

    // Try header-based authentication first (for HTTP)
    if let Some((user_id, signature)) = auth_from_headers {
        // Verify signature against the command body
        match auth_mgr
            .verify_signature(trimmed, &user_id, &signature)
            .await
        {
            Ok(_) => return Some((input, user_id)),
            Err(_) => return None,
        }
    }

    // Fall back to inline format: user_id:signature:command (for backward compatibility)
    match auth_mgr.parse_auth(trimmed) {
        Ok((user_id, signature, command)) => {
            // Verify signature
            match auth_mgr.verify_signature(command, user_id, signature).await {
                Ok(_) => Some((command, user_id.to_string())),
                Err(_) => None,
            }
        }
        Err(_) => {
            // If parsing fails, authentication is required for all commands
            None
        }
    }
}

pub async fn handle_line_command(
    req: Request<Incoming>,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
    server_state: Arc<ServerState>,
    auth_manager: Option<Arc<AuthManager>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() != hyper::Method::POST {
        return method_not_allowed();
    }
    if !is_authorized(&req) {
        return unauthorized();
    }

    // Extract auth headers before consuming the request body
    let auth_from_headers = extract_auth_from_headers(&req);

    let body = req.collect().await.unwrap().to_bytes();
    let input = String::from_utf8_lossy(&body).trim().to_string();
    let renderer: Arc<dyn Renderer + Send + Sync> = match CONFIG.server.output_format.as_str() {
        "json" => Arc::new(JsonRenderer),
        "arrow" => Arc::new(ArrowRenderer),
        _ => Arc::new(UnixRenderer),
    };

    if input.is_empty() {
        return render_error("Empty command", StatusCode::BAD_REQUEST, renderer);
    }

    // Check authentication before parsing
    let (command_to_parse, authenticated_user_id) =
        match check_auth_with_headers(&input, auth_from_headers, auth_manager.as_ref()).await {
            Some((cmd, uid)) => (cmd, Some(uid)),
            None => {
                return render_error("Authentication failed", StatusCode::UNAUTHORIZED, renderer);
            }
        };

    match parse_command(command_to_parse) {
        Ok(cmd) => {
            // Increment pending operations before dispatch
            server_state.increment_pending();
            let start = Instant::now();
            let result = dispatch_and_respond(
                cmd,
                registry,
                shard_manager,
                auth_manager.as_ref(),
                authenticated_user_id.as_deref(),
                renderer,
            )
            .await;
            let execution_time_ms = start.elapsed().as_secs_f64() * 1000.0;
            // Decrement after dispatch completes
            server_state.decrement_pending();
            add_execution_time_header(result, execution_time_ms)
        }
        Err(e) => render_error(&e.to_string(), StatusCode::BAD_REQUEST, renderer),
    }
}

pub async fn handle_json_command(
    req: Request<Incoming>,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
    server_state: Arc<ServerState>,
    auth_manager: Option<Arc<AuthManager>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() != hyper::Method::POST {
        return method_not_allowed();
    }
    if !is_authorized(&req) {
        return unauthorized();
    }

    // Extract auth headers before consuming the request body
    let auth_from_headers = extract_auth_from_headers(&req);

    let body = req.collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);
    let renderer: Arc<dyn Renderer + Send + Sync> = match CONFIG.server.output_format.as_str() {
        "arrow" => Arc::new(ArrowRenderer),
        _ => Arc::new(JsonRenderer),
    };

    match sonic_rs::from_slice::<JsonCommand>(&body) {
        Ok(json_cmd) => {
            // Check if authentication is bypassed via config
            let bypass_auth = CONFIG.auth.as_ref().map(|a| a.bypass_auth).unwrap_or(false);

            // Extract user_id before potential move
            let authenticated_user_id = auth_from_headers.as_ref().map(|(uid, _)| uid.clone());

            if !bypass_auth {
                // Check authentication for all commands
                // Try header-based authentication
                if let Some((user_id, signature)) = auth_from_headers {
                    // Use the raw body string for signature verification (what client signed)
                    if let Some(auth_mgr) = auth_manager.as_ref() {
                        match auth_mgr
                            .verify_signature(body_str.trim(), &user_id, &signature)
                            .await
                        {
                            Ok(_) => {
                                // Authentication successful, proceed
                            }
                            Err(_) => {
                                return render_error(
                                    "Authentication failed",
                                    StatusCode::UNAUTHORIZED,
                                    renderer,
                                );
                            }
                        }
                    }
                } else {
                    // No headers found, require authentication for all commands
                    return render_error(
                        "Authentication required: missing X-Auth-User and X-Auth-Signature headers",
                        StatusCode::UNAUTHORIZED,
                        renderer,
                    );
                }
            }

            let cmd: Command = json_cmd.into();
            info!("Received JSON command: {:?}", cmd);
            // Increment pending operations before dispatch
            server_state.increment_pending();
            let start = Instant::now();
            let result = dispatch_and_respond(
                cmd,
                registry,
                shard_manager,
                auth_manager.as_ref(),
                authenticated_user_id.as_deref(),
                renderer,
            )
            .await;
            let execution_time_ms = start.elapsed().as_secs_f64() * 1000.0;
            // Decrement after dispatch completes
            server_state.decrement_pending();
            add_execution_time_header(result, execution_time_ms)
        }
        Err(e) => render_error(
            &format!("Invalid JSON command: {e}"),
            StatusCode::BAD_REQUEST,
            renderer,
        ),
    }
}

async fn dispatch_and_respond(
    cmd: Command,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
    auth_manager: Option<&Arc<AuthManager>>,
    user_id: Option<&str>,
    renderer: Arc<dyn Renderer + Send + Sync>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut output = Vec::new();
    let result = dispatch_command(
        &cmd,
        &mut output,
        &shard_manager,
        &registry,
        auth_manager,
        user_id,
        renderer.as_ref(),
    )
    .await;

    if let Err(e) = result {
        return Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(full_body(format!("Execution error: {}", e).into_bytes()))
            .unwrap());
    }

    // Extract HTTP status code from response
    // Error responses are always JSON (even with ArrowRenderer) and contain a "status" field
    let http_status = extract_http_status_from_response(&output);

    // Set content type based on output format, but note that error responses
    // from ArrowRenderer are JSON even when output_format is "arrow"
    let content_type = if http_status != hyper::StatusCode::OK {
        // Error responses are always JSON (even from ArrowRenderer)
        "application/json"
    } else {
        match CONFIG.server.output_format.as_str() {
            "json" => "application/json",
            "arrow" => "application/vnd.apache.arrow.stream",
            _ => "text/plain",
        }
    };

    Ok(Response::builder()
        .status(http_status)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(full_body(output))
        .unwrap())
}

fn unauthorized() -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(full_body(b"Unauthorized".to_vec()))
        .unwrap())
}

fn method_not_allowed() -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(full_body(b"Method Not Allowed".to_vec()))
        .unwrap())
}

fn render_error(
    msg: &str,
    status: StatusCode,
    renderer: Arc<dyn Renderer + Send + Sync>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let resp = ResponseType::error(ResponseStatusCode::from(status), msg.to_string());
    let body = renderer.render(&resp);
    Ok(Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(full_body(body))
        .unwrap())
}

fn full_body(data: Vec<u8>) -> Full<Bytes> {
    Full::new(Bytes::from(data))
}

/// Extract HTTP status code from response bytes
/// Error responses are always JSON (even with ArrowRenderer) and contain a "status" field
/// Successful responses may be Arrow binary format or large JSON, so we default to OK
fn extract_http_status_from_response(output: &[u8]) -> hyper::StatusCode {
    // Only check JSON responses (start with '{')
    if !output.starts_with(b"{") {
        return hyper::StatusCode::OK;
    }

    // Error responses are typically small (< 500 bytes)
    // For larger responses, check if it's likely an error by looking for status field early
    let parse_len = if output.len() < 500 {
        output.len()
    } else {
        // For larger responses, only check first 200 bytes for performance
        // Status field should be near the beginning: {"status":400,...}
        output.len().min(200)
    };

    // Try to parse JSON and extract status code
    if let Ok(json_str) = std::str::from_utf8(&output[..parse_len]) {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
            if let Some(status_val) = json.get("status") {
                if let Some(status_num) = status_val.as_u64() {
                    return map_status_code_to_http(status_num);
                }
            }
        }
    }

    hyper::StatusCode::OK
}

/// Map internal status code number to HTTP status code
fn map_status_code_to_http(status: u64) -> hyper::StatusCode {
    match status {
        200 => hyper::StatusCode::OK,
        400 => hyper::StatusCode::BAD_REQUEST,
        401 => hyper::StatusCode::UNAUTHORIZED,
        403 => hyper::StatusCode::FORBIDDEN,
        404 => hyper::StatusCode::NOT_FOUND,
        500 => hyper::StatusCode::INTERNAL_SERVER_ERROR,
        503 => hyper::StatusCode::SERVICE_UNAVAILABLE,
        _ => hyper::StatusCode::OK,
    }
}

fn add_execution_time_header(
    response: Result<Response<Full<Bytes>>, Infallible>,
    execution_time_ms: f64,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match response {
        Ok(mut resp) => {
            let headers = resp.headers_mut();
            let execution_time_str = format!("{:.3}", execution_time_ms);
            if let Ok(header_value) = execution_time_str.parse::<hyper::header::HeaderValue>() {
                headers.insert("X-Execution-Time-Ms", header_value);
            }
            Ok(resp)
        }
        Err(e) => Err(e),
    }
}
