use http_body_util::BodyExt;
use hyper::{Request, Response, StatusCode, body::Incoming, header};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::RwLock;

use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::http::json_command::JsonCommand;
use crate::shared::config::CONFIG;
use crate::shared::response::{Response as ResponseType, json::JsonRenderer, render::Renderer};
use tracing::info;

fn is_authorized(req: &Request<Incoming>) -> bool {
    req.headers().get(header::AUTHORIZATION)
        == Some(
            &format!("Bearer {}", CONFIG.server.auth_token)
                .parse::<hyper::header::HeaderValue>()
                .unwrap(),
        )
}

pub async fn handle_line_command(
    req: Request<Incoming>,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
) -> Result<Response<String>, Infallible> {
    if req.method() != hyper::Method::POST {
        return method_not_allowed();
    }
    if !is_authorized(&req) {
        return unauthorized();
    }

    let body = req.collect().await.unwrap().to_bytes();
    let input = String::from_utf8_lossy(&body).trim().to_string();
    let renderer: Arc<dyn Renderer + Send + Sync> = Arc::new(JsonRenderer);

    if input.is_empty() {
        return render_error("Empty command", StatusCode::BAD_REQUEST, renderer);
    }

    match parse_command(&input) {
        Ok(cmd) => dispatch_and_respond(cmd, registry, shard_manager, renderer).await,
        Err(e) => render_error(&e.to_string(), StatusCode::BAD_REQUEST, renderer),
    }
}

pub async fn handle_json_command(
    req: Request<Incoming>,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
) -> Result<Response<String>, Infallible> {
    if req.method() != hyper::Method::POST {
        return method_not_allowed();
    }
    if !is_authorized(&req) {
        return unauthorized();
    }

    let body = req.collect().await.unwrap().to_bytes();
    let renderer: Arc<dyn Renderer + Send + Sync> = Arc::new(JsonRenderer);

    match serde_json::from_slice::<JsonCommand>(&body) {
        Ok(json_cmd) => {
            let cmd: Command = json_cmd.into();
            info!("Received JSON command: {:?}", cmd);
            dispatch_and_respond(cmd, registry, shard_manager, renderer).await
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
    renderer: Arc<dyn Renderer + Send + Sync>,
) -> Result<Response<String>, Infallible> {
    let mut output = Vec::new();
    let result = dispatch_command(
        &cmd,
        &mut output,
        &shard_manager,
        &registry,
        renderer.as_ref(),
    )
    .await;

    if let Err(e) = result {
        return Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(format!("Execution error: {}", e))
            .unwrap());
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(String::from_utf8_lossy(&output).to_string())
        .unwrap())
}

fn unauthorized() -> Result<Response<String>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body("Unauthorized".to_string())
        .unwrap())
}

fn method_not_allowed() -> Result<Response<String>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body("Method Not Allowed".to_string())
        .unwrap())
}

fn render_error(
    msg: &str,
    status: StatusCode,
    renderer: Arc<dyn Renderer + Send + Sync>,
) -> Result<Response<String>, Infallible> {
    let resp = ResponseType::error(
        crate::shared::response::StatusCode::from(status),
        msg.to_string(),
    );
    let body = renderer.render(&resp);
    Ok(Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(String::from_utf8_lossy(&body).to_string())
        .unwrap())
}
