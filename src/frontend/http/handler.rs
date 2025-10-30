use hyper::{Request, Response, StatusCode, body::Incoming};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::RwLock;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::server_state::ServerState;
use crate::shared::config::CONFIG;

use super::dispatcher::{handle_json_command, handle_line_command};
use super::static_files::{serve_asset, serve_index};

struct HttpHandler {
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
    server_state: Arc<ServerState>,
}

impl HttpHandler {
    fn new(
        registry: Arc<RwLock<SchemaRegistry>>,
        shard_manager: Arc<ShardManager>,
        server_state: Arc<ServerState>,
    ) -> Self {
        Self {
            registry,
            shard_manager,
            server_state,
        }
    }

    fn not_found() -> Response<String> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not Found".to_string())
            .unwrap()
    }

    fn serve_playground(path: &str) -> Option<Response<String>> {
        if !CONFIG.playground.enabled {
            return None;
        }
        match path {
            "/" => Some(serve_index()),
            p if p.starts_with("/static/") => {
                let name = p.trim_start_matches("/static/");
                Some(serve_asset(name))
            }
            _ => None,
        }
    }

    async fn handle(&self, req: Request<Incoming>) -> Result<Response<String>, Infallible> {
        let path = req.uri().path().to_string();

        // Check shutdown and backpressure before processing requests
        // Static files (playground) are exempt from these checks
        if !path.starts_with("/static/") && path != "/" {
            if self.server_state.is_shutting_down() {
                return Ok(Response::builder()
                    .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
                    .header(hyper::header::CONTENT_TYPE, "text/plain")
                    .body("Server is shutting down".to_string())
                    .unwrap());
            }

            if self.server_state.is_under_pressure() {
                return Ok(Response::builder()
                    .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
                    .header(hyper::header::CONTENT_TYPE, "text/plain")
                    .body("Server is under pressure, please retry later".to_string())
                    .unwrap());
            }
        }

        if let Some(resp) = Self::serve_playground(&path) {
            return Ok(resp);
        }

        match path.as_str() {
            "/command" => {
                handle_line_command(
                    req,
                    Arc::clone(&self.registry),
                    Arc::clone(&self.shard_manager),
                    Arc::clone(&self.server_state),
                )
                .await
            }
            "/json-command" => {
                handle_json_command(
                    req,
                    Arc::clone(&self.registry),
                    Arc::clone(&self.shard_manager),
                    Arc::clone(&self.server_state),
                )
                .await
            }
            _ => Ok(Self::not_found()),
        }
    }
}

pub async fn handle_request(
    req: Request<Incoming>,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
    server_state: Arc<ServerState>,
) -> Result<Response<String>, Infallible> {
    let handler = HttpHandler::new(registry, shard_manager, server_state);
    handler.handle(req).await
}
