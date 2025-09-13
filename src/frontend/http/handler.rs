use hyper::{Request, Response, StatusCode, body::Incoming};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::RwLock;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;

use super::dispatcher::{handle_json_command, handle_line_command};
use super::static_files::{serve_asset, serve_index};

struct HttpHandler {
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
}

impl HttpHandler {
    fn new(registry: Arc<RwLock<SchemaRegistry>>, shard_manager: Arc<ShardManager>) -> Self {
        Self {
            registry,
            shard_manager,
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

        if let Some(resp) = Self::serve_playground(&path) {
            return Ok(resp);
        }

        match path.as_str() {
            "/command" => {
                handle_line_command(
                    req,
                    Arc::clone(&self.registry),
                    Arc::clone(&self.shard_manager),
                )
                .await
            }
            "/json-command" => {
                handle_json_command(
                    req,
                    Arc::clone(&self.registry),
                    Arc::clone(&self.shard_manager),
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
) -> Result<Response<String>, Infallible> {
    let handler = HttpHandler::new(registry, shard_manager);
    handler.handle(req).await
}
