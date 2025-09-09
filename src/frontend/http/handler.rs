use hyper::{Request, Response, StatusCode, body::Incoming};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::RwLock;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;

use super::dispatcher::{handle_json_command, handle_line_command};

pub async fn handle_request(
    req: Request<Incoming>,
    registry: Arc<RwLock<SchemaRegistry>>,
    shard_manager: Arc<ShardManager>,
) -> Result<Response<String>, Infallible> {
    match req.uri().path() {
        "/command" => handle_line_command(req, registry, shard_manager).await,
        "/json-command" => handle_json_command(req, registry, shard_manager).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not Found".to_string())
            .unwrap()),
    }
}
