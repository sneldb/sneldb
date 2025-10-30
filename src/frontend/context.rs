use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::server_state::ServerState;
use crate::shared::config::CONFIG;

#[derive(Clone)]
pub struct FrontendContext {
    pub registry: Arc<RwLock<SchemaRegistry>>,
    pub shard_manager: Arc<ShardManager>,
    pub server_state: Arc<ServerState>,
}

impl FrontendContext {
    pub async fn from_config() -> Arc<Self> {
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new().expect("Failed to initialize SchemaRegistry"),
        ));

        let base_dir = PathBuf::from(&CONFIG.engine.data_dir);
        let wal_dir = PathBuf::from(&CONFIG.wal.dir);
        let shard_manager =
            Arc::new(ShardManager::new(CONFIG.engine.shard_count, base_dir, wal_dir).await);

        let server_state = Arc::new(ServerState::new(
            Arc::clone(&shard_manager),
            CONFIG.server.backpressure_threshold,
        ));

        Arc::new(Self {
            registry,
            shard_manager,
            server_state,
        })
    }
}
