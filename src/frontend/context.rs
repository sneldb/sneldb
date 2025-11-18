use crate::engine::auth::AuthManager;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::server_state::ServerState;
use crate::shared::config::CONFIG;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct FrontendContext {
    pub registry: Arc<RwLock<SchemaRegistry>>,
    pub shard_manager: Arc<ShardManager>,
    pub server_state: Arc<ServerState>,
    pub auth_manager: Option<Arc<AuthManager>>,
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

        // Initialize auth manager
        let auth_manager = Some(Arc::new(AuthManager::new(Arc::clone(&shard_manager))));

        // Load users from DB
        if let Some(ref auth_mgr) = auth_manager {
            if let Err(e) = auth_mgr.load_from_db().await {
                tracing::warn!("Failed to load users from DB: {}", e);
            }
            // Bootstrap admin user if no users exist
            if let Err(e) = auth_mgr.bootstrap_admin_user().await {
                tracing::warn!("Failed to bootstrap admin user: {}", e);
            }
            // Reload from DB after bootstrap to ensure cache is synced
            // This handles cases where users exist in DB but weren't in cache
            if let Err(e) = auth_mgr.load_from_db().await {
                tracing::warn!("Failed to reload users from DB after bootstrap: {}", e);
            }
        }

        Arc::new(Self {
            registry,
            shard_manager,
            server_state,
            auth_manager,
        })
    }
}
