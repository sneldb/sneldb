use crate::engine::auth::AuthManager;
use crate::engine::schema::SchemaRegistry;
use crate::engine::schema::registry::MiniSchema;
use crate::engine::schema::types::FieldType;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::server_state::ServerState;
use crate::shared::config::CONFIG;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

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

        // Initialize auth schema
        Self::init_auth_schema(&registry).await;

        // Load users from DB
        if let Some(ref auth_mgr) = auth_manager {
            if let Err(e) = auth_mgr.load_from_db().await {
                tracing::warn!("Failed to load users from DB: {}", e);
            }
        }

        Arc::new(Self {
            registry,
            shard_manager,
            server_state,
            auth_manager,
        })
    }

    /// Initialize the __auth_user schema if it doesn't exist
    async fn init_auth_schema(registry: &Arc<RwLock<SchemaRegistry>>) {
        let mut reg = registry.write().await;

        // Check if schema already exists
        if reg.get("__auth_user").is_some() {
            info!("Auth schema already defined");
            return;
        }

        // Define the auth user schema
        let mut fields = HashMap::new();
        fields.insert("user_id".to_string(), FieldType::String);
        fields.insert("secret_key".to_string(), FieldType::String);
        fields.insert("active".to_string(), FieldType::Bool);
        fields.insert("created_at".to_string(), FieldType::U64);

        let schema = MiniSchema { fields };

        match reg.define("__auth_user", schema) {
            Ok(_) => {
                info!("Auth schema '__auth_user' initialized successfully");
            }
            Err(e) => {
                tracing::warn!("Failed to initialize auth schema: {}", e);
            }
        }
    }
}
