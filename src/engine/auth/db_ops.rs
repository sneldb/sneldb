use super::storage::{AuthStorage, StoredUser};
use super::types::{AuthResult, PermissionCache, User, UserCache, UserKey};
use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Persists a user to auth storage.
pub async fn store_user_in_db(auth_storage: &Arc<dyn AuthStorage>, user: &User) -> AuthResult<()> {
    auth_storage.persist_user(user)
}

fn dedupe_latest(records: Vec<StoredUser>) -> Vec<UserKey> {
    let mut latest: HashMap<String, (u64, User)> = HashMap::new();
    for record in records {
        match latest.entry(record.user.user_id.clone()) {
            Entry::Vacant(slot) => {
                slot.insert((record.persisted_at, record.user));
            }
            Entry::Occupied(mut slot) => {
                if record.persisted_at >= slot.get().0 {
                    slot.insert((record.persisted_at, record.user));
                }
            }
        }
    }
    latest
        .into_values()
        .map(|(_, user)| UserKey::from(user))
        .collect()
}

/// Loads users from auth storage into caches (latest-wins).
pub async fn load_from_db(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    auth_storage: &Arc<dyn AuthStorage>,
) -> AuthResult<()> {
    let users = auth_storage.load_users()?;
    let loaded_keys = dedupe_latest(users);

    {
        let mut cache_guard = cache.write().await;
        cache_guard.clear();
        for key in &loaded_keys {
            cache_guard.insert(key.clone());
        }
    }

    {
        let mut perm_cache_guard = permission_cache.write().await;
        *perm_cache_guard = PermissionCache::new();
        for key in &loaded_keys {
            perm_cache_guard.update_user(key);
        }
    }

    Ok(())
}
