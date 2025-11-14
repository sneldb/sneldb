use crate::command::types::Command;
use crate::engine::auth::{AuthManager, PermissionSet};
use crate::engine::schema::SchemaRegistry;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, error};

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    auth_manager: &Arc<AuthManager>,
    registry: &Arc<RwLock<SchemaRegistry>>,
    admin_user_id: Option<&str>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    // Verify admin
    // Skip permission check if user_id is "bypass" (bypass_auth mode)
    let admin_id = match admin_user_id {
        Some(uid) => uid,
        None => {
            let resp = Response::error(StatusCode::Unauthorized, "Authentication required");
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
            return Ok(());
        }
    };

    // Skip permission checks for bypass user
    if admin_id != "bypass" && !auth_manager.is_admin(admin_id).await {
        let resp = Response::error(
            StatusCode::Forbidden,
            "Only admin users can manage permissions",
        );
        writer.write_all(&renderer.render(&resp)).await?;
        writer.flush().await?;
        return Ok(());
    }

    match cmd {
        Command::GrantPermission {
            permissions,
            event_types,
            user_id,
        } => {
            // Validate permissions
            let mut perm_set = PermissionSet::none();
            for perm in permissions {
                match perm.as_str() {
                    "read" => perm_set.read = true,
                    "write" => perm_set.write = true,
                    _ => {
                        let resp = Response::error(
                            StatusCode::BadRequest,
                            &format!("Invalid permission: {}. Must be 'read' or 'write'", perm),
                        );
                        writer.write_all(&renderer.render(&resp)).await?;
                        writer.flush().await?;
                        return Ok(());
                    }
                }
            }

            // Grant permissions for each event type
            // Note: This will merge with existing permissions (grant adds to existing)
            for event_type in event_types {
                // Validate that event_type exists in schema registry
                let schema_registry = registry.read().await;
                if !schema_registry.has_schema(event_type) {
                    let resp = Response::error(
                        StatusCode::BadRequest,
                        &format!("No schema defined for event type '{}'", event_type),
                    );
                    writer.write_all(&renderer.render(&resp)).await?;
                    writer.flush().await?;
                    return Ok(());
                }
                drop(schema_registry);

                // Get existing permissions for this event type
                let existing_perms = auth_manager
                    .get_permissions(user_id)
                    .await
                    .ok()
                    .and_then(|perms| perms.get(event_type).cloned())
                    .unwrap_or_else(PermissionSet::none);

                // Merge: grant adds permissions, doesn't remove existing ones
                let merged_perms = PermissionSet {
                    read: existing_perms.read || perm_set.read,
                    write: existing_perms.write || perm_set.write,
                };

                match auth_manager
                    .grant_permission(user_id, event_type, merged_perms)
                    .await
                {
                    Ok(_) => {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(
                                target: "sneldb::permissions",
                                admin_user = admin_id,
                                user_id,
                                event_type,
                                "Permission granted"
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            target: "sneldb::permissions",
                            user_id,
                            event_type,
                            error = %e,
                            "Failed to grant permission"
                        );
                        let resp = Response::error(
                            StatusCode::BadRequest,
                            &format!("Failed to grant permission: {}", e),
                        );
                        writer.write_all(&renderer.render(&resp)).await?;
                        writer.flush().await?;
                        return Ok(());
                    }
                }
            }

            let resp =
                Response::ok_lines(vec![format!("Permissions granted to user '{}'", user_id)]);
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
        }
        Command::RevokePermission {
            permissions,
            event_types,
            user_id,
        } => {
            // If permissions are specified, revoke only those; otherwise revoke all
            let revoke_read = permissions.is_empty() || permissions.iter().any(|p| p == "read");
            let revoke_write = permissions.is_empty() || permissions.iter().any(|p| p == "write");

            // Revoke permissions for each event type
            for event_type in event_types {
                // Get existing permissions
                let existing_perms = auth_manager
                    .get_permissions(user_id)
                    .await
                    .ok()
                    .and_then(|perms| perms.get(event_type).cloned())
                    .unwrap_or_else(PermissionSet::none);

                // Calculate new permissions after revocation
                let new_perms = PermissionSet {
                    read: existing_perms.read && !revoke_read,
                    write: existing_perms.write && !revoke_write,
                };

                // Update permissions (grant with new set, or revoke if empty)
                let result = if !new_perms.read && !new_perms.write {
                    // Remove permission entirely
                    auth_manager.revoke_permission(user_id, event_type).await
                } else {
                    // Update with reduced permissions
                    auth_manager
                        .grant_permission(user_id, event_type, new_perms)
                        .await
                };

                match result {
                    Ok(_) => {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(
                                target: "sneldb::permissions",
                                admin_user = admin_id,
                                user_id,
                                event_type,
                                "Permission revoked"
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            target: "sneldb::permissions",
                            user_id,
                            event_type,
                            error = %e,
                            "Failed to revoke permission"
                        );
                        let resp = Response::error(
                            StatusCode::BadRequest,
                            &format!("Failed to revoke permission: {}", e),
                        );
                        writer.write_all(&renderer.render(&resp)).await?;
                        writer.flush().await?;
                        return Ok(());
                    }
                }
            }

            let resp =
                Response::ok_lines(vec![format!("Permissions revoked from user '{}'", user_id)]);
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
        }
        Command::ShowPermissions { user_id } => match auth_manager.get_permissions(user_id).await {
            Ok(permissions) => {
                if permissions.is_empty() {
                    let resp =
                        Response::ok_lines(vec![format!("User '{}' has no permissions", user_id)]);
                    writer.write_all(&renderer.render(&resp)).await?;
                    writer.flush().await?;
                } else {
                    let mut lines = vec![format!("Permissions for user '{}':", user_id)];
                    for (event_type, perm_set) in permissions.iter() {
                        let mut perms = Vec::new();
                        if perm_set.read {
                            perms.push("read");
                        }
                        if perm_set.write {
                            perms.push("write");
                        }
                        if perms.is_empty() {
                            lines.push(format!("  {}: none", event_type));
                        } else {
                            lines.push(format!("  {}: {}", event_type, perms.join(", ")));
                        }
                    }
                    let resp = Response::ok_lines(lines);
                    writer.write_all(&renderer.render(&resp)).await?;
                    writer.flush().await?;
                }
            }
            Err(e) => {
                error!(
                    target: "sneldb::permissions",
                    user_id,
                    error = %e,
                    "Failed to get permissions"
                );
                let resp = Response::error(
                    StatusCode::BadRequest,
                    &format!("Failed to get permissions: {}", e),
                );
                writer.write_all(&renderer.render(&resp)).await?;
                writer.flush().await?;
            }
        },
        _ => {
            error!(target: "sneldb::permissions", "Invalid command variant for permissions handler");
            let resp = Response::error(StatusCode::BadRequest, "Invalid command variant");
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
        }
    }

    Ok(())
}
