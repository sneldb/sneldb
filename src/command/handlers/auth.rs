use crate::command::types::Command;
use crate::engine::auth::{AuthManager, BYPASS_USER_ID};
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::{error, info};

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    auth_manager: &Arc<AuthManager>,
    user_id: Option<&str>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    // Requires auth and admin role (skip if bypass)
    let authenticated_user_id = match user_id {
        Some(uid) => uid,
        None => {
            let resp = Response::error(StatusCode::Unauthorized, "Authentication required");
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
            return Ok(());
        }
    };

    // Skip permission checks for bypass user
    if authenticated_user_id != BYPASS_USER_ID
        && !auth_manager.is_admin(authenticated_user_id).await
    {
        let resp = Response::error(StatusCode::Forbidden, "Only admin users can manage users");
        writer.write_all(&renderer.render(&resp)).await?;
        writer.flush().await?;
        return Ok(());
    }

    match cmd {
        Command::CreateUser {
            user_id,
            secret_key,
            roles,
        } => {
            let result = if let Some(roles_vec) = roles {
                auth_manager
                    .create_user_with_roles(user_id.clone(), secret_key.clone(), roles_vec.clone())
                    .await
            } else {
                auth_manager
                    .create_user(user_id.clone(), secret_key.clone())
                    .await
            };

            match result {
                Ok(key) => {
                    info!(target: "sneldb::auth", user_id, admin_user = authenticated_user_id, "User created");
                    let resp = Response::ok_lines(vec![
                        format!("User '{}' created", user_id),
                        format!("Secret key: {}", key),
                    ]);
                    writer.write_all(&renderer.render(&resp)).await?;
                    writer.flush().await?;
                }
                Err(e) => {
                    error!(target: "sneldb::auth", user_id, error = %e, "Failed to create user");
                    let resp = Response::error(StatusCode::BadRequest, e.to_string());
                    writer.write_all(&renderer.render(&resp)).await?;
                    writer.flush().await?;
                }
            }
        }
        Command::RevokeKey { user_id } => match auth_manager.revoke_key(user_id).await {
            Ok(_) => {
                info!(target: "sneldb::auth", user_id, admin_user = authenticated_user_id, "User key revoked");
                let resp = Response::ok_lines(vec![format!("Key revoked for user '{}'", user_id)]);
                writer.write_all(&renderer.render(&resp)).await?;
                writer.flush().await?;
            }
            Err(e) => {
                error!(target: "sneldb::auth", user_id, error = %e, "Failed to revoke key");
                let resp = Response::error(StatusCode::BadRequest, e.to_string());
                writer.write_all(&renderer.render(&resp)).await?;
                writer.flush().await?;
            }
        },
        Command::ListUsers => {
            let users = auth_manager.list_users().await;
            let lines: Vec<String> = users
                .iter()
                .map(|u| {
                    format!(
                        "{}: {}",
                        u.user_id,
                        if u.active { "active" } else { "inactive" }
                    )
                })
                .collect();
            let resp = if lines.is_empty() {
                Response::ok_lines(vec!["No users found".to_string()])
            } else {
                Response::ok_lines(lines)
            };
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
        }
        _ => {
            error!(target: "sneldb::auth", "Invalid command variant for auth handler");
            let resp = Response::error(StatusCode::BadRequest, "Invalid command variant");
            writer.write_all(&renderer.render(&resp)).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}
