use crate::engine::auth::types::{PermissionCache, PermissionSet, UserCache, UserKey};
use crate::engine::auth::types::{SessionStore, SessionToken};
use crate::logging::init_for_tests;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn create_user_key(
    user_id: &str,
    roles: Vec<&str>,
    permissions: HashMap<String, PermissionSet>,
) -> UserKey {
    UserKey {
        user_id: user_id.to_string(),
        secret_key: "secret".to_string(),
        active: true,
        created_at: 1000,
        roles: roles.iter().map(|s| s.to_string()).collect(),
        permissions,
    }
}

// ─────────────────────────────
// Admin Role Tests
// ─────────────────────────────

#[test]
fn test_admin_role_can_read_all() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("admin_user", vec!["admin"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.can_read("admin_user", "any_event_type"));
    assert!(cache.can_read("admin_user", "another_event"));
    assert!(cache.is_admin("admin_user"));
}

#[test]
fn test_admin_role_can_write_all() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("admin_user", vec!["admin"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.can_write("admin_user", "any_event_type"));
    assert!(cache.can_write("admin_user", "another_event"));
}

#[test]
fn test_admin_role_overrides_permissions() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::none());
    let user = create_user_key("admin_user", vec!["admin"], perms);
    cache.update_user(&user);

    // Admin should have access even if permissions say no
    assert!(cache.can_read("admin_user", "event1"));
    assert!(cache.can_write("admin_user", "event1"));
}

// ─────────────────────────────
// Read-Only Role Tests
// ─────────────────────────────

#[test]
fn test_read_only_role_can_read_all() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("readonly_user", vec!["read-only"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.can_read("readonly_user", "any_event_type"));
    assert!(cache.can_read("readonly_user", "another_event"));
    assert!(!cache.can_write("readonly_user", "any_event_type"));
    assert!(!cache.can_write("readonly_user", "another_event"));
}

#[test]
fn test_viewer_role_alias_for_read_only() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("viewer_user", vec!["viewer"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.can_read("viewer_user", "any_event_type"));
    assert!(!cache.can_write("viewer_user", "any_event_type"));
    assert!(cache.has_role("viewer_user", "read-only"));
    assert!(cache.has_role("viewer_user", "viewer"));
}

#[test]
fn test_read_only_role_with_permissions() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::none());
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // Permissions override roles - if permissions say no, deny
    assert!(!cache.can_read("readonly_user", "event1"));
    assert!(!cache.can_write("readonly_user", "event1"));
}

#[test]
fn test_read_only_role_can_write_with_permission() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // Permissions override roles - read-only user CAN write if permissions allow it
    assert!(cache.can_read("readonly_user", "event1"));
    assert!(cache.can_write("readonly_user", "event1"));
}

// ─────────────────────────────
// Editor Role Tests
// ─────────────────────────────

#[test]
fn test_editor_role_can_read_and_write_all() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("editor_user", vec!["editor"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.can_read("editor_user", "any_event_type"));
    assert!(cache.can_write("editor_user", "any_event_type"));
    assert!(!cache.is_admin("editor_user"));
}

#[test]
fn test_editor_role_with_permissions() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::none());
    let user = create_user_key("editor_user", vec!["editor"], perms);
    cache.update_user(&user);

    // Permissions override roles - if permissions say no, deny
    assert!(!cache.can_read("editor_user", "event1"));
    assert!(!cache.can_write("editor_user", "event1"));
}

#[test]
fn test_editor_role_not_admin() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("editor_user", vec!["editor"], HashMap::new());
    cache.update_user(&user);

    assert!(!cache.is_admin("editor_user"));
}

// ─────────────────────────────
// Write-Only Role Tests
// ─────────────────────────────

#[test]
fn test_write_only_role_can_write_all() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("writeonly_user", vec!["write-only"], HashMap::new());
    cache.update_user(&user);

    assert!(!cache.can_read("writeonly_user", "any_event_type"));
    assert!(cache.can_write("writeonly_user", "any_event_type"));
}

#[test]
fn test_write_only_role_with_permissions() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_only());
    let user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&user);

    // Permissions override roles - write-only user CAN read if permissions allow it
    assert!(cache.can_read("writeonly_user", "event1"));
    assert!(!cache.can_write("writeonly_user", "event1")); // permissions say read-only
}

#[test]
fn test_write_only_role_can_read_with_permission() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());
    let user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&user);

    // Permissions override roles - write-only user CAN read if permissions allow it
    assert!(cache.can_read("writeonly_user", "event1"));
    assert!(cache.can_write("writeonly_user", "event1"));
}

// ─────────────────────────────
// Multiple Roles Tests
// ─────────────────────────────

#[test]
fn test_multiple_roles_admin_takes_precedence() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key(
        "multi_role_user",
        vec!["admin", "read-only", "editor"],
        HashMap::new(),
    );
    cache.update_user(&user);

    // Admin should take precedence
    assert!(cache.can_read("multi_role_user", "event1"));
    assert!(cache.can_write("multi_role_user", "event1"));
    assert!(cache.is_admin("multi_role_user"));
}

#[test]
fn test_role_removal() {
    init_for_tests();

    let mut cache = PermissionCache::new();

    // Add user with admin role
    let user1 = create_user_key("test_user", vec!["admin"], HashMap::new());
    cache.update_user(&user1);
    assert!(cache.is_admin("test_user"));

    // Remove admin role
    let user2 = create_user_key("test_user", vec![], HashMap::new());
    cache.update_user(&user2);
    assert!(!cache.is_admin("test_user"));
}

#[test]
fn test_role_change() {
    init_for_tests();

    let mut cache = PermissionCache::new();

    // Start with read-only
    let user1 = create_user_key("test_user", vec!["read-only"], HashMap::new());
    cache.update_user(&user1);
    assert!(cache.can_read("test_user", "event1"));
    assert!(!cache.can_write("test_user", "event1"));

    // Change to editor
    let user2 = create_user_key("test_user", vec!["editor"], HashMap::new());
    cache.update_user(&user2);
    assert!(cache.can_read("test_user", "event1"));
    assert!(cache.can_write("test_user", "event1"));
    assert!(!cache.has_role("test_user", "read-only"));
}

// ─────────────────────────────
// Permissions Without Roles Tests
// ─────────────────────────────

#[test]
fn test_permissions_without_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());
    perms.insert("event2".to_string(), PermissionSet::read_only());
    perms.insert("event3".to_string(), PermissionSet::write_only());
    perms.insert("event4".to_string(), PermissionSet::none());

    let user = create_user_key("regular_user", vec![], perms);
    cache.update_user(&user);

    assert!(cache.can_read("regular_user", "event1"));
    assert!(cache.can_write("regular_user", "event1"));

    assert!(cache.can_read("regular_user", "event2"));
    assert!(!cache.can_write("regular_user", "event2"));

    assert!(!cache.can_read("regular_user", "event3"));
    assert!(cache.can_write("regular_user", "event3"));

    assert!(!cache.can_read("regular_user", "event4"));
    assert!(!cache.can_write("regular_user", "event4"));
}

#[test]
fn test_permissions_for_unknown_event_type() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());

    let user = create_user_key("regular_user", vec![], perms);
    cache.update_user(&user);

    // No permissions for unknown event type
    assert!(!cache.can_read("regular_user", "unknown_event"));
    assert!(!cache.can_write("regular_user", "unknown_event"));
}

// ─────────────────────────────
// Has Role Tests
// ─────────────────────────────

#[test]
fn test_has_role_admin() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("admin_user", vec!["admin"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.has_role("admin_user", "admin"));
    assert!(!cache.has_role("admin_user", "read-only"));
    assert!(!cache.has_role("admin_user", "editor"));
}

#[test]
fn test_has_role_read_only() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("readonly_user", vec!["read-only"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.has_role("readonly_user", "read-only"));
    assert!(cache.has_role("readonly_user", "viewer")); // alias
    assert!(!cache.has_role("readonly_user", "admin"));
}

#[test]
fn test_has_role_editor() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("editor_user", vec!["editor"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.has_role("editor_user", "editor"));
    assert!(!cache.has_role("editor_user", "admin"));
}

#[test]
fn test_has_role_write_only() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("writeonly_user", vec!["write-only"], HashMap::new());
    cache.update_user(&user);

    assert!(cache.has_role("writeonly_user", "write-only"));
    assert!(!cache.has_role("writeonly_user", "admin"));
}

#[test]
fn test_has_role_unknown_role() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("test_user", vec!["unknown-role"], HashMap::new());
    cache.update_user(&user);

    assert!(!cache.has_role("test_user", "unknown-role"));
    assert!(!cache.has_role("test_user", "admin"));
}

#[test]
fn test_has_role_nonexistent_user() {
    init_for_tests();

    let cache = PermissionCache::new();

    assert!(!cache.has_role("nonexistent", "admin"));
    assert!(!cache.has_role("nonexistent", "read-only"));
}

// ─────────────────────────────
// Edge Cases
// ─────────────────────────────

#[test]
fn test_empty_permissions_map() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("test_user", vec![], HashMap::new());
    cache.update_user(&user);

    assert!(!cache.can_read("test_user", "any_event"));
    assert!(!cache.can_write("test_user", "any_event"));
}

#[test]
fn test_user_with_no_roles_and_no_permissions() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key("test_user", vec![], HashMap::new());
    cache.update_user(&user);

    assert!(!cache.can_read("test_user", "event1"));
    assert!(!cache.can_write("test_user", "event1"));
    assert!(!cache.is_admin("test_user"));
}

#[test]
fn test_role_case_sensitivity() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    // Roles are case-sensitive - "Admin" != "admin"
    let user = create_user_key("test_user", vec!["Admin"], HashMap::new());
    cache.update_user(&user);

    assert!(!cache.is_admin("test_user"));
    assert!(!cache.has_role("test_user", "admin"));
}

#[test]
fn test_permission_removal() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());
    let user1 = create_user_key("test_user", vec![], perms);
    cache.update_user(&user1);

    assert!(cache.can_read("test_user", "event1"));

    // Remove permissions
    let user2 = create_user_key("test_user", vec![], HashMap::new());
    cache.update_user(&user2);

    assert!(!cache.can_read("test_user", "event1"));
}

// ─────────────────────────────
// Priority Tests
// ─────────────────────────────

#[test]
fn test_admin_overrides_all_other_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key(
        "admin_user",
        vec!["admin", "read-only", "editor", "write-only"],
        HashMap::new(),
    );
    cache.update_user(&user);

    // Admin should have full access regardless of other roles
    assert!(cache.can_read("admin_user", "event1"));
    assert!(cache.can_write("admin_user", "event1"));
    assert!(cache.is_admin("admin_user"));
}

#[test]
fn test_permissions_priority_over_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::none());

    // Permissions override roles - if permissions say none, deny even with read-only role
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    assert!(!cache.can_read("readonly_user", "event1"));
    assert!(!cache.can_write("readonly_user", "event1"));
}

#[test]
fn test_permissions_override_editor_role() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_only());

    // Permissions override roles - editor role cannot write if permissions say read-only
    let user = create_user_key("editor_user", vec!["editor"], perms);
    cache.update_user(&user);

    assert!(cache.can_read("editor_user", "event1"));
    assert!(!cache.can_write("editor_user", "event1"));
}

#[test]
fn test_permissions_override_write_only_role() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());

    // Permissions override roles - write-only user CAN read if permissions allow it
    let user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&user);

    assert!(cache.can_read("writeonly_user", "event1"));
    assert!(cache.can_write("writeonly_user", "event1"));
}

// ─────────────────────────────
// Permissions Override Roles - Explicit Tests
// ─────────────────────────────

#[test]
fn test_read_only_role_with_write_permission() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("special_event".to_string(), PermissionSet::write_only());
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // Read-only role but has write permission on specific event
    // Can read because permission doesn't grant read, so falls back to role
    assert!(cache.can_read("readonly_user", "special_event"));
    // Can write because permission grants it
    assert!(cache.can_write("readonly_user", "special_event"));

    // Other events without permissions - read-only role applies
    assert!(cache.can_read("readonly_user", "other_event"));
    assert!(!cache.can_write("readonly_user", "other_event"));
}

#[test]
fn test_write_only_role_with_read_permission() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("special_event".to_string(), PermissionSet::read_only());
    let user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&user);

    // Write-only role but has read permission on specific event - should allow read
    assert!(cache.can_read("writeonly_user", "special_event"));
    assert!(!cache.can_write("writeonly_user", "special_event"));

    // Other events without permissions - write-only role applies
    assert!(!cache.can_read("writeonly_user", "other_event"));
    assert!(cache.can_write("writeonly_user", "other_event"));
}

#[test]
fn test_editor_role_with_restrictive_permission() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("restricted_event".to_string(), PermissionSet::read_only());
    let user = create_user_key("editor_user", vec!["editor"], perms);
    cache.update_user(&user);

    // Editor role but permission restricts to read-only - should respect permission
    assert!(cache.can_read("editor_user", "restricted_event"));
    assert!(!cache.can_write("editor_user", "restricted_event"));

    // Other events without permissions - editor role applies
    assert!(cache.can_read("editor_user", "other_event"));
    assert!(cache.can_write("editor_user", "other_event"));
}

#[test]
fn test_permissions_override_multiple_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());
    perms.insert("event2".to_string(), PermissionSet::read_only());
    perms.insert("event3".to_string(), PermissionSet::write_only());
    perms.insert("event4".to_string(), PermissionSet::none());

    // User with read-only role but various permissions
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // event1: read_write permission - can read and write (overrides read-only)
    assert!(cache.can_read("readonly_user", "event1"));
    assert!(cache.can_write("readonly_user", "event1"));

    // event2: read_only permission - can read, cannot write (matches role)
    assert!(cache.can_read("readonly_user", "event2"));
    assert!(!cache.can_write("readonly_user", "event2"));

    // event3: write_only permission - can read from role (permission doesn't grant read), can write from permission
    assert!(cache.can_read("readonly_user", "event3")); // Falls back to role
    assert!(cache.can_write("readonly_user", "event3")); // From permission

    // event4: none permission - cannot read or write (overrides read-only)
    assert!(!cache.can_read("readonly_user", "event4"));
    assert!(!cache.can_write("readonly_user", "event4"));

    // event5: no permission - read-only role applies
    assert!(cache.can_read("readonly_user", "event5"));
    assert!(!cache.can_write("readonly_user", "event5"));
}

// ─────────────────────────────
// Permission Override Edge Cases - Comprehensive Tests
// ─────────────────────────────

/// Test: GRANT WRITE on read-only user
/// Expected: Can WRITE (permission grants it), can READ (role provides it)
#[test]
fn test_read_only_role_grant_write_permission_read_from_role() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    // Grant only WRITE permission (read=false, write=true)
    perms.insert("event1".to_string(), PermissionSet::write_only());
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // Can write because permission grants it
    assert!(cache.can_write("readonly_user", "event1"));
    // Can read because role provides it (permission doesn't grant read, so falls back to role)
    assert!(cache.can_read("readonly_user", "event1"));

    // Other events without permissions - read-only role applies
    assert!(cache.can_read("readonly_user", "other_event"));
    assert!(!cache.can_write("readonly_user", "other_event"));
}

/// Test: GRANT READ then REVOKE WRITE on editor user
/// Expected: Can READ (permission grants it), cannot WRITE (permission denies it, overrides role)
#[test]
fn test_editor_role_grant_read_revoke_write_permission_denies_write() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    // Grant READ, deny WRITE (read=true, write=false)
    perms.insert("event1".to_string(), PermissionSet::read_only());
    let user = create_user_key("editor_user", vec!["editor"], perms);
    cache.update_user(&user);

    // Can read because permission grants it
    assert!(cache.can_read("editor_user", "event1"));
    // Cannot write because permission denies it (overrides editor role)
    assert!(!cache.can_write("editor_user", "event1"));

    // Other events without permissions - editor role applies
    assert!(cache.can_read("editor_user", "other_event"));
    assert!(cache.can_write("editor_user", "other_event"));
}

/// Test: REVOKE ALL permissions (read=false, write=false)
/// Expected: Explicitly denies both READ and WRITE, overriding role
#[test]
fn test_revoke_all_permissions_explicitly_denies_overrides_role() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    // Explicit denial: both read and write are false
    perms.insert("event1".to_string(), PermissionSet::none());

    // Test with read-only role
    let readonly_user = create_user_key("readonly_user", vec!["read-only"], perms.clone());
    cache.update_user(&readonly_user);
    assert!(!cache.can_read("readonly_user", "event1"));
    assert!(!cache.can_write("readonly_user", "event1"));

    // Test with editor role
    let editor_user = create_user_key("editor_user", vec!["editor"], perms.clone());
    cache.update_user(&editor_user);
    assert!(!cache.can_read("editor_user", "event1"));
    assert!(!cache.can_write("editor_user", "event1"));

    // Test with write-only role
    let writeonly_user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&writeonly_user);
    assert!(!cache.can_read("writeonly_user", "event1"));
    assert!(!cache.can_write("writeonly_user", "event1"));
}

/// Test: Permission set with read=false, write=true falls back to role for READ
#[test]
fn test_write_only_permission_falls_back_to_role_for_read() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::write_only());

    // Read-only role: can read from role, can write from permission
    let readonly_user = create_user_key("readonly_user", vec!["read-only"], perms.clone());
    cache.update_user(&readonly_user);
    assert!(cache.can_read("readonly_user", "event1")); // From role
    assert!(cache.can_write("readonly_user", "event1")); // From permission

    // Editor role: can read from role, can write from permission
    let editor_user = create_user_key("editor_user", vec!["editor"], perms.clone());
    cache.update_user(&editor_user);
    assert!(cache.can_read("editor_user", "event1")); // From role
    assert!(cache.can_write("editor_user", "event1")); // From permission

    // Write-only role: cannot read (role denies), can write from permission
    let writeonly_user = create_user_key("writeonly_user", vec!["write-only"], perms.clone());
    cache.update_user(&writeonly_user);
    assert!(!cache.can_read("writeonly_user", "event1")); // Role denies
    assert!(cache.can_write("writeonly_user", "event1")); // From permission

    // No role: cannot read (no role), can write from permission
    let no_role_user = create_user_key("no_role_user", vec![], perms);
    cache.update_user(&no_role_user);
    assert!(!cache.can_read("no_role_user", "event1")); // No role
    assert!(cache.can_write("no_role_user", "event1")); // From permission
}

/// Test: Permission set with read=true, write=false overrides role for WRITE
#[test]
fn test_read_only_permission_overrides_role_for_write() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_only());

    // Editor role: can read from permission, cannot write (permission denies, overrides role)
    let editor_user = create_user_key("editor_user", vec!["editor"], perms.clone());
    cache.update_user(&editor_user);
    assert!(cache.can_read("editor_user", "event1")); // From permission
    assert!(!cache.can_write("editor_user", "event1")); // Permission denies, overrides role

    // Write-only role: can read from permission, cannot write (permission denies, overrides role)
    let writeonly_user = create_user_key("writeonly_user", vec!["write-only"], perms.clone());
    cache.update_user(&writeonly_user);
    assert!(cache.can_read("writeonly_user", "event1")); // From permission
    assert!(!cache.can_write("writeonly_user", "event1")); // Permission denies, overrides role

    // Read-only role: can read from permission, cannot write (matches role)
    let readonly_user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&readonly_user);
    assert!(cache.can_read("readonly_user", "event1")); // From permission
    assert!(!cache.can_write("readonly_user", "event1")); // Permission denies
}

/// Test: Multiple event types with different permission patterns
#[test]
fn test_multiple_events_different_permission_patterns() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("write_only_event".to_string(), PermissionSet::write_only());
    perms.insert("read_only_event".to_string(), PermissionSet::read_only());
    perms.insert("none_event".to_string(), PermissionSet::none());
    perms.insert("read_write_event".to_string(), PermissionSet::read_write());

    // Read-only user with various permissions
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // write_only_event: can read from role, can write from permission
    assert!(cache.can_read("readonly_user", "write_only_event"));
    assert!(cache.can_write("readonly_user", "write_only_event"));

    // read_only_event: can read from permission, cannot write (permission denies)
    assert!(cache.can_read("readonly_user", "read_only_event"));
    assert!(!cache.can_write("readonly_user", "read_only_event"));

    // none_event: cannot read or write (explicit denial overrides role)
    assert!(!cache.can_read("readonly_user", "none_event"));
    assert!(!cache.can_write("readonly_user", "none_event"));

    // read_write_event: can read and write (permission grants both)
    assert!(cache.can_read("readonly_user", "read_write_event"));
    assert!(cache.can_write("readonly_user", "read_write_event"));

    // no_permission_event: can read from role, cannot write (role applies)
    assert!(cache.can_read("readonly_user", "no_permission_event"));
    assert!(!cache.can_write("readonly_user", "no_permission_event"));
}

/// Test: Editor role with GRANT READ then REVOKE WRITE scenario
#[test]
fn test_editor_role_grant_read_revoke_write_scenario() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    // Simulates: GRANT READ then REVOKE WRITE
    // Result: read=true, write=false
    perms.insert("restricted_event".to_string(), PermissionSet::read_only());
    let user = create_user_key("editor_user", vec!["editor"], perms);
    cache.update_user(&user);

    // Can read (permission grants it)
    assert!(cache.can_read("editor_user", "restricted_event"));
    // Cannot write (permission denies it, overrides editor role)
    assert!(!cache.can_write("editor_user", "restricted_event"));

    // Other events: editor role applies
    assert!(cache.can_read("editor_user", "other_event"));
    assert!(cache.can_write("editor_user", "other_event"));
}

/// Test: Write-only role with GRANT READ scenario
#[test]
fn test_write_only_role_grant_read_scenario() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    // Grant READ permission (read=true, write=false)
    perms.insert("readable_event".to_string(), PermissionSet::read_only());
    let user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&user);

    // Can read (permission grants it)
    assert!(cache.can_read("writeonly_user", "readable_event"));
    // Cannot write (permission denies it, overrides write-only role)
    assert!(!cache.can_write("writeonly_user", "readable_event"));

    // Other events: write-only role applies
    assert!(!cache.can_read("writeonly_user", "other_event"));
    assert!(cache.can_write("writeonly_user", "other_event"));
}

/// Test: Read-only role with GRANT WRITE scenario
#[test]
fn test_read_only_role_grant_write_scenario() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    // Grant WRITE permission (read=false, write=true)
    perms.insert("writable_event".to_string(), PermissionSet::write_only());
    let user = create_user_key("readonly_user", vec!["read-only"], perms);
    cache.update_user(&user);

    // Can read (role provides it, permission doesn't grant so falls back to role)
    assert!(cache.can_read("readonly_user", "writable_event"));
    // Can write (permission grants it)
    assert!(cache.can_write("readonly_user", "writable_event"));

    // Other events: read-only role applies
    assert!(cache.can_read("readonly_user", "other_event"));
    assert!(!cache.can_write("readonly_user", "other_event"));
}

/// Test: Permission set with read=false, write=false explicitly denies both
#[test]
fn test_none_permission_explicitly_denies_both() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("denied_event".to_string(), PermissionSet::none());

    // Test all roles - explicit denial should override all
    let readonly_user = create_user_key("readonly_user", vec!["read-only"], perms.clone());
    cache.update_user(&readonly_user);
    assert!(!cache.can_read("readonly_user", "denied_event"));
    assert!(!cache.can_write("readonly_user", "denied_event"));

    let editor_user = create_user_key("editor_user", vec!["editor"], perms.clone());
    cache.update_user(&editor_user);
    assert!(!cache.can_read("editor_user", "denied_event"));
    assert!(!cache.can_write("editor_user", "denied_event"));

    let writeonly_user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&writeonly_user);
    assert!(!cache.can_read("writeonly_user", "denied_event"));
    assert!(!cache.can_write("writeonly_user", "denied_event"));
}

/// Test: Permission set with read=true, write=true grants both
#[test]
fn test_read_write_permission_grants_both() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let mut perms = HashMap::new();
    perms.insert("full_access_event".to_string(), PermissionSet::read_write());

    // Test all roles - permission grants both
    let readonly_user = create_user_key("readonly_user", vec!["read-only"], perms.clone());
    cache.update_user(&readonly_user);
    assert!(cache.can_read("readonly_user", "full_access_event"));
    assert!(cache.can_write("readonly_user", "full_access_event"));

    let editor_user = create_user_key("editor_user", vec!["editor"], perms.clone());
    cache.update_user(&editor_user);
    assert!(cache.can_read("editor_user", "full_access_event"));
    assert!(cache.can_write("editor_user", "full_access_event"));

    let writeonly_user = create_user_key("writeonly_user", vec!["write-only"], perms);
    cache.update_user(&writeonly_user);
    assert!(cache.can_read("writeonly_user", "full_access_event"));
    assert!(cache.can_write("writeonly_user", "full_access_event"));
}

/// Test: Complex scenario - user transitions through permission states
#[test]
fn test_permission_state_transitions() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user_id = "test_user";
    let event_type = "event1";

    // Start: read-only role, no permissions
    let mut user = create_user_key(user_id, vec!["read-only"], HashMap::new());
    cache.update_user(&user);
    assert!(cache.can_read(user_id, event_type));
    assert!(!cache.can_write(user_id, event_type));

    // Step 1: Grant WRITE permission (read=false, write=true)
    user.permissions
        .insert(event_type.to_string(), PermissionSet::write_only());
    cache.update_user(&user);
    assert!(cache.can_read(user_id, event_type)); // From role
    assert!(cache.can_write(user_id, event_type)); // From permission

    // Step 2: Grant READ permission (read=true, write=true)
    user.permissions
        .insert(event_type.to_string(), PermissionSet::read_write());
    cache.update_user(&user);
    assert!(cache.can_read(user_id, event_type)); // From permission
    assert!(cache.can_write(user_id, event_type)); // From permission

    // Step 3: Revoke WRITE (read=true, write=false)
    user.permissions
        .insert(event_type.to_string(), PermissionSet::read_only());
    cache.update_user(&user);
    assert!(cache.can_read(user_id, event_type)); // From permission
    assert!(!cache.can_write(user_id, event_type)); // Permission denies, overrides role

    // Step 4: Revoke READ (read=false, write=false)
    user.permissions
        .insert(event_type.to_string(), PermissionSet::none());
    cache.update_user(&user);
    assert!(!cache.can_read(user_id, event_type)); // Explicit denial
    assert!(!cache.can_write(user_id, event_type)); // Explicit denial

    // Step 5: Remove permission entirely
    user.permissions.remove(event_type);
    cache.update_user(&user);
    assert!(cache.can_read(user_id, event_type)); // Back to role
    assert!(!cache.can_write(user_id, event_type)); // Back to role
}

// ─────────────────────────────
// Multiple Users Tests
// ─────────────────────────────

#[test]
fn test_multiple_users_different_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();

    let admin = create_user_key("admin", vec!["admin"], HashMap::new());
    let readonly = create_user_key("readonly", vec!["read-only"], HashMap::new());
    let editor = create_user_key("editor", vec!["editor"], HashMap::new());
    let writeonly = create_user_key("writeonly", vec!["write-only"], HashMap::new());
    let regular = {
        let mut perms = HashMap::new();
        perms.insert("event1".to_string(), PermissionSet::read_write());
        create_user_key("regular", vec![], perms)
    };

    cache.update_user(&admin);
    cache.update_user(&readonly);
    cache.update_user(&editor);
    cache.update_user(&writeonly);
    cache.update_user(&regular);

    // Admin
    assert!(cache.can_read("admin", "event1"));
    assert!(cache.can_write("admin", "event1"));
    assert!(cache.is_admin("admin"));

    // Read-only
    assert!(cache.can_read("readonly", "event1"));
    assert!(!cache.can_write("readonly", "event1"));
    assert!(!cache.is_admin("readonly"));

    // Editor
    assert!(cache.can_read("editor", "event1"));
    assert!(cache.can_write("editor", "event1"));
    assert!(!cache.is_admin("editor"));

    // Write-only
    assert!(!cache.can_read("writeonly", "event1"));
    assert!(cache.can_write("writeonly", "event1"));
    assert!(!cache.is_admin("writeonly"));

    // Regular (permissions only)
    assert!(cache.can_read("regular", "event1"));
    assert!(cache.can_write("regular", "event1"));
    assert!(!cache.can_read("regular", "event2")); // No permission
    assert!(!cache.is_admin("regular"));
}

// ─────────────────────────────
// PermissionSet Tests
// ─────────────────────────────

#[test]
fn test_permission_set_new() {
    init_for_tests();

    let perm = PermissionSet::new(true, false);
    assert!(perm.read);
    assert!(!perm.write);

    let perm = PermissionSet::new(false, true);
    assert!(!perm.read);
    assert!(perm.write);

    let perm = PermissionSet::new(true, true);
    assert!(perm.read);
    assert!(perm.write);

    let perm = PermissionSet::new(false, false);
    assert!(!perm.read);
    assert!(!perm.write);
}

#[test]
fn test_permission_set_read_only() {
    init_for_tests();

    let perm = PermissionSet::read_only();
    assert!(perm.read);
    assert!(!perm.write);
}

#[test]
fn test_permission_set_write_only() {
    init_for_tests();

    let perm = PermissionSet::write_only();
    assert!(!perm.read);
    assert!(perm.write);
}

#[test]
fn test_permission_set_read_write() {
    init_for_tests();

    let perm = PermissionSet::read_write();
    assert!(perm.read);
    assert!(perm.write);
}

#[test]
fn test_permission_set_none() {
    init_for_tests();

    let perm = PermissionSet::none();
    assert!(!perm.read);
    assert!(!perm.write);
}

// ─────────────────────────────
// PermissionCache Construction Tests
// ─────────────────────────────

#[test]
fn test_permission_cache_new() {
    init_for_tests();

    let cache = PermissionCache::new();
    // Verify cache is empty
    assert!(!cache.can_read("any_user", "any_event"));
    assert!(!cache.can_write("any_user", "any_event"));
    assert!(!cache.is_admin("any_user"));
}

#[test]
fn test_permission_cache_default() {
    init_for_tests();

    let cache = PermissionCache::default();
    // Verify default is same as new
    assert!(!cache.can_read("any_user", "any_event"));
    assert!(!cache.can_write("any_user", "any_event"));
    assert!(!cache.is_admin("any_user"));
}

// ─────────────────────────────
// PermissionCache::update_user Edge Cases
// ─────────────────────────────

#[test]
fn test_update_user_ignores_unknown_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key(
        "test_user",
        vec!["unknown-role", "another-unknown"],
        HashMap::new(),
    );
    cache.update_user(&user);

    // Unknown roles should be ignored - user should have no access
    assert!(!cache.can_read("test_user", "event1"));
    assert!(!cache.can_write("test_user", "event1"));
    assert!(!cache.is_admin("test_user"));
    assert!(!cache.has_role("test_user", "unknown-role"));
}

#[test]
fn test_update_user_with_mixed_known_and_unknown_roles() {
    init_for_tests();

    let mut cache = PermissionCache::new();
    let user = create_user_key(
        "test_user",
        vec!["read-only", "unknown-role"],
        HashMap::new(),
    );
    cache.update_user(&user);

    // Known role should work, unknown should be ignored
    assert!(cache.can_read("test_user", "event1"));
    assert!(!cache.can_write("test_user", "event1"));
    assert!(cache.has_role("test_user", "read-only"));
    assert!(!cache.has_role("test_user", "unknown-role"));
}

#[test]
fn test_update_user_removes_empty_permissions() {
    init_for_tests();

    let mut cache = PermissionCache::new();

    // Add user with permissions
    let mut perms = HashMap::new();
    perms.insert("event1".to_string(), PermissionSet::read_write());
    let user1 = create_user_key("test_user", vec![], perms);
    cache.update_user(&user1);
    assert!(cache.can_read("test_user", "event1"));

    // Update with empty permissions - should remove from cache
    let user2 = create_user_key("test_user", vec![], HashMap::new());
    cache.update_user(&user2);
    assert!(!cache.can_read("test_user", "event1"));
}

#[test]
fn test_update_user_multiple_times() {
    init_for_tests();

    let mut cache = PermissionCache::new();

    // Start with read-only
    let user1 = create_user_key("test_user", vec!["read-only"], HashMap::new());
    cache.update_user(&user1);
    assert!(cache.can_read("test_user", "event1"));
    assert!(!cache.can_write("test_user", "event1"));

    // Change to editor
    let user2 = create_user_key("test_user", vec!["editor"], HashMap::new());
    cache.update_user(&user2);
    assert!(cache.can_read("test_user", "event1"));
    assert!(cache.can_write("test_user", "event1"));
    assert!(!cache.has_role("test_user", "read-only"));

    // Change to admin
    let user3 = create_user_key("test_user", vec!["admin"], HashMap::new());
    cache.update_user(&user3);
    assert!(cache.can_read("test_user", "event1"));
    assert!(cache.can_write("test_user", "event1"));
    assert!(cache.is_admin("test_user"));
    assert!(!cache.has_role("test_user", "editor"));
}

#[test]
fn test_update_user_clears_all_roles_before_adding_new() {
    init_for_tests();

    let mut cache = PermissionCache::new();

    // Add user with admin role
    let user1 = create_user_key("test_user", vec!["admin"], HashMap::new());
    cache.update_user(&user1);
    assert!(cache.is_admin("test_user"));

    // Update with empty roles - should clear admin
    let user2 = create_user_key("test_user", vec![], HashMap::new());
    cache.update_user(&user2);
    assert!(!cache.is_admin("test_user"));
    assert!(!cache.can_read("test_user", "event1"));
}

// ─────────────────────────────
// UserCache Tests
// ─────────────────────────────

#[test]
fn test_user_cache_new() {
    init_for_tests();

    let cache = UserCache::new();
    assert_eq!(cache.all_users().len(), 0);
}

#[test]
fn test_user_cache_default() {
    init_for_tests();

    let cache = UserCache::default();
    assert_eq!(cache.all_users().len(), 0);
}

#[test]
fn test_user_cache_insert() {
    init_for_tests();

    let mut cache = UserCache::new();
    let user = create_user_key("user1", vec![], HashMap::new());
    cache.insert(user.clone());

    assert_eq!(cache.all_users().len(), 1);
    assert_eq!(cache.get("user1").unwrap().user_id, "user1");
}

#[test]
fn test_user_cache_insert_overwrites_existing() {
    init_for_tests();

    let mut cache = UserCache::new();
    let user1 = create_user_key("user1", vec![], HashMap::new());
    cache.insert(user1);

    let mut user2 = create_user_key("user1", vec![], HashMap::new());
    user2.secret_key = "new_secret".to_string();
    cache.insert(user2);

    assert_eq!(cache.all_users().len(), 1);
    assert_eq!(cache.get("user1").unwrap().secret_key, "new_secret");
}

#[test]
fn test_user_cache_get() {
    init_for_tests();

    let mut cache = UserCache::new();
    let user = create_user_key("user1", vec![], HashMap::new());
    cache.insert(user);

    let retrieved = cache.get("user1");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().user_id, "user1");

    let not_found = cache.get("nonexistent");
    assert!(not_found.is_none());
}

#[test]
fn test_user_cache_remove() {
    init_for_tests();

    let mut cache = UserCache::new();
    let user = create_user_key("user1", vec![], HashMap::new());
    cache.insert(user);

    assert_eq!(cache.all_users().len(), 1);
    let removed = cache.remove("user1");
    assert!(removed);
    assert_eq!(cache.all_users().len(), 0);
    assert!(cache.get("user1").is_none());
}

#[test]
fn test_user_cache_remove_nonexistent() {
    init_for_tests();

    let mut cache = UserCache::new();
    let removed = cache.remove("nonexistent");
    assert!(!removed);
}

#[test]
fn test_user_cache_clear() {
    init_for_tests();

    let mut cache = UserCache::new();
    cache.insert(create_user_key("user1", vec![], HashMap::new()));
    cache.insert(create_user_key("user2", vec![], HashMap::new()));
    cache.insert(create_user_key("user3", vec![], HashMap::new()));

    assert_eq!(cache.all_users().len(), 3);
    cache.clear();
    assert_eq!(cache.all_users().len(), 0);
}

#[test]
fn test_user_cache_all_users() {
    init_for_tests();

    let mut cache = UserCache::new();
    cache.insert(create_user_key("user1", vec![], HashMap::new()));
    cache.insert(create_user_key("user2", vec![], HashMap::new()));
    cache.insert(create_user_key("user3", vec![], HashMap::new()));

    let all = cache.all_users();
    assert_eq!(all.len(), 3);
    let user_ids: Vec<String> = all.iter().map(|u| u.user_id.clone()).collect();
    assert!(user_ids.contains(&"user1".to_string()));
    assert!(user_ids.contains(&"user2".to_string()));
    assert!(user_ids.contains(&"user3".to_string()));
}

#[test]
fn test_user_cache_multiple_users() {
    init_for_tests();

    let mut cache = UserCache::new();
    cache.insert(create_user_key("user1", vec![], HashMap::new()));
    cache.insert(create_user_key("user2", vec![], HashMap::new()));

    assert_eq!(cache.all_users().len(), 2);
    assert!(cache.get("user1").is_some());
    assert!(cache.get("user2").is_some());
}

// ─────────────────────────────
// SessionStore Tests
// ─────────────────────────────

fn create_session_token(user_id: &str, expires_at: u64) -> SessionToken {
    SessionToken {
        user_id: user_id.to_string(),
        expires_at,
    }
}

#[test]
fn test_session_store_new() {
    init_for_tests();

    let store = SessionStore::new();
    assert_eq!(store.len(), 0);
}

#[test]
fn test_session_store_default() {
    init_for_tests();

    let store = SessionStore::default();
    assert_eq!(store.len(), 0);
}

#[test]
fn test_session_store_insert() {
    init_for_tests();

    let mut store = SessionStore::new();
    let token = "test_token".to_string();
    let session = create_session_token("user1", 1000);
    store.insert(token.clone(), session);

    assert_eq!(store.len(), 1);
    assert!(store.get(&token).is_some());
}

#[test]
fn test_session_store_insert_overwrites_existing() {
    init_for_tests();

    let mut store = SessionStore::new();
    let token = "test_token".to_string();
    let session1 = create_session_token("user1", 1000);
    store.insert(token.clone(), session1);

    let session2 = create_session_token("user2", 2000);
    store.insert(token.clone(), session2);

    assert_eq!(store.len(), 1);
    assert_eq!(store.get(&token).unwrap().user_id, "user2");
}

#[test]
fn test_session_store_get() {
    init_for_tests();

    let mut store = SessionStore::new();
    let token = "test_token".to_string();
    let session = create_session_token("user1", 1000);
    store.insert(token.clone(), session.clone());

    let retrieved = store.get(&token);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().user_id, session.user_id);
    assert_eq!(retrieved.unwrap().expires_at, session.expires_at);

    let not_found = store.get("nonexistent");
    assert!(not_found.is_none());
}

#[test]
fn test_session_store_remove() {
    init_for_tests();

    let mut store = SessionStore::new();
    let token = "test_token".to_string();
    let session = create_session_token("user1", 1000);
    store.insert(token.clone(), session);

    assert_eq!(store.len(), 1);
    let removed = store.remove(&token);
    assert!(removed);
    assert_eq!(store.len(), 0);
    assert!(store.get(&token).is_none());
}

#[test]
fn test_session_store_remove_nonexistent() {
    init_for_tests();

    let mut store = SessionStore::new();
    let removed = store.remove("nonexistent");
    assert!(!removed);
}

#[test]
fn test_session_store_cleanup_expired() {
    init_for_tests();

    let mut store = SessionStore::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Add expired session
    store.insert(
        "expired1".to_string(),
        create_session_token("user1", now - 100),
    );
    // Add non-expired session
    store.insert(
        "active1".to_string(),
        create_session_token("user2", now + 3600),
    );
    // Add another expired session
    store.insert(
        "expired2".to_string(),
        create_session_token("user3", now - 200),
    );

    assert_eq!(store.len(), 3);
    let removed = store.cleanup_expired();
    assert_eq!(removed, 2);
    assert_eq!(store.len(), 1);
    assert!(store.get("expired1").is_none());
    assert!(store.get("active1").is_some());
    assert!(store.get("expired2").is_none());
}

#[test]
fn test_session_store_cleanup_expired_no_expired_sessions() {
    init_for_tests();

    let mut store = SessionStore::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    store.insert(
        "active1".to_string(),
        create_session_token("user1", now + 3600),
    );
    store.insert(
        "active2".to_string(),
        create_session_token("user2", now + 7200),
    );

    assert_eq!(store.len(), 2);
    let removed = store.cleanup_expired();
    assert_eq!(removed, 0);
    assert_eq!(store.len(), 2);
}

#[test]
fn test_session_store_len() {
    init_for_tests();

    let mut store = SessionStore::new();
    assert_eq!(store.len(), 0);

    store.insert("token1".to_string(), create_session_token("user1", 1000));
    assert_eq!(store.len(), 1);

    store.insert("token2".to_string(), create_session_token("user2", 2000));
    assert_eq!(store.len(), 2);

    store.remove("token1");
    assert_eq!(store.len(), 1);
}

#[test]
fn test_session_store_multiple_sessions() {
    init_for_tests();

    let mut store = SessionStore::new();
    store.insert("token1".to_string(), create_session_token("user1", 1000));
    store.insert("token2".to_string(), create_session_token("user2", 2000));
    store.insert("token3".to_string(), create_session_token("user3", 3000));

    assert_eq!(store.len(), 3);
    assert!(store.get("token1").is_some());
    assert!(store.get("token2").is_some());
    assert!(store.get("token3").is_some());
}

#[test]
fn test_session_store_revoke_user_sessions() {
    init_for_tests();

    let mut store = SessionStore::new();
    store.insert("token1".to_string(), create_session_token("user1", 1000));
    store.insert("token2".to_string(), create_session_token("user1", 2000));
    store.insert("token3".to_string(), create_session_token("user2", 3000));
    store.insert("token4".to_string(), create_session_token("user1", 4000));

    assert_eq!(store.len(), 4);

    // Revoke all sessions for user1
    let revoked = store.revoke_user_sessions("user1");
    assert_eq!(revoked, 3);
    assert_eq!(store.len(), 1);

    // Verify user1 tokens are gone
    assert!(store.get("token1").is_none());
    assert!(store.get("token2").is_none());
    assert!(store.get("token4").is_none());

    // Verify user2 token still exists
    assert!(store.get("token3").is_some());
}

#[test]
fn test_session_store_revoke_user_sessions_nonexistent_user() {
    init_for_tests();

    let mut store = SessionStore::new();
    store.insert("token1".to_string(), create_session_token("user1", 1000));

    // Revoke sessions for nonexistent user
    let revoked = store.revoke_user_sessions("nonexistent");
    assert_eq!(revoked, 0);
    assert_eq!(store.len(), 1);
    assert!(store.get("token1").is_some());
}

#[test]
fn test_session_store_revoke_user_sessions_empty_store() {
    init_for_tests();

    let mut store = SessionStore::new();
    let revoked = store.revoke_user_sessions("user1");
    assert_eq!(revoked, 0);
    assert_eq!(store.len(), 0);
}
