use super::storage::{AUTH_MAGIC, AuthStorage, AuthWalStorage};
use super::types::{PermissionSet, User};
use crate::shared::storage_header::BinaryHeader;
use std::fs::File;
use std::path::PathBuf;
use tempfile::tempdir;

fn read_header_magic(path: &PathBuf) -> [u8; 8] {
    let mut file = File::open(path).expect("open auth swal");
    let header = BinaryHeader::read_from(&mut file).expect("read header");
    header.magic
}

#[test]
fn writes_storage_header_and_payload() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("auth.swal");
    let storage = AuthWalStorage::new(path.clone()).expect("create auth storage");

    let mut perms = std::collections::HashMap::new();
    perms.insert("events".to_string(), PermissionSet::read_write());
    let user = User {
        user_id: "swal_user".to_string(),
        secret_key: "sekret".to_string(),
        active: true,
        created_at: 0,
        roles: vec!["admin".to_string()],
        permissions: perms,
    };

    storage.persist_user(&user).expect("persist user");

    let magic = read_header_magic(&path);
    assert_eq!(magic, AUTH_MAGIC);
}
