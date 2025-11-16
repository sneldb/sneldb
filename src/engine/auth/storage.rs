use crate::engine::auth::types::{AuthError, AuthResult, User};
use crate::shared::config::CONFIG;
use crate::shared::storage_header::MagicFile;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crc32fast::Hasher as Crc32Hasher;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

pub(crate) const AUTH_MAGIC: [u8; 8] = *b"EVDBAUT\0";
const AUTH_VERSION: u16 = 1;
const MAX_FRAME_SIZE: usize = 256 * 1024; // Keep auth frames bounded
const DEFAULT_SYNC_EVERY: u64 = 32;

pub trait AuthStorage: Send + Sync {
    fn persist_user(&self, user: &User) -> AuthResult<()>;
    fn load_users(&self) -> AuthResult<Vec<StoredUser>>;
}

#[derive(Debug, Clone)]
pub struct StoredUser {
    pub user: User,
    pub persisted_at: u64,
}

#[derive(Serialize, Deserialize)]
struct AuthWalRecord {
    ts: u64,
    user: User,
}

struct AuthWalHeader;

impl MagicFile for AuthWalHeader {
    const MAGIC: [u8; 8] = AUTH_MAGIC;
    const VERSION: u16 = AUTH_VERSION;
}

/// Binary WAL for auth users (header + per-frame CRC).
pub struct AuthWalStorage {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    sync_every: u64,
    writes_since_sync: Mutex<u64>,
    key: Key,
}

impl AuthWalStorage {
    pub fn new_with_sync(path: PathBuf, sync_every: Option<u64>) -> AuthResult<Self> {
        std::fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))
            .map_err(|e| AuthError::DatabaseError(format!("create auth wal dir failed: {e}")))?;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&path)
            .map_err(|e| AuthError::DatabaseError(format!("open auth wal failed: {e}")))?;

        // Write header if needed
        if file
            .metadata()
            .map_err(|e| AuthError::DatabaseError(format!("stat auth wal failed: {e}")))?
            .len()
            == 0
        {
            AuthWalHeader::write_header(&mut file).map_err(|e| {
                AuthError::DatabaseError(format!("write auth wal header failed: {e}"))
            })?;
        } else {
            file.seek(SeekFrom::Start(0))
                .map_err(|e| AuthError::DatabaseError(format!("seek auth wal failed: {e}")))?;
            AuthWalHeader::read_and_validate_header(&mut file)
                .map_err(|e| AuthError::DatabaseError(format!("auth wal header invalid: {e}")))?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|e| AuthError::DatabaseError(format!("seek auth wal end failed: {e}")))?;

        Ok(Self {
            path,
            writer: Mutex::new(BufWriter::new(file)),
            sync_every: sync_every.unwrap_or(DEFAULT_SYNC_EVERY),
            writes_since_sync: Mutex::new(0),
            key: Self::load_key(),
        })
    }

    pub fn new(path: PathBuf) -> AuthResult<Self> {
        Self::new_with_sync(path, None)
    }

    pub fn new_default() -> AuthResult<Self> {
        let wal_dir = std::env::var("SNELDB_AUTH_WAL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(&CONFIG.wal.dir).join("auth"));
        let path = wal_dir.join("auth.swal");
        match Self::new_with_sync(path.clone(), None) {
            Ok(storage) => Ok(storage),
            Err(e) => {
                warn!(
                    target: "sneldb::auth",
                    error = %e,
                    path = ?path,
                    "auth wal init failed, falling back to temp dir"
                );
                let tmp = std::env::temp_dir().join("sneldb-auth").join("auth.swal");
                Self::new(tmp)
            }
        }
    }

    fn encode_record(user: &User) -> Result<Vec<u8>, AuthError> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let record = AuthWalRecord {
            ts,
            user: user.clone(),
        };
        bincode::serialize(&record)
            .map_err(|e| AuthError::DatabaseError(format!("serialize auth wal record failed: {e}")))
    }

    fn compute_crc(bytes: &[u8]) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(bytes);
        hasher.finalize()
    }

    fn load_key() -> Key {
        if let Ok(raw) = std::env::var("SNELDB_AUTH_WAL_KEY") {
            if let Ok(bytes) = hex::decode(raw.trim()) {
                if bytes.len() == 32 {
                    return Key::from_slice(&bytes).to_owned();
                }
            }
            warn!(target: "sneldb::auth", "Invalid SNELDB_AUTH_WAL_KEY; falling back to derived key");
        }
        // Derive a deterministic key from the server auth token (or a static string if unavailable).
        let fallback_seed = CONFIG.server.auth_token.as_bytes();
        let mut hasher = Sha256::new();
        hasher.update(fallback_seed);
        let hash = hasher.finalize();
        Key::from_slice(&hash[..32]).to_owned()
    }
}

impl AuthStorage for AuthWalStorage {
    fn persist_user(&self, user: &User) -> AuthResult<()> {
        let payload = Self::encode_record(user)?;
        let mut nonce_bytes = [0u8; 12];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let cipher = ChaCha20Poly1305::new(&self.key);
        let ciphertext = cipher.encrypt(nonce, payload.as_ref()).map_err(|e| {
            AuthError::DatabaseError(format!("encrypt auth wal record failed: {e}"))
        })?;

        let mut frame = Vec::with_capacity(12 + ciphertext.len());
        frame.extend_from_slice(&nonce_bytes);
        frame.extend_from_slice(&ciphertext);

        if frame.len() > MAX_FRAME_SIZE {
            return Err(AuthError::DatabaseError("auth record too large".into()));
        }

        let crc = Self::compute_crc(&frame);
        let len = frame.len() as u32;

        let mut writer = self.writer.lock().expect("auth wal mutex poisoned");
        writer
            .write_all(&len.to_le_bytes())
            .and_then(|_| writer.write_all(&crc.to_le_bytes()))
            .and_then(|_| writer.write_all(&frame))
            .map_err(|e| AuthError::DatabaseError(format!("write auth wal failed: {e}")))?;

        writer
            .flush()
            .map_err(|e| AuthError::DatabaseError(format!("flush auth wal failed: {e}")))?;

        let mut counter = self
            .writes_since_sync
            .lock()
            .expect("writes_since_sync mutex poisoned");
        *counter += 1;
        if CONFIG.wal.fsync && *counter >= self.sync_every {
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| AuthError::DatabaseError(format!("fsync auth wal failed: {e}")))?;
            *counter = 0;
        }

        Ok(())
    }

    fn load_users(&self) -> AuthResult<Vec<StoredUser>> {
        let file = File::open(&self.path)
            .map_err(|e| AuthError::DatabaseError(format!("open auth wal for read failed: {e}")))?;
        let mut reader = BufReader::new(file);

        // Validate header
        AuthWalHeader::read_and_validate_header(&mut reader)
            .map_err(|e| AuthError::DatabaseError(format!("auth wal header invalid: {e}")))?;

        let mut out = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    warn!(target: "sneldb::auth", error = %e, "auth wal truncated while reading length");
                    break;
                }
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 || len > MAX_FRAME_SIZE {
                warn!(
                    target: "sneldb::auth",
                    len,
                    "auth wal frame has invalid length"
                );
                break;
            }

            let mut crc_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut crc_buf) {
                warn!(target: "sneldb::auth", error = %e, "auth wal truncated while reading crc");
                break;
            }
            let expected_crc = u32::from_le_bytes(crc_buf);

            let mut frame = vec![0u8; len];
            if let Err(e) = reader.read_exact(&mut frame) {
                warn!(target: "sneldb::auth", error = %e, "auth wal truncated while reading payload");
                break;
            }

            let actual_crc = Self::compute_crc(&frame);
            if actual_crc != expected_crc {
                warn!(
                    target: "sneldb::auth",
                    expected_crc,
                    actual_crc,
                    "auth wal crc mismatch; skipping frame"
                );
                continue;
            }

            if frame.len() < 12 {
                warn!(target: "sneldb::auth", "auth wal frame too small for nonce");
                continue;
            }
            let (nonce_bytes, ciphertext) = frame.split_at(12);
            let cipher = ChaCha20Poly1305::new(&self.key);
            match cipher.decrypt(Nonce::from_slice(nonce_bytes), ciphertext) {
                Ok(plaintext) => match bincode::deserialize::<AuthWalRecord>(&plaintext) {
                    Ok(record) => out.push(StoredUser {
                        user: record.user,
                        persisted_at: record.ts,
                    }),
                    Err(e) => {
                        warn!(target: "sneldb::auth", error = %e, "auth wal decode failed; skipping frame")
                    }
                },
                Err(e) => {
                    warn!(target: "sneldb::auth", error = %e, "auth wal decrypt failed; skipping frame")
                }
            };
        }

        Ok(out)
    }
}
