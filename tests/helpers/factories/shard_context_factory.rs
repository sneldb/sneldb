use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::segment::range_allocator::RangeAllocator;
use crate::engine::core::{EventIdGenerator, FlushManager, MemTable, WalHandle};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::context::ShardContext;
use crate::shared::config::CONFIG;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// A test factory for creating ShardContext with default or customized parameters.
pub struct ShardContextFactory {
    id: usize,
    capacity: usize,
    base_dir: Option<PathBuf>,
    wal_dir: Option<PathBuf>,
}

impl ShardContextFactory {
    pub fn new() -> Self {
        Self {
            id: 0,
            capacity: CONFIG.engine.fill_factor * CONFIG.engine.event_per_zone,
            base_dir: None,
            wal_dir: None,
        }
    }

    pub fn with_id(mut self, id: usize) -> Self {
        self.id = id;
        self
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_base_dir(mut self, dir: PathBuf) -> Self {
        self.base_dir = Some(dir);
        self
    }

    pub fn with_wal_dir(mut self, dir: PathBuf) -> Self {
        self.wal_dir = Some(dir);
        self
    }

    /// Create a ShardContext with default or configured values.
    /// Returns the context and its backing tempdir (if used).
    pub fn create(self) -> ShardContext {
        let tempdir = tempfile::tempdir().expect("tempdir creation failed");
        let base_dir = self
            .base_dir
            .unwrap_or_else(|| tempdir.path().to_path_buf());
        let wal_dir = self.wal_dir.unwrap_or_else(|| tempdir.path().to_path_buf());

        let (flush_sender, _rx) = mpsc::channel::<(
            u64,
            MemTable,
            Arc<tokio::sync::RwLock<SchemaRegistry>>,
            Arc<tokio::sync::Mutex<MemTable>>,
            Option<tokio::sync::oneshot::Sender<Result<(), StoreError>>>,
        )>(8);

        let wal_handle = WalHandle::new(self.id, &wal_dir).expect("Failed to create WAL writer");
        let wal = Arc::new(
            wal_handle
                .spawn_wal_thread()
                .expect("Failed to spawn WAL thread"),
        );

        let segment_ids = Arc::new(RwLock::new(vec![]));
        let flush_coordination_lock = Arc::new(tokio::sync::Mutex::new(()));
        let flush_manager = FlushManager::new(
            self.id,
            base_dir.clone(),
            Arc::clone(&segment_ids),
            Arc::clone(&flush_coordination_lock),
        );

        // Seed allocator from existing ids (if any)
        let existing: Vec<String> = segment_ids.read().unwrap().clone();
        let allocator = RangeAllocator::from_existing_ids(existing.iter().map(|s| s.as_str()));
        let mut allocator_preview = allocator.clone();
        let next_l0_id = allocator_preview.next_for_level(0);

        let ctx = ShardContext {
            id: self.id,
            memtable: MemTable::new(self.capacity),
            passive_buffers: Arc::new(PassiveBufferSet::new(8)),
            flush_sender,
            segment_id: 0,
            next_l0_id,
            allocator,
            segment_ids,
            base_dir,
            wal_dir: wal_dir.clone(),
            events: Default::default(),
            flush_count: 0,
            wal: Some(wal),
            flush_manager,
            flush_coordination_lock,
            event_id_gen: EventIdGenerator::new(),
        };

        ctx
    }
}
