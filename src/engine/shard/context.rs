use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::segment::range_allocator::RangeAllocator;
use crate::engine::core::{
    Event, EventId, EventIdGenerator, FlushManager, MemTable, SegmentIdLoader, WalHandle,
    WalRecovery,
};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use crate::shared::config::CONFIG;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, oneshot};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct ShardContext {
    pub id: usize,

    // LSM ingestion
    pub memtable: MemTable,
    pub passive_buffers: Arc<PassiveBufferSet>,
    pub flush_sender: Sender<(
        u64,
        MemTable,
        Arc<tokio::sync::RwLock<SchemaRegistry>>,
        Arc<Mutex<MemTable>>,
        Option<oneshot::Sender<Result<(), StoreError>>>,
    )>,
    pub segment_id: u64,
    pub next_l0_id: u32,
    pub allocator: RangeAllocator,
    pub segment_ids: Arc<RwLock<Vec<String>>>,
    pub base_dir: PathBuf,
    pub wal_dir: PathBuf,

    // Query / replay cache
    pub events: BTreeMap<String, Vec<Event>>,

    // WAL and flushing
    pub flush_count: usize,
    pub wal: Option<Arc<WalHandle>>,
    pub flush_manager: FlushManager,

    // Flush coordination - prevents concurrent segment index updates
    pub flush_coordination_lock: Arc<Mutex<()>>,

    pub event_id_gen: EventIdGenerator,
}

impl ShardContext {
    pub fn new(
        id: usize,
        flush_sender: Sender<(
            u64,
            MemTable,
            Arc<tokio::sync::RwLock<SchemaRegistry>>,
            Arc<Mutex<MemTable>>,
            Option<oneshot::Sender<Result<(), StoreError>>>,
        )>,
        base_dir: PathBuf,
        wal_dir: PathBuf,
    ) -> Self {
        // Step 1: Initialize WAL
        info!(target: "shard::context", shard_id = id, "Creating WAL handle");
        let wal_handle = WalHandle::new(id, &wal_dir).expect("Failed to create WAL writer");
        let wal = Arc::new(
            wal_handle
                .spawn_wal_thread()
                .expect("Failed to spawn WAL thread"),
        );

        // Step 2: Load existing segment IDs
        let segment_id_loader = SegmentIdLoader::new(base_dir.clone());
        let segment_ids = Arc::new(RwLock::new(segment_id_loader.load()));
        let segment_id = SegmentIdLoader::next_id(&segment_ids);
        let existing: Vec<String> = segment_ids.read().unwrap().clone();
        let allocator = RangeAllocator::from_existing_ids(existing.iter().map(|s| s.as_str()));
        let mut allocator_preview = allocator.clone();
        let next_l0_id = allocator_preview.next_for_level(0);

        // Step 3: Initialize core components
        let flush_coordination_lock = Arc::new(Mutex::new(()));
        let flush_manager = FlushManager::new(
            id,
            base_dir.clone(),
            Arc::clone(&segment_ids),
            Arc::clone(&flush_coordination_lock),
        );

        let capacity = CONFIG.engine.fill_factor * CONFIG.engine.event_per_zone;

        let mut ctx = Self {
            id,
            memtable: MemTable::new(capacity),
            passive_buffers: Arc::new(PassiveBufferSet::new(
                CONFIG.engine.max_inflight_passives.unwrap_or(8),
            )),
            flush_sender,
            segment_id,
            next_l0_id,
            allocator,
            segment_ids,
            base_dir,
            wal_dir: wal_dir.clone(),
            events: BTreeMap::new(),
            flush_count: 0,
            wal: Some(wal),
            flush_manager,
            flush_coordination_lock,
            event_id_gen: EventIdGenerator::new(),
        };

        // Step 4: Recover MemTable from WAL
        let wal_recovery = WalRecovery::new(id, &wal_dir);
        if let Err(err) = wal_recovery.recover(&mut ctx) {
            warn!(target: "shard::context", shard_id = id, "Failed to recover from WAL: {:?}", err);
        } else {
            info!(target: "shard::context", shard_id = id, "WAL recovery completed");
        }

        ctx
    }

    pub fn next_event_id(&mut self) -> EventId {
        self.event_id_gen.next(self.id as u16)
    }
}
