# WAL Archiving Performance Analysis

## Current Implementation Analysis

### Where Archiving Happens

```rust
// src/engine/core/write/flush_worker.rs (line 87-88)
let cleaner = WalCleaner::new(self.shard_id);
cleaner.cleanup_up_to(segment_id + 1);  // ⚠️ SYNCHRONOUS in async context
```

The archiving happens in the flush worker's async task but uses synchronous I/O.

### Current Flow

```
Flush completes (async)
  ↓
Lock passive memtable (async)
  ↓
Clear memtable (sync)
  ↓
WalCleaner::cleanup_up_to (sync)  ← BLOCKS HERE
  ↓
  if conservative_mode:
    WalArchiver::archive_logs_up_to (sync)
      ↓
      For each WAL file (sequential):
        - Read file (blocking I/O)
        - Parse JSON lines (CPU)
        - Serialize MessagePack (CPU)
        - Compress with Zstd (CPU-intensive)
        - Write archive (blocking I/O)
        - Sync to disk (blocking I/O)
  ↓
Delete WAL files (sync)
```

## Performance Issues Identified

### 🔴 Issue 1: Blocking in Async Context

**Problem:**
- `cleanup_up_to()` is synchronous and called in async `FlushWorker::run()`
- Blocks the tokio executor thread during archiving
- Could affect other async tasks on the same runtime

**Impact:** Medium-High
- Blocks for: `file_read + compression + file_write` per WAL file
- For 5 WAL files @ 10MB each: ~100-500ms blocked time (level 3)
- Prevents the flush worker from processing next memtable

**Severity:** Moderate (flush worker is dedicated per shard, but still wasteful)

---

### 🟡 Issue 2: Sequential Archiving

**Problem:**
```rust
// wal_archiver.rs line 113-126
for entry in entries.flatten() {
    // Archive one file at a time
    results.push(self.archive_log(id));  // Sequential
}
```

**Impact:** Medium
- With 5 WAL files to archive: 5× the time
- CPU-bound compression could be parallelized
- I/O could overlap with compression

**Example:**
- Sequential: 5 files × 100ms = 500ms total
- Parallel: max(100ms) = ~100-150ms total (3-5× faster)

---

### 🟡 Issue 3: No Async I/O

**Problem:**
- All file operations use `std::fs` (blocking)
- Could use `tokio::fs` for async operations
- Could use `spawn_blocking` for CPU-intensive work

**Impact:** Low-Medium
- Blocks runtime thread
- Reduces concurrency potential

---

### 🟢 Issue 4: Memory Allocations

**Problem:**
- Each archive loads entire WAL file into memory
- Allocates Vec for entries, MessagePack buffer, compressed buffer
- Could use streaming for large files

**Impact:** Low
- Memory usage: ~3× WAL file size (JSON + MessagePack + compressed)
- For 10MB WAL: ~30MB peak memory per file
- Acceptable for most cases, but could be optimized

---

### 🟢 Issue 5: No Compression Dictionary

**Problem:**
- Zstd supports training compression dictionaries
- WAL files have repetitive structure (event_type, context_id patterns)
- Could improve compression ratio by 10-30%

**Impact:** Low
- Slightly larger archives
- Opportunity for future optimization

## Recommended Improvements

### Priority 1: Move to Async with spawn_blocking 🚀

**Change:**
```rust
// In flush_worker.rs
pub async fn cleanup_up_to_async(&self, keep_from_log_id: u64) {
    let shard_id = self.shard_id;
    let wal_dir = self.wal_dir.clone();
    
    tokio::task::spawn_blocking(move || {
        let cleaner = WalCleaner::with_wal_dir(shard_id, wal_dir);
        cleaner.cleanup_up_to(keep_from_log_id);
    }).await.unwrap();
}
```

**Benefits:**
- ✅ Doesn't block async runtime
- ✅ Archives run on dedicated blocking thread pool
- ✅ Minimal code changes
- ✅ ~10-20% better throughput under load

**Effort:** Low (1-2 hours)
**Impact:** Medium-High

---

### Priority 2: Parallel Archiving with Rayon 🚀

**Change:**
```rust
// In wal_archiver.rs
use rayon::prelude::*;

pub fn archive_logs_up_to_parallel(&self, keep_from_log_id: u64) 
    -> Vec<Result<PathBuf, std::io::Error>> 
{
    let files_to_archive: Vec<u64> = std::fs::read_dir(&self.wal_dir)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
                .and_then(|n| n.parse::<u64>().ok())
                .filter(|&id| id < keep_from_log_id)
        })
        .collect();
    
    // Archive in parallel using rayon
    files_to_archive
        .par_iter()
        .map(|&id| self.archive_log(id))
        .collect()
}
```

**Benefits:**
- ✅ 3-5× faster for multiple files
- ✅ Better CPU utilization (compression is CPU-bound)
- ✅ Already have rayon as dependency

**Effort:** Medium (2-3 hours)
**Impact:** High (for workloads with many small WAL files)

---

### Priority 3: Async File I/O with tokio::fs 🔄

**Change:**
```rust
// In wal_archive.rs
pub async fn from_wal_file_async(
    wal_path: &Path,
    shard_id: usize,
    log_id: u64,
    compression: String,
    compression_level: i32,
) -> std::io::Result<Self> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    
    let file = tokio::fs::File::open(wal_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut entries = Vec::new();
    
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() { continue; }
        if let Ok(entry) = serde_json::from_str::<WalEntry>(&line) {
            entries.push(entry);
        }
    }
    
    // Compress in spawn_blocking
    let archive = Self { header, body };
    Ok(archive)
}
```

**Benefits:**
- ✅ Non-blocking I/O
- ✅ Better concurrency
- ✅ Can overlap I/O with CPU work

**Effort:** Medium-High (4-6 hours, need to refactor interfaces)
**Impact:** Medium

---

### Priority 4: Streaming Compression 🔄

**Change:**
```rust
use zstd::stream::write::Encoder;

pub fn write_to_file_streaming(&self, archive_dir: &Path) 
    -> std::io::Result<PathBuf> 
{
    let archive_path = archive_dir.join(self.generate_filename());
    let file = File::create(&archive_path)?;
    
    // Stream compression instead of loading all into memory
    let mut encoder = Encoder::new(file, self.header.compression_level)?;
    
    rmp_serde::encode::write(&mut encoder, self)?;
    
    encoder.finish()?;
    Ok(archive_path)
}
```

**Benefits:**
- ✅ Lower memory footprint
- ✅ Faster for large WAL files
- ✅ No intermediate buffer allocation

**Effort:** Medium (3-4 hours)
**Impact:** Medium (especially for large WAL files)

---

### Priority 5: Compression Dictionary Training 📚

**Change:**
```rust
// Train once at startup or periodically
let dictionary = zstd::dict::from_samples(sample_wal_files, dict_size)?;

// Use for compression
let compressed = zstd::encode_all(&data, level, &dictionary)?;

// Use for decompression
let decompressed = zstd::decode_all(&compressed, &dictionary)?;
```

**Benefits:**
- ✅ 10-30% better compression ratio
- ✅ Faster compression with trained dictionary
- ✅ Smaller archive files

**Effort:** Medium-High (6-8 hours, needs dictionary management)
**Impact:** Medium (better compression, not faster archiving)

---

### Priority 6: Deferred Archiving Queue 🔄

**Change:**
```rust
// Instead of archiving immediately, queue for background task
pub struct ArchiveQueue {
    tx: mpsc::Sender<ArchiveJob>,
}

impl ArchiveQueue {
    pub fn enqueue(&self, shard_id: usize, log_id: u64) {
        self.tx.send(ArchiveJob { shard_id, log_id }).await;
    }
}

// Dedicated archiving worker
async fn archive_worker(rx: mpsc::Receiver<ArchiveJob>) {
    while let Some(job) = rx.recv().await {
        tokio::task::spawn_blocking(move || {
            let archiver = WalArchiver::new(job.shard_id);
            archiver.archive_log(job.log_id)
        }).await;
    }
}
```

**Benefits:**
- ✅ Completely non-blocking flush
- ✅ Archiving happens independently
- ✅ Can rate-limit archiving to avoid I/O storms

**Effort:** High (8-10 hours, needs queue management and failure handling)
**Impact:** High (flush is no longer blocked by archiving)

## Recommended Implementation Order

### Phase 1: Quick Wins (Recommended for immediate deployment) ⭐

1. **Add `spawn_blocking` wrapper** (2 hours)
   - Move archiving off async runtime
   - Simple, safe, immediate benefit
   - No interface changes

2. **Parallel archiving with rayon** (3 hours)
   - Use existing rayon dependency
   - 3-5× faster for multiple files
   - Easy to implement and test

**Total effort:** 5 hours
**Expected improvement:** 3-5× faster archiving, non-blocking runtime

### Phase 2: Advanced Optimizations (For future iteration)

3. **Async file I/O** (6 hours)
   - Requires interface changes
   - Better for large files
   - More complex testing

4. **Streaming compression** (4 hours)
   - Memory efficiency
   - Faster for large WAL files

5. **Archive queue** (10 hours)
   - Completely decouple from flush
   - Most complex but highest impact

**Total effort:** 20 hours
**Expected improvement:** Complete non-blocking, 5-10× better throughput

### Phase 3: Advanced Features (Optional)

6. **Compression dictionary** (8 hours)
   - Better compression ratio
   - Requires dictionary management
   - Marginal performance impact on archiving speed

## Benchmarking Plan

### Test Scenario
- 10 WAL files, 10MB each
- Conservative mode enabled
- Compression level 3

### Metrics to Measure
- Time to archive all files
- Flush worker blocking time
- CPU utilization during archiving
- Memory usage peak
- Throughput impact on concurrent writes

### Expected Results

| Implementation | Time (10 files) | Blocking? | Memory | Complexity |
|----------------|-----------------|-----------|--------|------------|
| Current | 500-1000ms | Yes | 300MB | Low |
| + spawn_blocking | 500-1000ms | No | 300MB | Low |
| + Parallel (rayon) | 150-300ms | No | 500MB | Low |
| + Async I/O | 100-200ms | No | 300MB | Medium |
| + Streaming | 100-200ms | No | 100MB | Medium |
| + Archive queue | 50-100ms* | No | 100MB | High |

*Perceived time; actual archiving happens in background

## Code Examples for Priority 1 & 2

### Example 1: spawn_blocking Integration

```rust
// src/engine/core/wal/wal_cleaner.rs
impl WalCleaner {
    pub async fn cleanup_up_to_async(&self, keep_from_log_id: u64) {
        let shard_id = self.shard_id;
        let wal_dir = self.wal_dir.clone();
        let conservative_mode = crate::shared::config::CONFIG.wal.conservative_mode;
        
        tokio::task::spawn_blocking(move || {
            if conservative_mode {
                let archiver = WalArchiver::new(shard_id);
                let results = archiver.archive_logs_up_to(keep_from_log_id);
                
                if results.iter().any(|r| r.is_err()) {
                    return; // Don't delete if archiving failed
                }
            }
            
            // Delete WAL files
            let cleaner = WalCleaner::with_wal_dir(shard_id, wal_dir);
            cleaner.delete_logs_up_to(keep_from_log_id);
        }).await.unwrap();
    }
}

// src/engine/core/write/flush_worker.rs
cleaner.cleanup_up_to_async(segment_id + 1).await;  // Non-blocking!
```

### Example 2: Parallel Archiving with Rayon

```rust
// src/engine/core/wal/wal_archiver.rs
use rayon::prelude::*;

impl WalArchiver {
    pub fn archive_logs_up_to_parallel(
        &self,
        keep_from_log_id: u64,
    ) -> Vec<Result<PathBuf, std::io::Error>> {
        // Collect file IDs first
        let log_ids: Vec<u64> = std::fs::read_dir(&self.wal_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        name.strip_prefix("wal-")
                            .and_then(|s| s.strip_suffix(".log"))
                            .and_then(|n| n.parse::<u64>().ok())
                            .filter(|&id| id < keep_from_log_id)
                    })
                    .collect()
            })
            .unwrap_or_default();
        
        info!(
            target: "wal_archiver::archive_logs_up_to_parallel",
            shard_id = self.shard_id,
            file_count = log_ids.len(),
            "Starting parallel archive"
        );
        
        // Archive in parallel
        let results: Vec<_> = log_ids
            .par_iter()  // ← Rayon parallel iterator
            .map(|&id| {
                info!(
                    target: "wal_archiver::parallel",
                    shard_id = self.shard_id,
                    log_id = id,
                    "Archiving in parallel"
                );
                self.archive_log(id)
            })
            .collect();
        
        let success = results.iter().filter(|r| r.is_ok()).count();
        info!(
            target: "wal_archiver::archive_logs_up_to_parallel",
            shard_id = self.shard_id,
            success_count = success,
            total = results.len(),
            "Parallel archive complete"
        );
        
        results
    }
}
```

## Performance Estimates

### Current Implementation
```
Scenario: 5 WAL files, 10MB each
- Read: 5 × 20ms = 100ms
- Compress: 5 × 80ms = 400ms (level 3, ~500 MB/s)
- Write: 5 × 10ms = 50ms
Total: ~550ms (blocks flush worker)
```

### With spawn_blocking
```
Same 550ms, but doesn't block async runtime
Flush worker can process next memtable immediately
Improvement: Unblocks runtime, ~20% better throughput
```

### With spawn_blocking + Parallel
```
Scenario: 5 WAL files, 10MB each
- Read: max(20ms) = ~20ms (parallel)
- Compress: max(80ms) = ~80ms (parallel, CPU-bound)
- Write: max(10ms) = ~10ms (parallel)
Total: ~150ms (doesn't block runtime)
Improvement: 3.6× faster, non-blocking
```

### With Archive Queue (Future)
```
Archiving happens completely in background
Flush worker perceives: 0ms
Actual archiving: Same ~150ms but invisible to flush path
Improvement: Flush is completely unblocked
```

## Memory Impact Analysis

### Current
- Peak: ~30MB per WAL file (sequential)
- Total: ~30MB (one at a time)

### Parallel (Rayon)
- Peak: ~30MB × 5 files = ~150MB (parallel)
- Total: ~150MB (simultaneous)

### Mitigation
- Limit parallelism: `rayon::ThreadPoolBuilder::new().num_threads(3)`
- Balance: 3 files parallel = ~90MB, still 2-3× faster

## Recommended Action Plan

### Immediate (Include in Current PR) ✅

**Add spawn_blocking wrapper:**
```rust
// Minimal change to flush_worker.rs
let cleaner = WalCleaner::new(self.shard_id);
let segment_id_copy = segment_id;
tokio::task::spawn_blocking(move || {
    cleaner.cleanup_up_to(segment_id_copy + 1);
}).await.unwrap();
```

**Effort:** 30 minutes
**Risk:** Very low
**Benefit:** Non-blocking runtime

---

### Short-term (Next PR) 📋

**Implement parallel archiving:**
- Add `archive_logs_up_to_parallel()` method
- Use rayon for CPU parallelism
- Add config option: `archive_parallelism = 3`
- Add benchmarks

**Effort:** 4-6 hours
**Risk:** Low
**Benefit:** 3-5× faster archiving

---

### Long-term (Future Enhancement) 🔮

1. **Archive queue** - Completely decouple from flush
2. **Async I/O** - Full tokio::fs integration
3. **Streaming** - Memory-efficient for large files
4. **Dictionary** - Better compression ratio

## Configuration Additions (Proposed)

```toml
[wal]
# ... existing settings ...
conservative_mode = true
archive_dir = "../data/wal/archived/"
compression_level = 3
compression_algorithm = "zstd"

# NEW: Performance tuning
archive_parallelism = 3           # Parallel archive workers (0 = sequential)
archive_async = true              # Use spawn_blocking (recommended)
archive_queue_size = 100          # Deferred archiving queue (0 = synchronous)
```

## Conclusion

### Current Status
✅ **Functionally correct** - Archives work, tests pass, data is safe
⚠️ **Performance opportunity** - Synchronous blocking in async context

### Recommended Immediate Action
✅ **Add spawn_blocking** - 30 minutes, low risk, immediate benefit

### Recommended Short-term
📋 **Add parallel archiving** - 4-6 hours, 3-5× faster

### Long-term Vision
🔮 **Archive queue** - Complete decoupling, zero flush impact

**Bottom Line:** Current implementation is production-ready but has 3-5× performance headroom available with straightforward improvements.

