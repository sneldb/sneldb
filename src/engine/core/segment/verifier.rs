use crate::engine::core::segment::segment_id::SegmentId;
use std::path::PathBuf;
use tracing::{debug, error, warn};

/// Verifies that flushed segments are queryable
pub struct SegmentVerifier {
    base_dir: PathBuf,
    shard_id: usize,
}

impl SegmentVerifier {
    pub fn new(base_dir: PathBuf, shard_id: usize) -> Self {
        Self { base_dir, shard_id }
    }

    /// Verify segment is queryable via filesystem checks
    pub async fn verify(&self, segment_id: u64) -> bool {
        debug!(
            target: "sneldb::segment_verifier",
            shard_id = self.shard_id,
            segment_id,
            "Verifying segment queryability"
        );

        let segment_dir = SegmentId::from(segment_id as u32).join_dir(&self.base_dir);

        if !self.check_segment_directory(&segment_dir, segment_id) {
            return false;
        }

        if !self.check_segment_index(&segment_dir, segment_id) {
            return false;
        }

        debug!(
            target: "sneldb::segment_verifier",
            shard_id = self.shard_id,
            segment_id,
            "Segment verified as queryable"
        );

        true
    }

    /// Verify with retry logic for filesystem sync delays
    pub async fn verify_with_retry(
        &self,
        segment_id: u64,
        max_attempts: u32,
        delay_ms: u64,
    ) -> bool {
        for attempt in 1..=max_attempts {
            if self.verify(segment_id).await {
                if attempt > 1 {
                    debug!(
                        target: "sneldb::segment_verifier",
                        segment_id,
                        attempt,
                        "Verification succeeded after retry"
                    );
                }
                return true;
            }

            if attempt < max_attempts {
                debug!(
                    target: "sneldb::segment_verifier",
                    segment_id,
                    attempt,
                    "Verification failed, retrying in {}ms",
                    delay_ms
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }

        error!(
            target: "sneldb::segment_verifier",
            segment_id,
            max_attempts,
            "Segment verification failed after all attempts"
        );
        false
    }

    fn check_segment_directory(&self, segment_dir: &PathBuf, segment_id: u64) -> bool {
        if !segment_dir.exists() {
            warn!(
                target: "sneldb::segment_verifier",
                segment_id,
                "Segment directory does not exist: {:?}",
                segment_dir
            );
            return false;
        }
        true
    }

    fn check_segment_index(&self, _segment_dir: &PathBuf, segment_id: u64) -> bool {
        // The segment index is at the shard level, not segment level
        let index_path = self.base_dir.join("segments.idx");
        if !index_path.exists() {
            warn!(
                target: "sneldb::segment_verifier",
                segment_id,
                "Shard segment index file does not exist"
            );
            return false;
        }

        // File exists - the actual loading and validation will be done by the query path
        true
    }
}
