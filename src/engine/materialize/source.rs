use std::sync::Arc;

use async_trait::async_trait;

use crate::engine::core::read::flow::{BatchSender, FlowContext, FlowOperatorError, FlowSource};

use super::MaterializationError;
use super::StoredFrameMeta;
use super::store::MaterializedStore;

pub struct MaterializedSource {
    store: MaterializedStore,
    frames: Vec<StoredFrameMeta>,
}

impl MaterializedSource {
    pub fn new(store: MaterializedStore) -> Self {
        let frames = store.frames().to_vec();
        Self { store, frames }
    }

    pub fn frames(&self) -> &[StoredFrameMeta] {
        &self.frames
    }

    pub fn into_store(self) -> MaterializedStore {
        self.store
    }
}

#[async_trait]
impl FlowSource for MaterializedSource {
    async fn run(
        mut self,
        output: BatchSender,
        _ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        for meta in self.frames.into_iter() {
            let batch = self.store.read_frame(&meta).map_err(materialize_err)?;
            output
                .send(batch)
                .await
                .map_err(|_| FlowOperatorError::ChannelClosed)?;
        }
        Ok(())
    }
}

fn materialize_err(err: MaterializationError) -> FlowOperatorError {
    FlowOperatorError::operator(format!("materialized source error: {err}"))
}
