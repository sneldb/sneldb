use std::collections::HashSet;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::handlers::show::errors::ShowError;
use crate::engine::core::read::flow::BatchSchema;
use crate::shared::config::CONFIG;
use crate::shared::response::render::Renderer;

use serde_json::Value as JsonValue;

pub struct ShowResponseWriter<'a, W: AsyncWrite + Unpin> {
    writer: BufWriter<&'a mut W>,
    renderer: &'a dyn Renderer,
    column_metadata: Vec<(String, String)>,
    column_names: Vec<String>,
    batch_size: usize,
    materialized_frame_count: usize,
    seen_ids: Option<HashSet<u64>>,
    event_id_idx: Option<usize>,
    encode_buf: Vec<u8>,
    limit: Option<usize>,
    offset: Option<usize>,
    batch_count: usize,
    emitted: usize,
    skipped: usize,
}

impl<'a, W: AsyncWrite + Unpin> ShowResponseWriter<'a, W> {
    pub fn new(
        writer: &'a mut W,
        renderer: &'a dyn Renderer,
        schema: Arc<BatchSchema>,
        materialized_frame_count: usize,
        has_watermark_filtering: bool,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Self {
        let column_metadata: Vec<(String, String)> = schema
            .columns()
            .iter()
            .map(|column| (column.name.clone(), column.logical_type.clone()))
            .collect();

        let column_names: Vec<String> = column_metadata
            .iter()
            .map(|(name, _)| name.clone())
            .collect();

        let event_id_idx = column_names.iter().position(|name| name == "event_id");

        let batch_size = CONFIG
            .query
            .as_ref()
            .and_then(|cfg| cfg.streaming_batch_size)
            .unwrap_or(1000) as usize;

        let encode_capacity = if batch_size > 0 { 65536 } else { 4096 };

        let limit = limit.map(|value| value as usize);
        let offset = offset.map(|value| value as usize);

        let seen_ids = if has_watermark_filtering {
            None
        } else {
            let estimated_unique_ids = (limit.unwrap_or(10_000) / 100).max(100);
            Some(HashSet::with_capacity(estimated_unique_ids))
        };

        Self {
            writer: BufWriter::with_capacity(65536, writer),
            renderer,
            column_metadata,
            column_names,
            batch_size,
            materialized_frame_count,
            seen_ids,
            event_id_idx,
            encode_buf: Vec::with_capacity(encode_capacity),
            limit,
            offset,
            batch_count: 0,
            emitted: 0,
            skipped: 0,
        }
    }

    pub async fn write(mut self, mut stream: QueryBatchStream) -> Result<(), ShowError> {
        self.renderer
            .stream_schema(&self.column_metadata, &mut self.encode_buf);
        self.writer
            .write_all(&self.encode_buf)
            .await
            .map_err(|err| ShowError::new(err.to_string()))?;
        self.encode_buf.clear();

        let mut done = false;
        let column_names = self.column_names.clone();
        let column_name_refs: Vec<&str> = column_names.iter().map(|name| name.as_str()).collect();
        let column_count = column_names.len();

        while !done {
            match stream.recv().await {
                Some(batch_arc) => {
                    if batch_arc.is_empty() {
                        continue;
                    }

                    let column_views: Vec<&[JsonValue]> = batch_arc
                        .columns()
                        .map(|column| column as &[JsonValue])
                        .collect();

                    let mut valid_row_indices: Vec<usize> = Vec::new();
                    let is_materialized_frame = self.batch_count < self.materialized_frame_count;

                    for row_idx in 0..batch_arc.len() {
                        if let Some(ref mut seen_ids_set) = self.seen_ids {
                            if let Some(idx) = self.event_id_idx {
                                if let Some(id) = column_views[idx][row_idx].as_u64() {
                                    if is_materialized_frame {
                                        seen_ids_set.insert(id);
                                    } else if !seen_ids_set.insert(id) {
                                        continue;
                                    }
                                }
                            }
                        }

                        if let Some(offset) = self.offset {
                            if self.skipped < offset {
                                self.skipped += 1;
                                continue;
                            }
                        }

                        if let Some(limit) = self.limit {
                            if self.emitted >= limit {
                                done = true;
                                break;
                            }
                        }

                        valid_row_indices.push(row_idx);
                        self.emitted += 1;
                    }

                    self.batch_count += 1;

                    if valid_row_indices.is_empty() {
                        continue;
                    }

                    if self.batch_size > 0 {
                        let batch_rows: Vec<Vec<&JsonValue>> = valid_row_indices
                            .iter()
                            .map(|&row_idx| {
                                (0..column_count)
                                    .map(|col_idx| &column_views[col_idx][row_idx])
                                    .collect()
                            })
                            .collect();

                        self.renderer.stream_batch(
                            &column_name_refs,
                            &batch_rows,
                            &mut self.encode_buf,
                        );
                        self.writer
                            .write_all(&self.encode_buf)
                            .await
                            .map_err(|err| ShowError::new(err.to_string()))?;
                        self.encode_buf.clear();
                    } else {
                        for &row_idx in &valid_row_indices {
                            let row_refs: Vec<&JsonValue> = (0..column_count)
                                .map(|col_idx| &column_views[col_idx][row_idx])
                                .collect();
                            self.renderer.stream_row(
                                &column_name_refs,
                                &row_refs,
                                &mut self.encode_buf,
                            );
                            self.writer
                                .write_all(&self.encode_buf)
                                .await
                                .map_err(|err| ShowError::new(err.to_string()))?;
                            self.encode_buf.clear();
                        }
                    }
                }
                None => break,
            }
        }

        self.renderer.stream_end(self.emitted, &mut self.encode_buf);
        self.writer
            .write_all(&self.encode_buf)
            .await
            .map_err(|err| ShowError::new(err.to_string()))?;
        self.encode_buf.clear();
        self.writer
            .flush()
            .await
            .map_err(|err| ShowError::new(err.to_string()))?;

        Ok(())
    }
}
