use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::BatchSchema;
use crate::engine::types::ScalarValue;
use crate::shared::config::CONFIG;
use crate::shared::response::ArrowStreamEncoder;
use crate::shared::response::render::{Renderer, StreamingFormat};

pub struct QueryResponseWriter<'a, W: AsyncWrite + Unpin> {
    writer: BufWriter<&'a mut W>,
    renderer: &'a dyn Renderer,
    schema: Arc<BatchSchema>,
    column_metadata: Vec<(String, String)>,
    column_names: Vec<String>,
    batch_size: usize,
    encode_buf: Vec<u8>,
    seen_ids: HashSet<u64>,
    event_id_idx: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    emitted: usize,
    skipped: usize,
    limit_reached: bool,
}

impl<'a, W: AsyncWrite + Unpin> QueryResponseWriter<'a, W> {
    pub fn new(
        writer: &'a mut W,
        renderer: &'a dyn Renderer,
        schema: Arc<BatchSchema>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Self {
        let column_metadata: Vec<(String, String)> = schema
            .columns()
            .iter()
            .map(|column| (column.name.clone(), column.logical_type.clone()))
            .collect();

        let column_names: Vec<String> = schema
            .columns()
            .iter()
            .map(|column| column.name.clone())
            .collect();

        let event_id_idx = column_names.iter().position(|name| name == "event_id");

        let batch_size = CONFIG
            .query
            .as_ref()
            .and_then(|cfg| cfg.streaming_batch_size)
            .unwrap_or(1000) as usize;

        let encode_capacity = if batch_size > 0 { 65536 } else { 4096 };

        Self {
            writer: BufWriter::with_capacity(65536, writer),
            renderer,
            schema,
            column_metadata,
            column_names,
            batch_size,
            encode_buf: Vec::with_capacity(encode_capacity),
            seen_ids: HashSet::new(),
            event_id_idx,
            limit: limit.map(|value| value as usize),
            offset: offset.map(|value| value as usize),
            emitted: 0,
            skipped: 0,
            limit_reached: false,
        }
    }

    pub async fn write(mut self, stream: QueryBatchStream) -> io::Result<()> {
        match self.renderer.streaming_format() {
            StreamingFormat::Json => self.write_json(stream).await,
            StreamingFormat::Arrow => self.write_arrow(stream).await,
        }
    }

    async fn write_json(&mut self, mut stream: QueryBatchStream) -> io::Result<()> {
        self.renderer
            .stream_schema(&self.column_metadata, &mut self.encode_buf);
        self.writer.write_all(&self.encode_buf).await?;
        self.encode_buf.clear();

        let column_count = self.column_names.len();

        while !self.limit_reached {
            match stream.recv().await {
                Some(batch_arc) => {
                    if batch_arc.is_empty() {
                        continue;
                    }

                    let columns = batch_arc.columns_ref();
                    let mut valid_row_indices = Vec::new();

                    for row_idx in 0..batch_arc.len() {
                        let event_id = self.event_id_idx.and_then(|idx| {
                            columns[idx].get(row_idx).and_then(|value| value.as_u64())
                        });

                        if self.try_accept_row(event_id) {
                            valid_row_indices.push(row_idx);
                        }

                        if self.limit_reached {
                            break;
                        }
                    }

                    if valid_row_indices.is_empty() {
                        continue;
                    }

                    // Create column_refs_str after mutable borrows are done
                    let column_refs_str: Vec<&str> =
                        self.column_names.iter().map(|s| s.as_str()).collect();

                    if self.batch_size > 0 {
                        let batches: Vec<Vec<ScalarValue>> = valid_row_indices
                            .iter()
                            .map(|&row_idx| {
                                (0..column_count)
                                    .map(|col_idx| columns[col_idx][row_idx].clone())
                                    .collect()
                            })
                            .collect();

                        self.renderer.stream_batch(
                            &column_refs_str,
                            &batches,
                            &mut self.encode_buf,
                        );
                        self.writer.write_all(&self.encode_buf).await?;
                        self.encode_buf.clear();
                    } else {
                        for &row_idx in &valid_row_indices {
                            let row_values: Vec<ScalarValue> = (0..column_count)
                                .map(|col_idx| columns[col_idx][row_idx].clone())
                                .collect();
                            self.renderer.stream_row(
                                &column_refs_str,
                                &row_values,
                                &mut self.encode_buf,
                            );
                            self.writer.write_all(&self.encode_buf).await?;
                            self.encode_buf.clear();
                        }
                    }
                }
                None => break,
            }
        }

        self.renderer.stream_end(self.emitted, &mut self.encode_buf);
        self.writer.write_all(&self.encode_buf).await?;
        self.encode_buf.clear();
        self.writer.flush().await
    }

    async fn write_arrow(&mut self, mut stream: QueryBatchStream) -> io::Result<()> {
        let mut encoder = ArrowStreamEncoder::new(&self.schema).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to initialize Arrow encoder: {err}"),
            )
        })?;

        encoder.write_schema(&mut self.encode_buf).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to encode Arrow schema: {err}"),
            )
        })?;
        self.writer.write_all(&self.encode_buf).await?;
        self.encode_buf.clear();

        while !self.limit_reached {
            match stream.recv().await {
                Some(batch_arc) => {
                    if batch_arc.is_empty() {
                        continue;
                    }

                    let columns = batch_arc.columns_ref();
                    let mut valid_row_indices = Vec::new();

                    for row_idx in 0..batch_arc.len() {
                        let event_id = self.event_id_idx.and_then(|idx| {
                            columns[idx].get(row_idx).and_then(|value| value.as_u64())
                        });

                        if self.try_accept_row(event_id) {
                            valid_row_indices.push(row_idx);
                        }

                        if self.limit_reached {
                            break;
                        }
                    }

                    if valid_row_indices.is_empty() {
                        continue;
                    }

                    let row_indices_opt = if valid_row_indices.len() == batch_arc.len() {
                        let contiguous = valid_row_indices
                            .iter()
                            .enumerate()
                            .all(|(expected, &actual)| expected == actual);
                        if contiguous {
                            None
                        } else {
                            Some(valid_row_indices.as_slice())
                        }
                    } else {
                        Some(valid_row_indices.as_slice())
                    };

                    encoder
                        .write_batch(
                            &self.schema,
                            batch_arc.as_ref(),
                            row_indices_opt,
                            &mut self.encode_buf,
                        )
                        .map_err(|err| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("Failed to encode Arrow batch: {err}"),
                            )
                        })?;
                    self.writer.write_all(&self.encode_buf).await?;
                    self.encode_buf.clear();
                }
                None => break,
            }
        }

        encoder.write_end(&mut self.encode_buf).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to finalize Arrow stream: {err}"),
            )
        })?;
        self.writer.write_all(&self.encode_buf).await?;
        self.encode_buf.clear();
        self.writer.flush().await
    }

    fn try_accept_row(&mut self, event_id: Option<u64>) -> bool {
        if let Some(id) = event_id {
            if !self.seen_ids.insert(id) {
                return false;
            }
        }

        if let Some(offset) = self.offset {
            if self.skipped < offset {
                self.skipped += 1;
                return false;
            }
        }

        if let Some(limit) = self.limit {
            if self.emitted >= limit {
                self.limit_reached = true;
                return false;
            }
        }

        self.emitted += 1;
        true
    }
}
