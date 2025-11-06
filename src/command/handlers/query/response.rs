use std::io;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::engine::core::read::result::QueryResult;
use crate::engine::types::ScalarValue;
use crate::shared::response::Response;
use crate::shared::response::render::Renderer;

pub struct QueryResponseFormatter<'a> {
    renderer: &'a dyn Renderer,
}

impl<'a> QueryResponseFormatter<'a> {
    pub fn new(renderer: &'a dyn Renderer) -> Self {
        Self { renderer }
    }

    pub async fn write<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        result: QueryResult,
    ) -> io::Result<()> {
        match result {
            QueryResult::Selection(selection) => {
                let table = selection.finalize();
                if table.rows.is_empty() {
                    let response = Response::ok_lines(vec!["No matching events found".to_string()]);
                    return writer.write_all(&self.renderer.render(&response)).await;
                }

                let response = self.build_table_response(table.columns, table.rows);
                writer.write_all(&self.renderer.render(&response)).await
            }
            QueryResult::Aggregation(aggregation) => {
                let table = aggregation.finalize();
                let response = self.build_table_response(table.columns, table.rows);
                writer.write_all(&self.renderer.render(&response)).await
            }
        }
    }

    fn build_table_response(
        &self,
        columns: Vec<crate::engine::core::read::result::ColumnSpec>,
        rows: Vec<Vec<ScalarValue>>,
    ) -> Response {
        let column_metadata = columns
            .into_iter()
            .map(|column| (column.name, column.logical_type))
            .collect::<Vec<(String, String)>>();

        let count = rows.len();
        Response::ok_table(column_metadata, rows, count)
    }
}
