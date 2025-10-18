use std::fs::{File, create_dir_all};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use crate::shared::storage_header::{BinaryHeader, FileKind};

use super::compressed_column_index_factory::CompressedColumnIndexFactory;

pub struct ColumnFactory {
    segment_dir: PathBuf,
    uid: String,
    field: String,
    zfc_factory: CompressedColumnIndexFactory,
}

impl ColumnFactory {
    pub fn new() -> Self {
        Self {
            segment_dir: PathBuf::from("/tmp/segments/00000"),
            uid: "uid_test".to_string(),
            field: "field_a".to_string(),
            zfc_factory: CompressedColumnIndexFactory::new(),
        }
    }

    pub fn with_segment_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.segment_dir = dir.into();
        self
    }

    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = uid.to_string();
        self
    }

    pub fn with_field(mut self, field: &str) -> Self {
        self.field = field.to_string();
        self
    }

    pub fn with_zfc_entry(
        mut self,
        zone_id: u32,
        block_start: u64,
        comp_len: u32,
        uncomp_len: u32,
        num_rows: u32,
        in_block_offsets: Vec<u32>,
    ) -> Self {
        self.zfc_factory = self.zfc_factory.with_zone_entry(
            zone_id,
            block_start,
            comp_len,
            uncomp_len,
            num_rows,
            in_block_offsets,
        );
        self
    }

    pub fn write_minimal(self) -> (PathBuf, PathBuf) {
        self.write_with_col_kind(FileKind::SegmentColumn)
    }

    pub fn write_with_col_kind(self, kind: FileKind) -> (PathBuf, PathBuf) {
        create_dir_all(&self.segment_dir).expect("create segment dir");
        let zfc_path = self
            .segment_dir
            .join(format!("{}_{}.zfc", self.uid, self.field));
        let col_path = self
            .segment_dir
            .join(format!("{}_{}.col", self.uid, self.field));

        let idx = self.zfc_factory.create();
        idx.write_to_path(&zfc_path).expect("write zfc");

        write_col_with_header(&col_path, kind);

        (zfc_path, col_path)
    }
}

fn write_col_with_header(path: &Path, kind: FileKind) {
    let f = File::create(path).expect("create col");
    let mut w = BufWriter::new(f);
    BinaryHeader::new(kind.magic(), 1, 0)
        .write_to(&mut w)
        .expect("write header");
}
