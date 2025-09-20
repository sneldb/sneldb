use std::path::PathBuf;

use crate::engine::core::WriteJob;

pub struct ColumnPathResolver<'a> {
    write_jobs: &'a Vec<WriteJob>,
}

impl<'a> ColumnPathResolver<'a> {
    pub fn new(write_jobs: &'a Vec<WriteJob>) -> Self {
        Self { write_jobs }
    }

    pub fn column_path_for_key(&self, key: &(String, String)) -> PathBuf {
        for job in self.write_jobs {
            if &job.key == key {
                return job.path.clone();
            }
        }
        PathBuf::from(format!("{}_{}.col", key.0, key.1))
    }

    pub fn zfc_path_for_key(&self, key: &(String, String)) -> PathBuf {
        let col = self.column_path_for_key(key);
        let mut s = col.to_string_lossy().to_string();
        if s.ends_with(".col") {
            s.truncate(s.len() - 4);
        }
        s.push_str(".zfc");
        PathBuf::from(s)
    }
}
