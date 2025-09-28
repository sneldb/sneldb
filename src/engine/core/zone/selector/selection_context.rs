use std::path::PathBuf;

use crate::engine::core::{FilterPlan, QueryCaches, QueryPlan};

pub struct SelectionContext<'a> {
    pub plan: &'a FilterPlan,
    pub query_plan: &'a QueryPlan,
    pub base_dir: &'a PathBuf,
    pub caches: Option<&'a QueryCaches>,
}
