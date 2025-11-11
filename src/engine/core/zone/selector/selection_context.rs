use std::path::PathBuf;

use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::core::{QueryCaches, QueryPlan};

pub struct SelectionContext<'a> {
    pub plan: &'a FilterGroup,
    pub query_plan: &'a QueryPlan,
    pub base_dir: &'a PathBuf,
    pub caches: Option<&'a QueryCaches>,
}
