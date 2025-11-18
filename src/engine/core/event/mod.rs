use crate::engine::types::ScalarValue;
use indexmap::IndexMap;

pub type PayloadMap = IndexMap<String, ScalarValue>;

pub mod event;
pub mod event_builder;
pub mod event_id;

#[cfg(test)]
mod event_builder_tests;
#[cfg(test)]
mod event_id_tests;
#[cfg(test)]
mod event_tests;
