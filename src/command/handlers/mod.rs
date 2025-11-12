pub mod auth;
pub mod compare;
pub mod define;
pub mod flush;
pub mod kway_merger;
pub mod query;
pub mod query_batch_stream;
pub mod remember;
pub mod replay;
pub mod rlte_coordinator;
pub mod row_comparator;
pub mod segment_discovery;
pub mod shard_command_builder;
pub mod show;
pub mod store;

#[cfg(test)]
mod auth_test;
#[cfg(test)]
mod define_tests;
#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod kway_merger_test;
#[cfg(test)]
mod query_batch_stream_test;
#[cfg(test)]
mod query_tests;
#[cfg(test)]
mod replay_tests;
#[cfg(test)]
mod rlte_coordinator_test;
#[cfg(test)]
mod row_comparator_test;
#[cfg(test)]
mod segment_discovery_test;
#[cfg(test)]
mod shard_command_builder_test;
#[cfg(test)]
mod remember_tests;
#[cfg(test)]
mod store_tests;
