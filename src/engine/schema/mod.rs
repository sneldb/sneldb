pub mod errors;
pub mod registry;
pub mod schema_store;
pub mod types;

pub use errors::SchemaError;
pub use registry::{MiniSchema, SchemaRegistry};
pub use types::{EnumType, FieldType};

#[cfg(test)]
mod registery_test;
#[cfg(test)]
mod schema_store_test;
