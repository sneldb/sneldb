mod batch_codec;
mod bitmap;
mod compression;
mod decoder;
mod encoder;
mod schema;
mod types;
mod utils;
mod value_codec;

#[cfg(test)]
mod batch_codec_test;
#[cfg(test)]
mod bitmap_test;
#[cfg(test)]
mod compression_test;
#[cfg(test)]
mod decoder_test;
#[cfg(test)]
mod encoder_test;
#[cfg(test)]
mod schema_test;
#[cfg(test)]
mod utils_test;
#[cfg(test)]
mod value_codec_test;

pub use batch_codec::{BatchCodec, Lz4BatchCodec};
pub use schema::{batch_schema_to_snapshots, schema_hash, schema_to_batch_schema};
pub use types::EncodedFrame;
