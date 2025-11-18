pub mod aggregate_stream;
mod sequence_stream;
mod stream_merger;
mod streaming;

#[cfg(test)]
mod aggregate_stream_test;
#[cfg(test)]
mod sequence_stream_test;
#[cfg(test)]
mod stream_merger_test;
#[cfg(test)]
mod streaming_test;

pub use sequence_stream::SequenceStreamMerger;
pub use stream_merger::StreamMergerKind;
