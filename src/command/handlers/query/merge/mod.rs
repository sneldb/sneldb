mod aggregate_stream;
mod batch_merger;
mod ordered;
mod sequence_stream;
mod stream_merger;
mod streaming;
mod unordered;

#[cfg(test)]
mod aggregate_stream_test;
#[cfg(test)]
mod batch_merger_test;
#[cfg(test)]
mod ordered_test;
#[cfg(test)]
mod sequence_stream_test;
#[cfg(test)]
mod stream_merger_test;
#[cfg(test)]
mod streaming_test;
#[cfg(test)]
mod unordered_test;

pub use batch_merger::BatchMerger;
pub use sequence_stream::SequenceStreamMerger;
pub use stream_merger::StreamMergerKind;
