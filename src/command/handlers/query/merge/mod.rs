mod batch_merger;
mod ordered;
mod stream_merger;
mod streaming;
mod unordered;

#[cfg(test)]
mod batch_merger_test;
#[cfg(test)]
mod ordered_test;
#[cfg(test)]
mod stream_merger_test;
#[cfg(test)]
mod streaming_test;
#[cfg(test)]
mod unordered_test;

pub use batch_merger::BatchMerger;
pub use stream_merger::StreamMergerKind;
