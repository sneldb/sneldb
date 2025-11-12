mod response_writer;

#[cfg(test)]
mod response_writer_test;

use crate::shared::config::CONFIG;

pub use response_writer::QueryResponseWriter;

pub struct QueryStreamingConfig;

impl QueryStreamingConfig {
    pub fn enabled() -> bool {
        // Check for test override first (thread-local)
        #[cfg(test)]
        {
            if let Some(override_value) = TEST_STREAMING_ENABLED_OVERRIDE.get() {
                return override_value;
            }
        }

        // Fall back to config
        CONFIG
            .query
            .as_ref()
            .and_then(|cfg| cfg.streaming_enabled)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod test_helpers {
    use std::cell::Cell;

    thread_local! {
        /// Thread-local override for streaming_enabled in tests
        pub(super) static TEST_STREAMING_ENABLED_OVERRIDE: Cell<Option<bool>> = Cell::new(None);
    }

    /// Guard that restores the previous streaming_enabled value when dropped
    pub struct StreamingEnabledGuard {
        previous: Option<bool>,
    }

    impl Drop for StreamingEnabledGuard {
        fn drop(&mut self) {
            TEST_STREAMING_ENABLED_OVERRIDE.set(self.previous);
        }
    }

    /// Sets streaming_enabled override for the current test thread
    ///
    /// Returns a guard that will restore the previous value when dropped.
    /// This allows tests to set the override at the start and have it
    /// automatically cleaned up when the test completes.
    ///
    /// # Example
    /// ```no_run
    /// use crate::command::handlers::query::streaming::test_helpers::set_streaming_enabled;
    ///
    /// #[tokio::test]
    /// async fn my_test() {
    ///     let _guard = set_streaming_enabled(true);
    ///     // Test code here - streaming will be enabled
    ///     // Guard automatically restores previous value when test ends
    /// }
    /// ```
    pub fn set_streaming_enabled(enabled: bool) -> StreamingEnabledGuard {
        let previous = TEST_STREAMING_ENABLED_OVERRIDE.get();
        TEST_STREAMING_ENABLED_OVERRIDE.set(Some(enabled));
        StreamingEnabledGuard { previous }
    }
}

#[cfg(test)]
use test_helpers::TEST_STREAMING_ENABLED_OVERRIDE;

#[cfg(test)]
pub use test_helpers::set_streaming_enabled;
