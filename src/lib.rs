//! Utilities for retrying falliable, asynchronous operations.

use std::fmt::Debug;
use std::time::Duration;

use tokio::time;
use tracing::warn;

/// Calls a fallible async function multiple times, with a given timeout.
///
/// If a `base_delay` is provided, the function is given an exponentially
/// increasing delay on each run, up until the maximum number of attempts.
///
/// Returns the first successful result if any, or the last error.
#[derive(Copy, Clone, Debug)]
pub struct Retry {
    /// Name of the operation being retried.
    pub name: &'static str,

    /// The number of attempts to make.
    pub attempts: u32,

    /// The base delay after the first attempt, if provided.
    pub base_delay: Duration,

    /// Exponential factor to increase the delay by on each attempt.
    pub delay_factor: f64,

    /// If true, the delay will be selected randomly from the range [delay/2, delay).
    pub enable_jitter: bool,
}

impl Retry {
    /// Construct a new [`Retry`] object with default parameters.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            attempts: 3,
            base_delay: Duration::ZERO,
            delay_factor: 1.0,
            enable_jitter: false,
        }
    }

    /// Set the number of attempts to make.
    pub const fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Set the base delay.
    pub const fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Set the exponential factor increasing delay.
    pub const fn delay_factor(mut self, delay_factor: f64) -> Self {
        self.delay_factor = delay_factor;
        self
    }

    /// Enable jitter.
    pub const fn jitter(mut self, enabled: bool) -> Self {
        self.enable_jitter = enabled;
        self
    }

    fn apply_jitter(&self, delay: Duration) -> Duration {
        if self.enable_jitter {
            // [0.5, 1.0)
            delay.mul_f64(0.5 + fastrand::f64() / 2.0)
        } else {
            delay
        }
    }

    /// Set a custom error handler, returning a [`RetryWith`] that can be
    /// [`run`](RetryWith::run).
    ///
    /// The handler receives the error (by value), the 1-indexed attempt
    /// number that just failed, and the total number of attempts configured.
    ///
    /// This should be the last builder method before calling `run`.
    pub fn on_error<F>(self, handler: F) -> RetryWith<F> {
        RetryWith {
            inner: self,
            on_error: handler,
        }
    }

    /// Run a falliable asynchronous function using this retry configuration.
    ///
    /// Failed attempts are logged at `warn` level via `tracing`. To customize
    /// error handling, use [`on_error`](Self::on_error) instead.
    ///
    /// Panics if the number of attempts is set to `0`, or the base delay is
    /// incorrectly set to a negative duration.
    pub async fn run<T, E: Debug>(self, func: impl AsyncFnMut() -> Result<T, E>) -> Result<T, E> {
        let name = self.name;
        self.on_error(|err: E, _attempt, _total| {
            warn!(?err, "failed retryable operation {}, retrying", name);
        })
        .run(func)
        .await
    }
}

/// A [`Retry`] paired with a custom error handler. Created by
/// [`Retry::on_error`].
pub struct RetryWith<F> {
    inner: Retry,
    on_error: F,
}

impl<F> RetryWith<F> {
    /// Run a falliable asynchronous function using this retry configuration.
    ///
    /// Unlike [`Retry::run`], this does not require `E: Debug` since error
    /// formatting is delegated to the custom error handler.
    ///
    /// Panics if the number of attempts is set to `0`, or the base delay is
    /// incorrectly set to a negative duration.
    pub async fn run<T, E>(self, mut func: impl AsyncFnMut() -> Result<T, E>) -> Result<T, E>
    where
        F: FnMut(E, u32, u32),
    {
        let Self {
            inner,
            mut on_error,
        } = self;
        assert!(inner.attempts > 0, "attempts must be greater than 0");
        assert!(
            inner.base_delay >= Duration::ZERO && inner.delay_factor >= 0.0,
            "retry delay cannot be negative"
        );
        let mut delay = inner.base_delay;
        for i in 0..inner.attempts {
            match func().await {
                Ok(value) => return Ok(value),
                Err(err) if i == inner.attempts - 1 => return Err(err),
                Err(err) => {
                    on_error(err, i + 1, inner.attempts);
                    time::sleep(inner.apply_jitter(delay)).await;
                    delay = delay.mul_f64(inner.delay_factor);
                }
            }
        }
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Retry;

    #[tokio::test]
    #[should_panic]
    async fn zero_retry_attempts() {
        let _ = Retry::new("test")
            .attempts(0)
            .run(async || Ok::<_, std::io::Error>(()))
            .await;
    }

    #[tokio::test]
    async fn successful_retry() {
        let mut count = 0;
        let task = Retry::new("test").run(async || {
            count += 1;
            Ok::<_, std::io::Error>(())
        });
        let result = task.await;
        assert_eq!(count, 1);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn failed_retry() {
        let mut count = 0;
        let retry = Retry::new("test");
        let task = retry.run(async || {
            count += 1;
            Err::<(), ()>(())
        });
        let result = task.await;
        assert_eq!(count, retry.attempts);
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_retry() {
        let start = Instant::now();

        let mut count = 0;
        // Will retry at 0s, 1s, 3s, 7s, 15s
        let task = Retry::new("test")
            .attempts(5)
            .base_delay(Duration::from_secs(1))
            .delay_factor(2.0)
            .run(async || {
                count += 1;
                println!("elapsed = {:?}", start.elapsed());
                if start.elapsed() < Duration::from_secs(5) {
                    Err::<(), ()>(())
                } else {
                    Ok(())
                }
            });
        let result = task.await;
        assert_eq!(count, 4);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn custom_error_handler() {
        let mut errors: Vec<(u32, u32)> = Vec::new();
        let mut count = 0;
        let result = Retry::new("test")
            .on_error(|_err: &str, attempt, total| {
                errors.push((attempt, total));
            })
            .run(async || {
                count += 1;
                if count < 3 {
                    Err::<(), &str>("boom")
                } else {
                    Ok(())
                }
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(errors, vec![(1, 3), (2, 3)]);
    }

    #[tokio::test]
    async fn silent_error_handler() {
        let mut count = 0;
        let result = Retry::new("silent")
            .on_error(|_: (), _, _| {})
            .run(async || {
                count += 1;
                if count < 3 { Err::<(), ()>(()) } else { Ok(()) }
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn error_handler_no_debug_bound() {
        struct OpaqueError;

        let mut count = 0;
        let result = Retry::new("opaque")
            .attempts(2)
            .on_error(|_: OpaqueError, _, _| {})
            .run(async || {
                count += 1;
                if count < 2 {
                    Err::<(), _>(OpaqueError)
                } else {
                    Ok(())
                }
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_retry_with_jitter() {
        let start = Instant::now();

        let mut count = 0;
        // Earliest possible retry is 0s, 50ms, 525ms, 5.525s
        let task = Retry::new("test_jitter")
            .attempts(4)
            .base_delay(Duration::from_millis(100))
            .delay_factor(10.0)
            .jitter(true)
            .run(async || {
                count += 1;
                println!("elapsed = {:?}", start.elapsed());
                if start.elapsed() < Duration::from_millis(500) {
                    Err::<(), ()>(())
                } else {
                    Ok(())
                }
            });
        let result = task.await;
        assert_eq!(count, 3);
        assert!(result.is_ok());
    }
}
