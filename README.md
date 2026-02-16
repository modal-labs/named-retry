# named-retry

This is a simple, `impl Copy` utility for retrying fallible asynchronous operations, with helpful log messages through `tracing`.

```rust
use std::time::Duration;
use named_retry::Retry;

let retry = Retry::new("test")
    .attempts(5)
    .base_delay(Duration::from_secs(1))
    .delay_factor(2.0)
    .jitter(true);

let result = retry.run(async || { Ok::<_, ()>("done!") }).await;
assert_eq!(result, Ok("done!"));
```

By default, failed attempts are logged at `warn` level via `tracing`. Use `on_error` to customize error handling, for example when polling where some errors are expected:

```rust
use tracing::debug;

let result = Retry::new("poll")
    .attempts(10)
    .base_delay(Duration::from_secs(1))
    .on_error(|err, _attempt, _total| {
        debug!(?err, "not ready yet");
    })
    .run(async || { Ok::<_, ()>("done!") })
    .await;
```
