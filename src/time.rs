use crate::Timestamp;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current timestamp in nanoseconds.
#[must_use]
pub fn timestamp() -> Timestamp {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_nanos()
}
