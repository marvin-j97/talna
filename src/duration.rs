/// Helpers for calculating durations
pub struct Duration;

impl Duration {
    /// Formats N years as nanosecond time frame.
    #[must_use]
    pub const fn years(n: f64) -> u128 {
        Self::months(n) * 12
    }

    /// Formats N months as nanosecond time frame.
    #[must_use]
    pub const fn months(n: f64) -> u128 {
        Self::weeks(n) * 4
    }

    /// Formats N weeks as nanosecond time frame.
    #[must_use]
    pub const fn weeks(n: f64) -> u128 {
        Self::days(n) * 7
    }

    /// Formats N days as nanosecond time frame.
    #[must_use]
    pub const fn days(n: f64) -> u128 {
        Self::hours(n) * 24
    }

    /// Formats N hours as nanosecond time frame.
    #[must_use]
    pub const fn hours(n: f64) -> u128 {
        Self::minutes(n) * 60
    }

    /// Formats N minutes as nanosecond time frame.
    #[must_use]
    pub const fn minutes(n: f64) -> u128 {
        Self::seconds(n) * 60
    }

    /// Formats N seconds as nanosecond time frame.
    #[must_use]
    pub const fn seconds(n: f64) -> u128 {
        Self::millis(n) * 1_000
    }

    /// Formats N milliseconds as nanosecond time frame.
    #[must_use]
    pub const fn millis(n: f64) -> u128 {
        Self::micros(n) * 1_000
    }

    /// Formats N microseconds as nanosecond time frame.
    #[must_use]
    pub const fn micros(n: f64) -> u128 {
        Self::nanos(n) * 1_000
    }

    /// Formats N nanoseconds as nanosecond time frame.
    #[must_use]
    pub const fn nanos(n: f64) -> u128 {
        n as u128
    }
}
