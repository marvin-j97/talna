/// Helpers for calculating durations
pub struct Duration;

impl Duration {
    /// Formats N years as nanosecond time frame.
    #[must_use]
    pub const fn years(n: usize) -> u128 {
        Self::months(n) * 12
    }

    /// Formats N months as nanosecond time frame.
    #[must_use]
    pub const fn months(n: usize) -> u128 {
        Self::weeks(n) * 4
    }

    /// Formats N weeks as nanosecond time frame.
    #[must_use]
    pub const fn weeks(n: usize) -> u128 {
        Self::days(n) * 7
    }

    /// Formats N days as nanosecond time frame.
    #[must_use]
    pub const fn days(n: usize) -> u128 {
        Self::hours(n) * 24
    }

    /// Formats N hours as nanosecond time frame.
    #[must_use]
    pub const fn hours(n: usize) -> u128 {
        Self::minutes(n) * 60
    }

    /// Formats N minutes as nanosecond time frame.
    #[must_use]
    pub const fn minutes(n: usize) -> u128 {
        Self::seconds(n) * 60
    }

    /// Formats N seconds as nanosecond time frame.
    #[must_use]
    pub const fn seconds(n: usize) -> u128 {
        (n as u128) * 1_000_000_000
    }
}
