/// Helpers for calculating durations
///
/// ```
/// # let path = std::path::Path::new(".testy2");
/// # if path.try_exists()? {
/// #   std::fs::remove_dir_all(path)?;
/// # }
/// #
/// # use talna::{Database, Duration, MetricName, tagset, timestamp};
/// #
/// # let db = Database::builder().open(path)?;
/// #
/// let metric_name = MetricName::try_from("cpu.total").unwrap();
///
/// db.write(
///     metric_name,
///     25.42,
///     tagset!(
///         "env" => "prod",
///         "service" => "db",
///         "host" => "h-1",
///     ),
/// )?;
///
/// db.write(
///     metric_name,
///     42.42,
///     tagset!(
///         "env" => "prod",
///         "service" => "db",
///         "host" => "h-2",
///     ),
/// )?;
///
/// let now = timestamp();
///
/// let grouped_timeseries = db
///   .avg(metric_name, /* group by tag */ "host")
///   .filter("env:prod AND service:db")
///   .start(now - Duration::minutes(15.0))
///   .granularity(Duration::minutes(1.0))
///   .build()?
///   .collect()?;
///
/// println!("{grouped_timeseries:#?}");
///
/// # Ok::<(), talna::Error>(())
/// ```
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
