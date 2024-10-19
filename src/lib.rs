//! A simple, embeddable time series database.
//!
//! It uses <https://github.com/fjall-rs/fjall> as its underlying storage engine,
//! being able to ingest ~700k data points per seconds.
//!
//! The LSM-based storage engine causes no degradation in write ingestion speed, even for large datasets,
//! has low write amplification (good for SSDs) and compresses the on-disk data (again, good for SSDs).
//!
//! The tagging and querying mechanism is modelled after Datadog's metrics service (<https://www.datadoghq.com/blog/engineering/timeseries-indexing-at-scale/>).
//!
//! Data points are f32s by default, but can be switched to f64 using the `high_precision` feature flag.
//!
//! 1 billion data points (default config, jemalloc, i9 11900k):
//!
//! - ingested in 1374s (~727k inserts per second)
//! - average memory usage: 100 MB , peak: ~170 MB
//! - query latency for 1 million data points (`AVG | env:prod AND service:db AND (host:h-1 OR host:h-2 OR host:h-3)`): 110ms
//! - disk space: 12 GB
//!
//! ```
//! # let path = std::path::Path::new(".testy");
//! # if path.try_exists()? {
//! #   std::fs::remove_dir_all(path)?;
//! # }
//! #
//! use talna::{Database, MetricName, tagset};
//!
//! let db = Database::new(path, /* cache size in MiB */ 64)?;
//!
//! let metric_name = MetricName::try_from("cpu.total").unwrap();
//!
//! db.write(
//!     metric_name,
//!     25.42, // actual value (float)
//!     tagset!(
//!         "env" => "prod",
//!         "service" => "db",
//!         "host" => "h-1",
//!     ),
//! )?;
//!
//! db.write(
//!     metric_name,
//!     42.42, // actual value (float)
//!     tagset!(
//!         "env" => "prod",
//!         "service" => "db",
//!         "host" => "h-2",
//!     ),
//! )?;
//!
//! let grouped_timeseries = db
//!   .avg(metric_name, /* group by tag */ "host")
//!   .filter("env:prod AND service:db")
//!   // use .start() and .end() to set the time bounds
//!   // use .granularity() to set the granularity (bucket width in nanoseconds)
//!   .build()?
//!   .collect()?;
//!
//! println!("{grouped_timeseries:#?}");
//!
//! # Ok::<(), talna::Error>(())
//! ```

// #![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/logo.png")]
// #![doc(html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/logo.png")]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![warn(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]
#![warn(clippy::multiple_crate_versions)]
#![warn(clippy::result_unit_err)]

mod agg;
mod db;
mod duration;
mod error;
mod merge;
mod metric_name;

#[doc(hidden)]
pub mod query;

mod series_key;
mod smap;
mod tag_index;
mod tag_sets;
mod time;

type SeriesId = u64;
type HashMap<K, V> = std::collections::HashMap<K, V, rustc_hash::FxBuildHasher>;

pub use db::Database;
pub use duration::Duration;
pub use error::{Error, Result};
pub use metric_name::MetricName;
pub use time::timestamp;

/// A list of tags.
pub type TagSet<'a> = [(&'a str, &'a str)];

#[doc(hidden)]
pub use series_key::SeriesKey;

/// Value used in time series
#[cfg(feature = "high_precision")]
pub type Value = f64;

/// Value used in time series
#[cfg(not(feature = "high_precision"))]
pub type Value = f32;

/// Macro to create a list of tags.
///
/// # Examples
///
/// ```
/// use talna::{tagset, TagSet};
///
/// let tags: &TagSet = tagset!(
///   "service" => "db",
///   "env" => "production",
/// );
/// ```
#[macro_export]
macro_rules! tagset {
  ($($k:expr => $v:expr),* $(,)?) => {{
      &[$(($k.into(), $v.into()),)*]
  }}
}
