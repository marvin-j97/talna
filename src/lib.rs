//! A simple, embeddable time series database.
//!
//! It uses <https://github.com/fjall-rs/fjall> as its underlying storage engine.
//!
//! The tagging and querying mechanism is modelled after Datadog's metrics service (<https://www.datadoghq.com/blog/engineering/timeseries-indexing-at-scale/>).
//!
//! Data points are f32s by default, but can be switched to f64 using the `high_precision` feature flag.
//!
//! ## Basic usage
//!
//! ```
//! # let path = std::path::Path::new(".testy");
//! # if path.try_exists()? {
//! #   std::fs::remove_dir_all(path)?;
//! # }
//! #
//! use talna::{Database, Duration, MetricName, tagset, timestamp};
//!
//! let db = Database::builder().open(path)?;
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
//! let now = timestamp();
//!
//! let grouped_timeseries = db
//!   .avg(metric_name, /* group by tag */ "host")
//!   .filter("env:prod AND service:db")
//!   // use .start() and .end() to set the time bounds
//!   .start(now - Duration::months(1.0))
//!   // use .granularity() to set the granularity (bucket width in nanoseconds)
//!   .granularity(Duration::days(1.0))
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
#![warn(clippy::needless_lifetimes)]

mod agg;
mod db;
mod db_builder;
mod error;
mod granularity;
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

pub use agg::{Bucket, GroupedAggregation};
pub use db::Database;
pub use db_builder::Builder as DatabaseBuilder;
pub use error::{Error, Result};
pub use granularity::Granularity;
pub use metric_name::MetricName;
pub use time::timestamp;

/// A list of tags.
pub type TagSet<'a> = [(&'a str, &'a str)];

#[doc(hidden)]
pub use series_key::SeriesKey;

/// Nanosecond timestamp
pub type Timestamp = u128;

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
