//! A simple, embeddable time series database
//!
//! ```
//! # let path = std::path::Path::new(".testy");
//! # if path.try_exists()? {
//! #   std::fs::remove_dir_all(path)?;
//! # }
//! #
//! # fn timestamp() -> u128 {
//! #     0
//! # }
//! #
//! use talna::{Database, tagset};
//!
//! let db = Database::new(path, /* 64 MiB cache */ 64)?;
//!
//! db.write(
//!     "cpu.total", // metric name
//!     timestamp(), // nanosecond timestamp (u128)
//!     25.42, // actual value (f64)
//!     tagset!(
//!         "env" => "prod",
//!         "service" => "db",
//!         "host" => "h-1",
//!     ),
//! )?;
//!
//! let buckets = db
//!   .avg(/* metric */ "cpu.total", /* group by tag */ "host")
//!   .filter("env:prod AND service:db")
//!   // use .start() and .end() to set the time bounds
//!   // use .bucket() to set the granularity (bucket width in nanoseconds)
//!   .run()?;
//!
//! println!("{buckets:#?}");
//!
//! # Ok::<(), talna::Error>(())
//! ```

mod agg;
mod db;
mod merge;

#[doc(hidden)]
pub mod query;

mod smap;
mod tag_index;
mod tag_sets;

type SeriesId = u64;
type HashMap<K, V> = std::collections::HashMap<K, V, rustc_hash::FxBuildHasher>;

pub use db::{Database, TagSet};

pub type Value = f64;

// TODO: custom error
pub type Error = fjall::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! tagset {
  ($($k:expr => $v:expr),* $(,)?) => {{
      &[$(($k.into(), $v.into()),)*]
  }}
}
