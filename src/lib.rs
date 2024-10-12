mod agg;
mod db;
mod merge;
mod query;
// mod reader;
mod smap;
mod tag_index;
mod tag_sets;

pub use db::Database;
pub use db::TagSet;
pub use std::{path::Path, time::Instant};

type SeriesId = u64;
pub type Value = f64;

#[macro_export]
macro_rules! tagset {
  ($($k:expr => $v:expr),* $(,)?) => {{
      &[$(($k.into(), $v.into()),)*]
  }}
}
