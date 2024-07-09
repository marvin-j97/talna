mod db;
mod merge;
mod query;
mod reader;
mod smap;
mod tag_index;

use db::Database;
use query::parse_filter_query;
use std::{ops::Bound, path::Path, time::Instant};

type SeriesId = u64;

macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        core::convert::From::from([$(($k.into(), $v.into()),)*])
    }}
}

fn main() -> fjall::Result<()> {
    env_logger::builder()
        .filter_module("fjall", log::LevelFilter::Info)
        .filter_module("talna", log::LevelFilter::Trace)
        .parse_default_env()
        .init();

    let path = Path::new(".testy");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let db = Database::new(path)?;

    let metric_name = "cpu.total";

    for host in ["h-1", "h-2", "h-3", "i-187"] {
        for idx in 0..1_000 {
            db.write(
                metric_name,
                idx,
                2.0,
                &map! {
                    "env" => "prod",
                    "service" => "db",
                    "host" => host,
                },
            )?;
        }
    }

    db.write(
        metric_name,
        0,
        20.0,
        &map! {
            "env" => "dev",
            "service" => "db",
            "host" => "h-1",
        },
    )?;
    db.write(
        metric_name,
        0,
        20.0,
        &map! {
            "env" => "dev",
            "service" => "db",
            "host" => "h-2",
        },
    )?;
    db.write(
        metric_name,
        0,
        20.0,
        &map! {
            "env" => "dev",
            "service" => "db",
            "host" => "h-3",
        },
    )?;

    let query_string = "service:db AND host:i-187";
    let filter = parse_filter_query(query_string).unwrap();

    let start = Instant::now();
    db.query(
        metric_name,
        &filter,
        (Bound::Included(500), Bound::Unbounded),
    )?;
    log::info!("done in {:?}", start.elapsed());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn create_series_key() {
        assert_eq!(
            "cpu.total#service:web",
            Database::create_series_key(
                "cpu.total",
                &map! {
                    "service" => "web",
                }
            )
        );
    }

    #[test_log::test]
    fn create_series_key_2() {
        assert_eq!(
            "cpu.total#host:i-187;service:web",
            Database::create_series_key(
                "cpu.total",
                &map! {
                    "service" => "web",
                    "host" => "i-187",
                }
            )
        );
    }

    #[test_log::test]
    fn create_series_key_3() {
        assert_eq!(
            "cpu.total#env:dev;host:i-187;service:web",
            Database::create_series_key(
                "cpu.total",
                &map! {
                    "service" => "web",
                    "host" => "i-187",
                    "env" => "dev"
                }
            )
        );
    }
}
