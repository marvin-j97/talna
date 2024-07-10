mod db;
mod merge;
mod query;
mod reader;
mod smap;
mod tag_index;
mod tag_sets;

use db::Database;
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

    {
        use rand::Rng;

        for host in ["h-1", "h-2", "h-3", "i-187"] {
            let mut rng = rand::thread_rng();
            for idx in 0..100 {
                // Generate a value that starts low and increases over time with some random variation
                let base_value: f64 = if idx < 50 {
                    10.0 // Low load
                } else {
                    75.0 // High load
                };

                // Add some random variation
                let value = (base_value + rng.gen_range(-5.0..5.0)).max(0.0);

                db.write(
                    metric_name,
                    idx,
                    value,
                    &map! {
                        "env" => "prod",
                        "service" => "db",
                        "host" => host,
                    },
                )?;
            }
        }
    }

    let filter_expr = "env:prod AND service:db";

    /* for sample in db
        .query_with_filter(
            metric_name,
            filter_expr,
            (Bound::Unbounded, Bound::Unbounded),
        )?
        .unwrap()
    {
        let sample = sample?;
        eprintln!("{sample:?}");
    } */

    let start = Instant::now();

    let avg = db.aggregate_avg(
        metric_name,
        filter_expr,
        (Bound::Unbounded, Bound::Unbounded),
        "host",
        10,
    )?;

    eprintln!("AVG result: {avg:#?}");

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
