mod agg;
mod db;
mod merge;
mod query;
mod reader;
mod smap;
mod tag_index;
mod tag_sets;

use db::Database;
use std::{path::Path, time::Instant};

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

    let db = Database::new(path, 64)?;

    let metric_name = "cpu.total";

    {
        use rand::Rng;

        for host in ["h-1", "h-2", "h-3", "i-187"] {
            let mut rng = rand::thread_rng();
            for idx in 0..100_000 {
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

    // TODO: allow tag:* as well

    let filter_expr = "env:prod AND service:db AND (host:h-1 OR host:h-2)";

    {
        let start = Instant::now();

        let avg = db
            .avg(metric_name, "host")
            .filter(filter_expr)
            .bucket(100_000)
            .run()?;

        log::info!("done in {:?}", start.elapsed());

        log::info!(
            "AVG result: {avg:#?}, BCS: {} MiB, blocks: {}",
            db.keyspace.inner.config.block_cache.size() / 1_024 / 1_024,
            db.keyspace.inner.config.block_cache.len(),
        );
    }

    for _ in 0..10 {
        let start = Instant::now();

        let _avg = db
            .avg(metric_name, "host")
            .filter(filter_expr)
            .bucket(100_000)
            .run()?;

        log::info!("done in {:?}", start.elapsed());
    }

    {
        log::info!("WILDCARD");
        let filter_expr = "*";

        let start = Instant::now();

        let avg = db
            .avg(metric_name, "host")
            .filter(filter_expr)
            .bucket(100_000)
            .run()?;
        log::info!("done in {:?}", start.elapsed());

        eprintln!(
            "AVG result: {avg:#?}, BCS: {} MiB, blocks: {}",
            db.keyspace.inner.config.block_cache.size() / 1_024 / 1_024,
            db.keyspace.inner.config.block_cache.len(),
        );
    }

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
