use std::path::Path;
use std::time::Instant;
use talna::{Database, Value};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> fjall::Result<()> {
    env_logger::builder()
        .filter_module("lsm_tree", log::LevelFilter::Warn)
        .filter_module("fjall", log::LevelFilter::Info)
        .filter_module("talna", log::LevelFilter::Trace)
        .parse_default_env()
        .init();

    let path = Path::new(".testy");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let db = Database::new(path, 128)?;

    let metric_name = "cpu.total";

    let start = Instant::now();

    {
        use rand::Rng;

        for host in ["h-0", "h-1", "h-2"] {
            let mut rng = rand::thread_rng();

            let tagset = talna::tagset!(
                "env" => "prod",
                "service" => "db",
                "host" => host,
            );

            for idx in 0..1_000_000 {
                // Generate a value that starts low and increases over time with some random variation
                let base_value: Value = if idx < 10_000 {
                    10.0 // Low load
                } else {
                    75.0 // High load
                };

                // Add some random variation
                let value = (base_value + rng.gen_range(-5.0..5.0)).max(0.0);

                db.write(metric_name, value, tagset)?;

                if idx % 5_000_000 == 0 {
                    log::info!("[{host}] ingested {idx}");
                }
            }
        }
    }

    log::info!("ingested in {:?}", start.elapsed());

    // TODO: allow tag sets (OR conjunction): host:[h-1, h-2]
    // TODO: allow negative query, e.g. -env:prod
    // TODO: wildcard, e.g. service:web.*, service:*-canary, region: *west*

    let filter_expr = "env:prod AND service:db";
    log::info!("querying: {filter_expr:?}");

    let now = talna::timestamp();

    for _ in 0..10 {
        let start = Instant::now();

        let buckets = db
            .avg(metric_name, "host")
            .filter(filter_expr)
            //.bucket(100_000)
            //.start(1_000_000_000)
            //.start(now - 1_500_000_000)
            .run()?;

        log::info!("done in {:?}", start.elapsed());

        log::info!("AVG result: {buckets:#?}");
    }

    /* for _ in 0..10 {
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

        eprintln!("AVG result: {avg:#?}");
    } */

    Ok(())
}
