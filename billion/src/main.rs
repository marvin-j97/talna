use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use std::{path::Path, sync::Arc};
use talna::{Database, MetricName};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> talna::Result<()> {
    env_logger::builder()
        .filter_module("lsm_tree", log::LevelFilter::Warn)
        .filter_module("fjall", log::LevelFilter::Warn)
        .filter_module("talna", log::LevelFilter::Trace)
        .filter_module("billion", log::LevelFilter::Trace)
        .parse_default_env()
        .init();

    let path = Path::new(".testy");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let mut rng = rand::thread_rng();
    let db = Database::new(path, 128)?;
    let metric_name = MetricName::try_from("cpu.total").unwrap();

    let max_memory_bytes = Arc::new(AtomicU64::new(0));

    std::thread::spawn({
        let max_memory_bytes = max_memory_bytes.clone();

        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();

        let pid = std::process::id();
        let pid = sysinfo::Pid::from(pid as usize);

        move || loop {
            sys.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                true,
                sysinfo::ProcessRefreshKind::new().with_memory(),
            );
            let proc = sys.processes();
            let child = proc.get(&pid).unwrap();
            max_memory_bytes.fetch_max(child.memory(), std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let start = Instant::now();

    {
        use rand::Rng;

        for (hidx, host) in [
            "h-0", "h-1", "h-2", "h-3", "h-4", "h-5", "h-6", "h-7", "h-8", "h-9",
        ]
        .into_iter()
        .enumerate()
        {
            let tagset = talna::tagset!(
                "env" => "prod",
                "service" => "db",
                "host" => host,
            );

            for idx in 0..100_000_000 {
                let items_written = (hidx * 100_000_000 + idx) as u128;

                let value = rng.gen_range(0.0..100.0);
                db.write_at(metric_name, items_written, value, tagset)?;

                if idx > 0 && idx % 5_000_000 == 0 {
                    let elapsed = start.elapsed();
                    let elapsed_ns = elapsed.as_nanos();

                    let ns_per_item = elapsed_ns / items_written;
                    let write_speed = 1_000_000_000 / ns_per_item;

                    let max_memory = max_memory_bytes.load(std::sync::atomic::Ordering::Relaxed);

                    log::info!(
                        "[{host}] ingested {idx} - {write_speed} WPS - peak mem: {} MiB",
                        max_memory / 1_024 / 1_024
                    );
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let elapsed_ns = elapsed.as_nanos();

    let items_written = 1_000_000_000;
    let ns_per_item = elapsed_ns / items_written;
    let write_speed = 1_000_000_000 / ns_per_item;

    let max_memory = max_memory_bytes.load(std::sync::atomic::Ordering::Relaxed);

    log::info!("ingested 1 billion in {elapsed:?}");
    log::info!("write latency per item: {:?}ns", elapsed_ns / 1_000_000_000);
    log::info!("write speed: {write_speed} WPS");
    log::info!("peak mem: {} MiB", max_memory / 1_024 / 1_024);

    let disk_space = fs_extra::dir::get_size(".testy").unwrap();
    log::info!("disk space (GiB): {}", disk_space / 1_024 / 1_024 / 1_024);

    let lower_bound = 1_000_000_000 - 500_000;

    for _ in 0..5 {
        let start = Instant::now();

        let buckets = db
            .avg(metric_name, "host")
            .filter("host:h-0 OR host:h-1")
            .start(lower_bound)
            .build()?
            .collect()?;

        eprintln!("query in {:?}", start.elapsed());

        eprintln!("{buckets:#?}");
    }

    Ok(())
}
