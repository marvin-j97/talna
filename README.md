# talna

[![CI](https://github.com/marvin-j97/talna/actions/workflows/test.yml/badge.svg)](https://github.com/marvin-j97/talna/actions/workflows/test.yml)
[![docs.rs](https://img.shields.io/docsrs/talna?color=green)](https://docs.rs/talna)
[![Crates.io](https://img.shields.io/crates/v/talna?color=blue)](https://crates.io/crates/talna)
![MSRV](https://img.shields.io/badge/MSRV-1.79.0-blue)
[![dependency status](https://deps.rs/repo/github/marvin-j97/talna/status.svg)](https://deps.rs/repo/github/marvin-j97/talna)

Ilocano: Peace, Serenity

Icelandic, Old Norse: Numbers

---

A simple, embeddable time series database using [`fjall`](https://github.com/fjall-rs/fjall).

It uses <https://github.com/fjall-rs/fjall> as its underlying storage engine,
being able to ingest ~700k data points per seconds.

The LSM-based storage engine causes no degradation in write ingestion speed, even for large datasets,
has low write amplification (good for SSDs) and compresses the on-disk data (again, good for SSDs).

The tagging and querying mechanism is modelled after Datadog's metrics service (<https://www.datadoghq.com/blog/engineering/timeseries-indexing-at-scale/>).

Data points are f32s by default, but can be switched to f64 using the `high_precision` feature flag.

1 billion data points (default config, jemalloc, i9 11900k):

- ingested in 1374s (~727k inserts per second)
- average memory usage: 100 MB , peak: ~170 MB
- query latency for 1 million data points (`AVG | env:prod AND service:db AND (host:h-1 OR host:h-2 OR host:h-3)`): 110ms
- disk space: 12 GB

```rs
# let path = std::path::Path::new(".testy");
# if path.try_exists()? {
#   std::fs::remove_dir_all(path)?;
# }
#
# fn timestamp() -> u128 {
#     0
# }
#
use talna::{Database, tagset};

let db = Database::new(path, /* cache size in MiB */ 64)?;

db.write(
    "cpu.total", // metric name
    25.42, // actual value (f64)
    tagset!(
        "env" => "prod",
        "service" => "db",
        "host" => "h-1",
    ),
)?;

db.write(
    "cpu.total", // metric name
    42.42, // actual value (f64)
    tagset!(
        "env" => "prod",
        "service" => "db",
        "host" => "h-2",
    ),
)?;

let buckets = db
  .avg(/* metric */ "cpu.total", /* group by tag */ "host")
  .filter("env:prod AND service:db")
  // use .start() and .end() to set the time bounds
  // use .bucket() to set the granularity (bucket width in nanoseconds)
  .run()?;

println!("{buckets:#?}");

# Ok::<(), talna::Error>(())
```