use criterion::{criterion_group, criterion_main, Criterion};
use talna::tagset;

fn intersection(c: &mut Criterion) {
    let v = vec![vec![1, 2, 3, 4, 5], vec![1, 3, 5], vec![1, 3]];

    c.bench_function("intersection", |b| {
        b.iter(|| talna::query::filter::intersection(&v));
    });
}

fn union(c: &mut Criterion) {
    let v = vec![vec![1, 2, 3, 4, 5], vec![1, 3, 5], vec![1, 3]];

    c.bench_function("union", |b| {
        b.iter(|| talna::query::filter::union(&v));
    });
}

fn join_tags(c: &mut Criterion) {
    let tags = tagset!(
      "service" => "db",
      "env" => "prod",
      "host" => "host-1",
    );

    c.bench_function("join tags", |b| {
        b.iter(|| {
            talna::Database::join_tags(tags);
        });
    });
}

fn parse_filter_query(c: &mut Criterion) {
    c.bench_function("parse filter query (simple)", |b| {
        b.iter(|| {
            talna::query::filter::parse_filter_query("service:db AND env:prod").unwrap();
        });
    });

    c.bench_function("parse filter query (complex)", |b| {
        b.iter(|| {
            talna::query::filter::parse_filter_query(
                "os:debian AND service:db AND (env:prod OR env:staging)",
            )
            .unwrap();
        });
    });
}

fn insert_timestamp(c: &mut Criterion) {
    c.bench_function("insert single", |b| {
        let tags = tagset!(
            "service" => "db",
            "env" => "prod",
            "host" => "host-1",
        );

        let dir = tempfile::tempdir().unwrap();
        let db = talna::Database::new(&dir, 64).unwrap();

        let mut ts = 0;

        b.iter(|| {
            db.write("cpu", ts, 52.74, tags).unwrap();
            ts += 1;
        });
    });
}

fn avg(c: &mut Criterion) {
    c.bench_function("avg", |b| {
        let tags = tagset!(
            "service" => "db",
            "env" => "prod",
            "host" => "host-1",
        );

        let dir = tempfile::tempdir().unwrap();
        let db = talna::Database::new(&dir, 64).unwrap();

        db.write("cpu", 0, 10.0, tags).unwrap();
        db.write("cpu", 1, 11.0, tags).unwrap();
        db.write("cpu", 2, 12.0, tags).unwrap();
        db.write("cpu", 3, 13.0, tags).unwrap();
        db.write("cpu", 4, 14.0, tags).unwrap();

        b.iter(|| {
            db.avg("cpu", "host")
                .filter("service:db AND env:prod")
                .run()
                .unwrap();
        });
    });
}

criterion_group!(
    benches,
    intersection,
    union,
    join_tags,
    parse_filter_query,
    insert_timestamp,
    avg
);
criterion_main!(benches);