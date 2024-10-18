use crate::query::filter::parse_filter_query;
use crate::series_key::SeriesKey;
use crate::smap::SeriesMapping;
use crate::tag_index::TagIndex;
use crate::tag_sets::TagSets;
use crate::time::timestamp;
use crate::SeriesId;
use crate::Value;
use byteorder::{BigEndian, ReadBytesExt};
use fjall::{BlockCache, Partition, PartitionCreateOptions, TxKeyspace};
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{collections::BTreeMap, ops::Bound, path::Path, sync::RwLock};

/// A list of tags.
pub type TagSet<'a> = [(&'a str, &'a str)];

const METRICS_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyz_.";

const MINUTE_IN_NS: u128 = 60_000_000_000;

#[derive(Clone)]
pub struct Series {
    pub(crate) id: SeriesId,
    pub(crate) inner: Partition,
}

impl Series {
    pub fn insert(&self, ts: u128, value: Value) -> fjall::Result<()> {
        // NOTE: Invert timestamp to store in reverse order
        self.inner.insert((!ts).to_be_bytes(), value.to_be_bytes())
    }
}

#[derive(Debug)]
pub struct StreamItem {
    pub series_id: SeriesId,
    pub ts: u128,
    pub value: Value,
}

pub struct SeriesStream {
    // pub(crate) series_id: SeriesId,
    pub(crate) tags: crate::HashMap<String, String>,
    pub(crate) reader: Box<dyn Iterator<Item = fjall::Result<StreamItem>>>,
}

/// An embeddable time series database.
pub struct Database {
    pub(crate) keyspace: TxKeyspace,
    series: RwLock<BTreeMap<SeriesId, Series>>,
    smap: SeriesMapping,
    tag_index: TagIndex,
    pub(crate) tag_sets: TagSets,
}

// TODO: series should be stored in FIFO... but FIFO should not cause write stalls
// if disjoint...

impl Database {
    /// Uses an existing `fjall` keyspace to open a time series database.
    ///
    /// Partitions are prefixed with `_talna#` to avoid name clashes with other applications.
    pub fn from_keyspace(keyspace: TxKeyspace) -> fjall::Result<Self> {
        let tag_index = TagIndex::new(&keyspace)?;
        let tag_sets = TagSets::new(&keyspace)?;
        let series_mapping = SeriesMapping::new(&keyspace)?;

        let mut series_map = BTreeMap::new();

        // NOTE: Recover in-memory series map
        for kv in series_mapping.partition.inner().iter() {
            let (_, bytes) = kv?;

            let series_id = {
                let mut reader = &bytes[..];
                reader.read_u64::<BigEndian>()?
            };

            let series = Series {
                id: series_id,
                inner: keyspace
                    .open_partition(
                        &Self::get_series_name(series_id),
                        PartitionCreateOptions::default()
                            .block_size(64_000)
                            .compression(fjall::CompressionType::Lz4),
                    )?
                    .inner()
                    .clone(),
            };

            series_map.insert(series_id, series);
        }

        Ok(Self {
            keyspace,
            series: RwLock::new(series_map),
            smap: series_mapping,
            tag_index,
            tag_sets,
        })
    }

    /// Opens a new time series database.
    ///
    /// If you have a keyspace already in your application, you probably
    /// want to use [`Database::from_keyspace`] instead
    pub fn new<P: AsRef<Path>>(path: P, cache_mib: u64) -> fjall::Result<Self> {
        let keyspace = fjall::Config::new(path)
            .block_cache(Arc::new(BlockCache::with_capacity_bytes(
                cache_mib * 1_024 * 1_024,
            )))
            .open_transactional()?;

        Self::from_keyspace(keyspace)
    }

    fn get_series_name(series_id: SeriesId) -> String {
        format!("_talna#s#{series_id}")
    }

    fn prepare_query(
        &self,
        series_ids: &[SeriesId],
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> crate::Result<Vec<SeriesStream>> {
        // NOTE: Invert timestamps because stored in reverse order
        let range = (
            max.map(|x| u128::to_be_bytes(!x)),
            min.map(|x| u128::to_be_bytes(!x)),
        );

        let lock = self.series.read().expect("lock is poisoned");

        let readers = series_ids
            .iter()
            .map(|id| lock.get(id).cloned().unwrap())
            .collect::<Vec<_>>();

        drop(lock);

        readers
            .into_iter()
            .map(|series| {
                // TODO: maybe cache tagsets in Arc<HashMap> ...
                let tags = self.tag_sets.get(series.id)?;

                Ok(SeriesStream {
                    // series_id: series.id,
                    tags,
                    reader: Box::new(series.inner.range(range).map(move |x| match x {
                        Ok((k, v)) => {
                            let mut k = Cursor::new(k);
                            let ts = k.read_u128::<BigEndian>()?;
                            // NOTE: Invert timestamp back to original value
                            let ts = !ts;

                            let mut v = Cursor::new(v);

                            #[cfg(feature = "high_precision")]
                            let value = v.read_f64::<BigEndian>()?;

                            #[cfg(not(feature = "high_precision"))]
                            let value = v.read_f32::<BigEndian>()?;

                            Ok(StreamItem {
                                series_id: series.id,
                                value,
                                ts,
                            })
                        }
                        Err(e) => Err(e),
                    })),
                })
            })
            .collect::<crate::Result<Vec<_>>>()
    }

    pub(crate) fn start_query(
        &self,
        metric: &str,
        filter_expr: &str,
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> fjall::Result<Vec<SeriesStream>> {
        let filter = parse_filter_query(filter_expr).unwrap();

        let series_ids = filter.evaluate(&self.smap, &self.tag_index, metric)?;
        if series_ids.is_empty() {
            log::debug!("Query did not match any series");
            return Ok(vec![]);
        }

        log::debug!(
            "Querying metric {metric}{{{filter}}} [{min:?}..{max:?}] in series {series_ids:?}"
        );

        let streams = self.prepare_query(&series_ids, (min, max))?;

        Ok(streams)
    }

    /// Returns the average value for each bucket.
    pub fn avg<'a>(
        &'a self,
        metric: &'a str,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Average> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns the sum of the values of each bucket.
    pub fn sum<'a>(
        &'a self,
        metric: &'a str,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Sum> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns the minimum value for each bucket.
    pub fn min<'a>(
        &'a self,
        metric: &'a str,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Min> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns the maximum value for each bucket.
    pub fn max<'a>(
        &'a self,
        metric: &'a str,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Max> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Counts data points (ignores their value) per bucket.
    pub fn count<'a>(
        &'a self,
        metric: &'a str,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Count> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Write a data point to the database for the given metric, and tags it accordingly.
    pub fn write(&self, metric: &str, value: Value, tags: &TagSet) -> fjall::Result<()> {
        self.write_at(metric, timestamp(), value, tags)
    }

    #[doc(hidden)]
    pub fn write_at(
        &self,
        metric: &str,
        ts: u128,
        value: Value,
        tags: &TagSet,
    ) -> fjall::Result<()> {
        if !metric.chars().all(|c| METRICS_NAME_CHARS.contains(c)) {
            panic!("oops");
        }

        let series_key = SeriesKey::format(metric, tags);
        let series_id: Option<u64> = self.smap.get(&series_key)?;

        let series = if let Some(series_id) = series_id {
            // NOTE: Series already exists (happy path)

            self.series
                .read()
                .expect("lock is poisoned")
                .get(&series_id)
                .cloned()
                .unwrap()
        } else {
            // NOTE: Create series
            //
            // We need to run in a transaction (for serializability)
            //
            // Because we cannot rely on the series not being created since the
            // start of the function, we need to again look it up inside the transaction
            // to really make sure

            let mut tx = self.keyspace.write_tx();

            let series_id = tx.get(&self.smap.partition, &series_key)?.map(|bytes| {
                let mut reader = &bytes[..];
                reader.read_u64::<BigEndian>().expect("should deserialize")
            });

            if let Some(series_id) = series_id {
                // NOTE: Series was created since the start of the function

                self.series
                    .read()
                    .expect("lock is poisoned")
                    .get(&series_id)
                    .cloned()
                    .unwrap()
            } else {
                // NOTE: Actually create series

                let mut series_lock = self.series.write().expect("lock is poisoned");
                let next_series_id = series_lock.keys().max().map(|x| x + 1).unwrap_or_default();

                log::trace!("Creating series {next_series_id} for permutation {series_key:?}");

                let partition = self.keyspace.open_partition(
                    &Self::get_series_name(next_series_id),
                    PartitionCreateOptions::default()
                        // TODO: hyper_mode: maybe use manual_journal_persist(true),
                        // TODO: only flush to buffers either on interval or so
                        .block_size(64_000)
                        .compression(fjall::CompressionType::Lz4),
                )?;

                series_lock.insert(
                    next_series_id,
                    Series {
                        id: next_series_id,
                        inner: partition.inner().clone(),
                    },
                );

                drop(series_lock);

                self.smap.insert(&mut tx, &series_key, next_series_id);

                self.tag_index
                    .index(&mut tx, metric, tags, next_series_id)?;

                let mut serialized_tag_set = SeriesKey::allocate_string_for_tags(tags, 0);
                SeriesKey::join_tags(&mut serialized_tag_set, tags);

                self.tag_sets
                    .insert(&mut tx, next_series_id, &serialized_tag_set);

                tx.commit()?;

                // NOTE: Get inner because we don't want to insert and read series data in a transactional context
                Series {
                    id: next_series_id,
                    inner: partition.inner().clone(),
                }
            }
        };

        // NOTE: Invert timestamp to store in reverse order
        // because forward iteration is faster
        series.insert(ts, value)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tagset;
    use test_log::test;

    #[test]
    fn test_agg_cnt() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::new(&folder, 16)?;

        db.write_at(
            "cpu.total",
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            "cpu.total",
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            "cpu.total",
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.count("cpu.total", "service").build()?;
        assert_eq!(2, aggregator.len());
        assert!(aggregator.contains_key("talna"));
        assert!(aggregator.contains_key("smoltable"));

        for (group, mut aggregator) in aggregator {
            let bucket = aggregator.next().unwrap()?;

            match group.as_ref() {
                "talna" => {
                    assert_eq!(5.0, bucket.value);
                    assert_eq!(0, bucket.start);
                    assert_eq!(4, bucket.end);
                    assert_eq!(5, bucket.len);
                }
                "smoltable" => {
                    assert_eq!(2.0, bucket.value);
                    assert_eq!(5, bucket.start);
                    assert_eq!(6, bucket.end);
                    assert_eq!(2, bucket.len);
                }
                _ => {
                    unreachable!();
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_agg_max() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::new(&folder, 16)?;

        db.write_at(
            "cpu.total",
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            "cpu.total",
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            "cpu.total",
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.max("cpu.total", "service").build()?;
        assert_eq!(2, aggregator.len());
        assert!(aggregator.contains_key("talna"));
        assert!(aggregator.contains_key("smoltable"));

        for (group, mut aggregator) in aggregator {
            let bucket = aggregator.next().unwrap()?;

            match group.as_ref() {
                "talna" => {
                    assert_eq!(20.0, bucket.value);
                    assert_eq!(0, bucket.start);
                    assert_eq!(4, bucket.end);
                    assert_eq!(5, bucket.len);
                }
                "smoltable" => {
                    assert_eq!(7.0, bucket.value);
                    assert_eq!(5, bucket.start);
                    assert_eq!(6, bucket.end);
                    assert_eq!(2, bucket.len);
                }
                _ => {
                    unreachable!();
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_agg_min() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::new(&folder, 16)?;

        db.write_at(
            "cpu.total",
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            "cpu.total",
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            "cpu.total",
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.min("cpu.total", "service").build()?;
        assert_eq!(2, aggregator.len());
        assert!(aggregator.contains_key("talna"));
        assert!(aggregator.contains_key("smoltable"));

        for (group, mut aggregator) in aggregator {
            let bucket = aggregator.next().unwrap()?;

            match group.as_ref() {
                "talna" => {
                    assert_eq!(4.0, bucket.value);
                    assert_eq!(0, bucket.start);
                    assert_eq!(4, bucket.end);
                    assert_eq!(5, bucket.len);
                }
                "smoltable" => {
                    assert_eq!(5.0, bucket.value);
                    assert_eq!(5, bucket.start);
                    assert_eq!(6, bucket.end);
                    assert_eq!(2, bucket.len);
                }
                _ => {
                    unreachable!();
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_agg_sum() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::new(&folder, 16)?;

        db.write_at(
            "cpu.total",
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            "cpu.total",
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            "cpu.total",
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.sum("cpu.total", "service").build()?;
        assert_eq!(2, aggregator.len());
        assert!(aggregator.contains_key("talna"));
        assert!(aggregator.contains_key("smoltable"));

        for (group, mut aggregator) in aggregator {
            let bucket = aggregator.next().unwrap()?;

            match group.as_ref() {
                "talna" => {
                    assert_eq!(50.0, bucket.value);
                    assert_eq!(0, bucket.start);
                    assert_eq!(4, bucket.end);
                    assert_eq!(5, bucket.len);
                }
                "smoltable" => {
                    assert_eq!(12.0, bucket.value);
                    assert_eq!(5, bucket.start);
                    assert_eq!(6, bucket.end);
                    assert_eq!(2, bucket.len);
                }
                _ => {
                    unreachable!();
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_agg_avg() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::new(&folder, 16)?;

        db.write_at(
            "cpu.total",
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            "cpu.total",
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            "cpu.total",
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            "cpu.total",
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.avg("cpu.total", "service").build()?;
        assert_eq!(2, aggregator.len());
        assert!(aggregator.contains_key("talna"));
        assert!(aggregator.contains_key("smoltable"));

        for (group, mut aggregator) in aggregator {
            let bucket = aggregator.next().unwrap()?;

            match group.as_ref() {
                "talna" => {
                    assert_eq!(10.0, bucket.value);
                    assert_eq!(0, bucket.start);
                    assert_eq!(4, bucket.end);
                    assert_eq!(5, bucket.len);
                }
                "smoltable" => {
                    assert_eq!(6.0, bucket.value);
                    assert_eq!(5, bucket.start);
                    assert_eq!(6, bucket.end);
                    assert_eq!(2, bucket.len);
                }
                _ => {
                    unreachable!();
                }
            }
        }

        Ok(())
    }
}
