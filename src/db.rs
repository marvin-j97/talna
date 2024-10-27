use crate::query::filter::parse_filter_query;
use crate::series_key::SeriesKey;
use crate::smap::SeriesMapping;
use crate::tag_index::TagIndex;
use crate::tag_sets::OwnedTagSets;
use crate::tag_sets::TagSets;
use crate::time::timestamp;
use crate::DatabaseBuilder;
use crate::MetricName;
use crate::SeriesId;
use crate::TagSet;
use crate::Value;
use byteorder::{BigEndian, ReadBytesExt};
use fjall::{Partition, PartitionCreateOptions, TxKeyspace};
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{collections::BTreeMap, ops::Bound, sync::RwLock};

pub const MINUTE_IN_NS: u128 = 60_000_000_000;

#[derive(Clone)]
pub struct Series {
    pub(crate) id: SeriesId,
    pub(crate) inner: Partition,
}

impl Series {
    pub fn insert(&self, ts: u128, value: Value) -> crate::Result<()> {
        // NOTE: Invert timestamp to store in reverse order
        self.inner
            .insert((!ts).to_be_bytes(), value.to_be_bytes())
            .map_err(Into::into)
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
    pub(crate) tags: OwnedTagSets,
    pub(crate) reader: Box<dyn Iterator<Item = crate::Result<StreamItem>>>,
}

pub struct DatabaseInner {
    pub(crate) keyspace: TxKeyspace,
    series: RwLock<BTreeMap<SeriesId, Series>>,
    smap: SeriesMapping,
    tag_index: TagIndex,
    pub(crate) tag_sets: TagSets,
    hyper_mode: bool,
}

/// An embeddable time series database
#[derive(Clone)]
pub struct Database(Arc<DatabaseInner>);

// TODO: series should be stored in FIFO... but FIFO should not cause write stalls
// if disjoint...

impl Database {
    /// Creates a new database builder.
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    pub(crate) fn from_keyspace(keyspace: TxKeyspace, hyper_mode: bool) -> crate::Result<Self> {
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

        Ok(Self(Arc::new(DatabaseInner {
            keyspace,
            series: RwLock::new(series_map),
            smap: series_mapping,
            tag_index,
            tag_sets,
            hyper_mode,
        })))
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

        let lock = self.0.series.read().expect("lock is poisoned");

        series_ids
            .iter()
            .map(|id| lock.get(id).cloned().expect("series should exist"))
            .map(|series| {
                // TODO: maybe cache tagsets in QuickCache...
                let tags = self.0.tag_sets.get(series.id)?;

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
                        Err(e) => Err(e.into()),
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
    ) -> crate::Result<Vec<SeriesStream>> {
        // TODO: crate::Error with InvalidQuery enum variant
        let filter = parse_filter_query(filter_expr).expect("filter should be valid");

        let series_ids = filter.evaluate(&self.0.smap, &self.0.tag_index, metric)?;
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

    /// Returns an aggregation builder.
    ///
    /// The aggregation returns the average value for each bucket.
    #[must_use]
    pub fn avg<'a>(
        &'a self,
        metric: MetricName<'a>,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Average> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: &metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns an aggregation builder.
    ///
    /// The aggregation returns the sum of the values of each bucket.
    #[must_use]
    pub fn sum<'a>(
        &'a self,
        metric: MetricName<'a>,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Sum> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: &metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns an aggregation builder.
    ///
    /// The aggregation returns the minimum value for each bucket.
    #[must_use]
    pub fn min<'a>(
        &'a self,
        metric: MetricName<'a>,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Min> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: &metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns an aggregation builder.
    ///
    /// The aggregation returns the maximum value for each bucket.
    #[must_use]
    pub fn max<'a>(
        &'a self,
        metric: MetricName<'a>,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Max> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: &metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Returns an aggregation builder.
    ///
    /// The aggregation counts data points (ignores their value) per bucket.
    #[must_use]
    pub fn count<'a>(
        &'a self,
        metric: MetricName<'a>,
        group_by: &'a str,
    ) -> crate::agg::Builder<crate::agg::Count> {
        crate::agg::Builder {
            phantom: PhantomData,
            database: self,
            metric_name: &metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /// Write a data point to the database for the given metric, and tags it accordingly.
    ///
    /// # Errors
    ///
    /// Returns error if an I/O error occurred.
    pub fn write(&self, metric: MetricName, value: Value, tags: &TagSet) -> crate::Result<()> {
        self.write_at(metric, timestamp(), value, tags)
    }

    #[doc(hidden)]
    pub fn write_at(
        &self,
        metric: MetricName,
        ts: u128,
        value: Value,
        tags: &TagSet,
    ) -> crate::Result<()> {
        let series_key = SeriesKey::format(metric, tags);
        let series_id: Option<u64> = self.0.smap.get(&series_key)?;

        let series = if let Some(series_id) = series_id {
            // NOTE: Series already exists (happy path)
            self.0
                .series
                .read()
                .expect("lock is poisoned")
                .get(&series_id)
                .cloned()
                .expect("series should exist")
        } else {
            // NOTE: Create series
            self.initialize_new_series(&series_key, metric, tags)?
        };

        // NOTE: Invert timestamp to store in reverse order
        // because forward iteration is faster
        series.insert(ts, value)?;

        Ok(())
    }

    fn initialize_new_series(
        &self,
        series_key: &str,
        metric: MetricName,
        tags: &TagSet,
    ) -> crate::Result<Series> {
        // NOTE: We need to run in a transaction (for serializability)
        //
        // Because we cannot rely on the series not being created since the
        // start of the function, we need to again look it up inside the transaction
        // to really make sure
        let mut tx = self.0.keyspace.write_tx();

        let series_id = tx.get(&self.0.smap.partition, series_key)?.map(|bytes| {
            let mut reader = &bytes[..];
            reader.read_u64::<BigEndian>().expect("should deserialize")
        });

        let series = if let Some(series_id) = series_id {
            // NOTE: Series was created since the start of the function

            self.0
                .series
                .read()
                .expect("lock is poisoned")
                .get(&series_id)
                .cloned()
                .expect("series should exist")
        } else {
            // NOTE: Actually create series

            let mut series_lock = self.0.series.write().expect("lock is poisoned");
            let next_series_id = series_lock.keys().max().map(|x| x + 1).unwrap_or_default();

            log::trace!("Creating series {next_series_id} for permutation {series_key:?}");

            let partition = self.0.keyspace.open_partition(
                &Self::get_series_name(next_series_id),
                PartitionCreateOptions::default()
                    .manual_journal_persist(self.0.hyper_mode)
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

            self.0.smap.insert(&mut tx, series_key, next_series_id);

            self.0
                .tag_index
                .index(&mut tx, metric, tags, next_series_id)?;

            let mut serialized_tag_set = SeriesKey::allocate_string_for_tags(tags, 0);
            SeriesKey::join_tags(&mut serialized_tag_set, tags);

            self.0
                .tag_sets
                .insert(&mut tx, next_series_id, &serialized_tag_set);

            tx.commit()?;

            // NOTE: Get inner because we don't want to insert and read series data in a transactional context
            Series {
                id: next_series_id,
                inner: partition.inner().clone(),
            }
        };

        Ok(series)
    }

    /// Flushes writes.
    ///
    /// If sync is `true`, the writes are guaranteed to be written to disk
    /// when this function exits.
    ///
    /// # Errors
    ///
    /// Returns error if an I/O error occurred.
    pub fn flush(&self, sync: bool) -> crate::Result<()> {
        use fjall::PersistMode::{Buffer, SyncAll};

        self.0
            .keyspace
            .persist(if sync { SyncAll } else { Buffer })?;

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::tagset;
    use test_log::test;

    #[test]
    fn test_agg_cnt() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder().open(&folder)?;
        let metric_name = MetricName::try_from("hello").unwrap();

        db.write_at(
            metric_name,
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            metric_name,
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            metric_name,
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.count(metric_name, "service").build()?;
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
        let db = Database::builder().open(&folder)?;
        let metric_name = MetricName::try_from("hello").unwrap();

        db.write_at(
            metric_name,
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            metric_name,
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            metric_name,
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.max(metric_name, "service").build()?;
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
        let db = Database::builder().open(&folder)?;
        let metric_name = MetricName::try_from("hello").unwrap();

        db.write_at(
            metric_name,
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            metric_name,
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            metric_name,
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.min(metric_name, "service").build()?;
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
        let db = Database::builder().open(&folder)?;
        let metric_name = MetricName::try_from("hello").unwrap();

        db.write_at(
            metric_name,
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            metric_name,
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            metric_name,
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.sum(metric_name, "service").build()?;
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
        let db = Database::builder().open(&folder)?;
        let metric_name = MetricName::try_from("hello").unwrap();

        db.write_at(
            metric_name,
            0,
            4.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            1,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            2,
            6.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            3,
            10.0,
            tagset!(
                "service" => "talna",
            ),
        )?;
        db.write_at(
            metric_name,
            4,
            20.0,
            tagset!(
                "service" => "talna",
            ),
        )?;

        db.write_at(
            metric_name,
            5,
            7.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;
        db.write_at(
            metric_name,
            6,
            5.0,
            tagset!(
                "service" => "smoltable",
            ),
        )?;

        let aggregator = db.avg(metric_name, "service").build()?;
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
