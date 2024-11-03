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
use crate::Timestamp;
use crate::Value;
use byteorder::{BigEndian, ReadBytesExt};
use fjall::{Partition, PartitionCreateOptions, TxKeyspace};
use std::io::Cursor;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;

pub const MINUTE_IN_NS: u128 = 60_000_000_000;

#[derive(Debug)]
pub struct StreamItem {
    pub series_id: SeriesId,
    pub ts: Timestamp,
    pub value: Value,
}

pub struct SeriesStream {
    pub(crate) tags: OwnedTagSets,
    pub(crate) reader: Box<dyn Iterator<Item = crate::Result<StreamItem>>>,
}

pub struct DatabaseInner {
    pub(crate) keyspace: TxKeyspace,

    /// Actual time series data
    data: Partition,

    /// Series mapping, series key -> series ID
    smap: SeriesMapping,

    // Inverted index of tag permutations
    tag_index: TagIndex,

    /// Maps series ID to its tags
    pub(crate) tag_sets: TagSets,

    #[allow(unused)]
    hyper_mode: bool,
}

/// An embeddable time series database
#[derive(Clone)]
pub struct Database(Arc<DatabaseInner>);

impl Database {
    /// Creates a new database builder.
    #[must_use]
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    pub(crate) fn from_keyspace(keyspace: TxKeyspace, hyper_mode: bool) -> crate::Result<Self> {
        let tag_index = TagIndex::new(&keyspace)?;
        let tag_sets = TagSets::new(&keyspace)?;
        let series_mapping = SeriesMapping::new(&keyspace)?;

        let data = keyspace
            .open_partition(
                "_talna#data",
                PartitionCreateOptions::default()
                    .use_bloom_filters(false)
                    .manual_journal_persist(true)
                    .block_size(64_000)
                    .compression(fjall::CompressionType::Lz4),
            )?
            .inner()
            .clone();

        Ok(Self(Arc::new(DatabaseInner {
            keyspace,
            data,
            smap: series_mapping,
            tag_index,
            tag_sets,
            hyper_mode,
        })))
    }

    fn format_data_point_key(series_id: SeriesId, ts: Timestamp) -> [u8; 24] {
        let mut data_point_key =
            [0; std::mem::size_of::<SeriesId>() + std::mem::size_of::<Timestamp>()];

        data_point_key[0..8].copy_from_slice(&series_id.to_be_bytes());
        data_point_key[8..24].copy_from_slice(&(!ts).to_be_bytes());
        data_point_key
    }

    fn prepare_query(
        &self,
        series_ids: &[SeriesId],
        (min, max): (Bound<Timestamp>, Bound<Timestamp>),
    ) -> crate::Result<Vec<SeriesStream>> {
        use fjall::Slice;
        use Bound::{Excluded, Included, Unbounded};

        series_ids
            .iter()
            .map(|&series_id| {
                // TODO: maybe cache tagsets in QuickCache...
                let tags = self.0.tag_sets.get(series_id)?;

                let kv_stream: Box<dyn Iterator<Item = fjall::Result<(Slice, Slice)>>> =
                    match (min, max) {
                        (Unbounded, Unbounded) => {
                            Box::new(self.0.data.prefix(series_id.to_be_bytes()))
                        }
                        (min @ (Included(_) | Excluded(_)), Unbounded) => {
                            let max =
                                Included(Self::format_data_point_key(series_id, Timestamp::MAX));
                            let min = min.map(|ts| Self::format_data_point_key(series_id, ts));

                            Box::new(self.0.data.range((max, min)))
                        }
                        (Unbounded, max @ (Included(_) | Excluded(_))) => {
                            let min = Self::format_data_point_key(series_id, 0);
                            let max = max.map(|ts| Self::format_data_point_key(series_id, ts));
                            Box::new(self.0.data.range((max, Included(min))))
                        }
                        (min @ (Included(_) | Excluded(_)), max @ (Included(_) | Excluded(_))) => {
                            let min = min.map(|ts| Self::format_data_point_key(series_id, ts));
                            let max = max.map(|ts| Self::format_data_point_key(series_id, ts));
                            Box::new(self.0.data.range((max, min)))
                        }
                    };

                Ok(SeriesStream {
                    tags,
                    reader: Box::new(kv_stream.map(move |x| match x {
                        Ok((k, v)) => {
                            use std::io::Seek;

                            let mut k = Cursor::new(k);

                            // Skip series ID
                            k.seek_relative(std::mem::size_of::<SeriesId>() as i64)?;

                            let ts = k.read_u128::<BigEndian>()?;
                            // NOTE: Invert timestamp back to original value
                            let ts = !ts;

                            let mut v = Cursor::new(v);

                            #[cfg(feature = "high_precision")]
                            let value = v.read_f64::<BigEndian>()?;

                            #[cfg(not(feature = "high_precision"))]
                            let value = v.read_f32::<BigEndian>()?;

                            Ok(StreamItem {
                                series_id,
                                ts,
                                value,
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
        (min, max): (Bound<Timestamp>, Bound<Timestamp>),
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
        ts: Timestamp,
        value: Value,
        tags: &TagSet,
    ) -> crate::Result<()> {
        let series_key = SeriesKey::format(metric, tags);
        let series_id: Option<SeriesId> = self.0.smap.get(&series_key)?;

        let series_id = if let Some(series_id) = series_id {
            // NOTE: Series already exists (happy path)
            series_id
        } else {
            // NOTE: Create series
            self.initialize_new_series(&series_key, metric, tags)?
        };

        let data_point_key = Self::format_data_point_key(series_id, ts);
        self.0.data.insert(data_point_key, value.to_be_bytes())?;

        if !self.0.hyper_mode {
            self.0.keyspace.persist(fjall::PersistMode::Buffer)?;
        }

        Ok(())
    }

    fn initialize_new_series(
        &self,
        series_key: &str,
        metric: MetricName,
        tags: &TagSet,
    ) -> crate::Result<SeriesId> {
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

        let series_id = if let Some(series_id) = series_id {
            // NOTE: Series was created since the start of the function
            series_id
        } else {
            // NOTE: Actually create series

            // TODO: atomic, persistent counter
            let next_series_id = self.0.smap.partition.inner().len()? as SeriesId;

            log::trace!("Creating series {next_series_id} for permutation {series_key:?}");

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

            next_series_id
        };

        Ok(series_id)
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
    fn test_range_cnt() -> crate::Result<()> {
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

        {
            let aggregator = db.count(metric_name, "service").start(2).build()?;
            assert_eq!(1, aggregator.len());
            assert!(aggregator.contains_key("talna"));

            for (group, mut aggregator) in aggregator {
                let bucket = aggregator.next().unwrap()?;

                match group.as_ref() {
                    "talna" => {
                        assert_eq!(3.0, bucket.value);
                        assert_eq!(2, bucket.start);
                        assert_eq!(4, bucket.end);
                        assert_eq!(3, bucket.len);
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }

        {
            let aggregator = db.count(metric_name, "service").end(3).build()?;
            assert_eq!(1, aggregator.len());
            assert!(aggregator.contains_key("talna"));

            for (group, mut aggregator) in aggregator {
                let bucket = aggregator.next().unwrap()?;

                match group.as_ref() {
                    "talna" => {
                        assert_eq!(4.0, bucket.value);
                        assert_eq!(0, bucket.start);
                        assert_eq!(3, bucket.end);
                        assert_eq!(4, bucket.len);
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }

        {
            let aggregator = db.count(metric_name, "service").start(1).end(3).build()?;
            assert_eq!(1, aggregator.len());
            assert!(aggregator.contains_key("talna"));

            for (group, mut aggregator) in aggregator {
                let bucket = aggregator.next().unwrap()?;

                match group.as_ref() {
                    "talna" => {
                        assert_eq!(3.0, bucket.value);
                        assert_eq!(1, bucket.start);
                        assert_eq!(3, bucket.end);
                        assert_eq!(3, bucket.len);
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }

        Ok(())
    }

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
