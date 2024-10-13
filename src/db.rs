use crate::merge::StreamItem;
use crate::query::filter::parse_filter_query;
use crate::smap::SeriesMapping;
use crate::tag_index::TagIndex;
use crate::tag_sets::TagSets;
use crate::Value;
use crate::{merge::Merger, SeriesId};
use byteorder::{BigEndian, ReadBytesExt};
use fjall::{BlockCache, Partition, PartitionCreateOptions, TxKeyspace};
use std::io::Cursor;
use std::sync::Arc;
use std::{collections::BTreeMap, ops::Bound, path::Path, sync::RwLock};

pub type TagSet = Vec<(String, String)>;

const METRICS_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyz_.";

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

pub struct QueryStream {
    pub(crate) affected_series: Vec<SeriesId>,
    pub(crate) reader: Box<dyn Iterator<Item = fjall::Result<StreamItem>>>,
}

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
    pub fn new<P: AsRef<Path>>(path: P, cache_mib: u64) -> fjall::Result<Self> {
        let keyspace = fjall::Config::new(path)
            .block_cache(Arc::new(BlockCache::with_capacity_bytes(
                cache_mib * 1_024 * 1_024,
            )))
            .open_transactional()?;

        let tag_index = TagIndex::new(&keyspace)?;
        let tag_sets = TagSets::new(&keyspace)?;
        let series_mapping = SeriesMapping::new(&keyspace)?;

        Ok(Self {
            keyspace,
            series: RwLock::default(), // TODO: recover series s#
            smap: series_mapping,
            tag_index,
            tag_sets,
        })
    }

    fn get_series_name(series_id: SeriesId) -> String {
        format!("talna#s#{series_id}")
    }

    #[doc(hidden)]
    pub fn join_tags(tags: &[(String, String)]) -> String {
        let mut tags = tags.iter().collect::<Vec<_>>();
        tags.sort();

        let total_len = tags
            .iter()
            .map(|(key, value)| key.len() + value.len() + 1) // +1 for the ':' between key and value
            .sum::<usize>()
            + tags.len().saturating_sub(1); // Add space for the semicolons

        let mut result = String::with_capacity(total_len);

        for (idx, (key, value)) in tags.iter().enumerate() {
            if idx > 0 {
                result.push(';');
            }
            result.push_str(key);
            result.push(':');
            result.push_str(value);
        }

        result
    }

    pub(crate) fn create_series_key(metric: &str, tags: &[(String, String)]) -> String {
        let joined_tags = Self::join_tags(tags);
        format!("{metric}#{joined_tags}")
    }

    fn get_reader(
        &self,
        series_ids: &[SeriesId],
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> Merger<impl DoubleEndedIterator<Item = fjall::Result<StreamItem>>> {
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

        let readers = readers
            .into_iter()
            .map(|series| {
                series.inner.range(range).map(move |x| match x {
                    Ok((k, v)) => {
                        let mut k = Cursor::new(k);
                        let ts = k.read_u128::<BigEndian>()?;

                        let mut v = Cursor::new(v);
                        let value = v.read_f64::<BigEndian>()?;

                        Ok(StreamItem {
                            series_id: series.id,
                            ts,
                            value,
                        })
                    }
                    Err(e) => Err(e),
                })
            })
            .collect::<Vec<_>>();

        Merger::new(readers)
    }

    pub(crate) fn start_query(
        &self,
        metric: &str,
        filter_expr: &str,
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> fjall::Result<QueryStream> {
        let filter = parse_filter_query(filter_expr).unwrap();

        let series_ids = filter.evaluate(&self.smap, &self.tag_index, metric)?;
        if series_ids.is_empty() {
            log::debug!("Query did not match any series");
            return Ok(QueryStream {
                affected_series: vec![],
                reader: Box::new(std::iter::empty()),
            });
        }

        log::debug!(
            "Querying metric {metric}{{{filter}}} [{min:?}..{max:?}] in series {series_ids:?}"
        );

        let reader = self.get_reader(&series_ids, (min, max));

        Ok(QueryStream {
            affected_series: series_ids,
            reader: Box::new(reader),
        })
    }

    pub fn avg<'a>(
        &'a self,
        metric: &'a str,
        group_by: &'a str,
    ) -> crate::agg::avg::Aggregator<'a> {
        const MINUTE_IN_NS: u128 = 900_000_000_000;

        crate::agg::avg::Aggregator {
            database: self,
            metric_name: metric,
            filter_expr: "*", // TODO: need wildcard
            bucket_width: MINUTE_IN_NS,
            group_by,
            max_ts: None,
            min_ts: None,
        }
    }

    /*  // TODO: QueryInput struct
    pub fn query(
        &self,
        metric: &str,
        filter_expression: &str,
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> fjall::Result<Option<Merger<impl DoubleEndedIterator<Item = fjall::Result<StreamItem>>>>>
    {
        let filter = parse_filter_query(filter_expression).unwrap();

        Ok(self
            .start_query(metric, &filter, (min, max))?
            .map(|stream| reader))
    } */

    pub fn write(
        &self,
        metric: &str,
        ts: u128,
        value: Value,
        tags: &[(String, String)],
    ) -> fjall::Result<()> {
        if !metric.chars().all(|c| METRICS_NAME_CHARS.contains(c)) {
            panic!("oops");
        }

        let series_key = Self::create_series_key(metric, tags);
        let series_id = self.smap.get(&series_key)?;

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

                self.tag_index.index(&mut tx, metric, next_series_id)?;
                for (key, value) in tags {
                    let term = format!("{metric}#{key}:{value}");
                    self.tag_index.index(&mut tx, &term, next_series_id)?;
                }

                self.tag_sets
                    .insert(&mut tx, next_series_id, &Self::join_tags(tags));

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

    #[test_log::test]
    fn create_series_key() {
        assert_eq!(
            "cpu.total#service:web",
            Database::create_series_key("cpu.total", tagset!("service" => "web"))
        );
    }

    #[test_log::test]
    fn create_series_key_2() {
        assert_eq!(
            "cpu.total#host:i-187;service:web",
            Database::create_series_key(
                "cpu.total",
                tagset!(
                        "service" => "web",
                        "host" => "i-187",
                )
            )
        );
    }

    #[test_log::test]
    fn create_series_key_3() {
        assert_eq!(
            "cpu.total#env:dev;host:i-187;service:web",
            Database::create_series_key(
                "cpu.total",
                tagset!(
                    "service" => "web",
                    "host" => "i-187",
                    "env" => "dev"
                )
            )
        );
    }
}
