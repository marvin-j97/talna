use crate::query::filter::Node as Filter;
use crate::reader::Reader;
use crate::smap::SeriesMapping;
use crate::tag_index::TagIndex;
use crate::{merge::Merger, SeriesId};
use fjall::{BlockCache, Partition, PartitionCreateOptions, TxKeyspace};

use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    path::Path,
    sync::RwLock,
};

const METRICS_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyz_.";

pub struct Database {
    keyspace: TxKeyspace,
    series: RwLock<BTreeMap<SeriesId, Partition>>,
    smap: SeriesMapping,
    tag_index: TagIndex,
}

// TODO: series should be stored in FIFO... but FIFO should not cause write stalls
// if disjoint...

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> fjall::Result<Self> {
        let keyspace = fjall::Config::new(path)
            .block_cache(Arc::new(BlockCache::with_capacity_bytes(
                128 * 1_024 * 1_024,
            )))
            .open_transactional()?;

        let tag_index = TagIndex::new(&keyspace)?;
        let series_mapping = SeriesMapping::new(&keyspace)?;

        Ok(Self {
            keyspace,
            series: RwLock::default(), // TODO: recover series s#
            smap: series_mapping,
            tag_index,
        })
    }

    fn get_series_name(series_id: SeriesId) -> String {
        format!("s#{series_id}")
    }

    pub(crate) fn create_series_key(metric: &str, tags: &HashMap<String, String>) -> String {
        // Sort tags
        let mut tags = tags.iter().collect::<Vec<_>>();
        tags.sort();

        let joined_tags =
            tags.iter()
                .enumerate()
                .fold("".to_string(), |mut s, (idx, (key, value))| {
                    if idx > 0 {
                        s.push(';');
                    }
                    s.push_str(key);
                    s.push(':');
                    s.push_str(value);
                    s
                });

        format!("{metric}#{joined_tags}")
    }

    // -> fjall::Result<impl Iterator<Item = fjall::Result<(u128, f32)>> + 'a>

    fn get_reader(
        &self,
        series_ids: &[SeriesId],
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> Reader {
        let range = (min.map(u128::to_be_bytes), (max.map(u128::to_be_bytes)));

        let lock = self.series.read().expect("lock is poisoned");

        let readers = series_ids
            .iter()
            .map(|id| lock.get(id).cloned().unwrap())
            .collect::<Vec<_>>();

        drop(lock);

        Reader::new(readers, |partitions| {
            let readers = partitions
                .iter()
                .map(|x| x.range(range))
                .collect::<Vec<_>>();

            let reader = Merger::new(readers).map(|kv| match kv {
                Ok((ts, v)) => Ok((!ts, v)),
                Err(kv) => Err(kv),
            });

            Box::new(reader)
        })
    }

    pub fn query(
        &self,
        metric: &str,
        filter: &Filter,
        (min, max): (Bound<u128>, Bound<u128>),
    ) -> fjall::Result<()> {
        let series_ids = filter.evaluate(&self.tag_index, metric)?;
        if series_ids.is_empty() {
            log::debug!("Query did not match any series");
            return Ok(());
        }

        log::debug!(
            "Querying metric {metric}{{{filter}}} [{min:?}..{max:?}] in series {series_ids:?}"
        );

        let reader = self.get_reader(&series_ids, (min, max));
        eprintln!("scanned {}", reader.count());

        Ok(())
    }

    pub fn write(
        &self,
        metric: &str,
        ts: u128,
        value: f32,
        tags: &HashMap<String, String>,
    ) -> fjall::Result<()> {
        if !metric.chars().all(|c| METRICS_NAME_CHARS.contains(c)) {
            panic!("oops");
        }

        let series_key = Self::create_series_key(metric, tags);
        let series_id = self.smap.get(&series_key)?;

        let series = if let Some(series_id) = series_id {
            self.series
                .read()
                .expect("lock is poisoned")
                .get(&series_id)
                .cloned()
        } else {
            None
        };

        // TODO: if the series is not found here, but then created, we create 2, if concurrent writes happen to same series

        let series = if let Some(series) = series {
            series
        } else {
            let mut tx = self.keyspace.write_tx();

            let mut series_lock = self.series.write().expect("lock is poisoned");
            let next_series_id = series_lock.keys().max().map(|x| x + 1).unwrap_or_default();

            log::trace!("creating series {next_series_id} for permutation {series_key:?}");

            let series = self.keyspace.open_partition(
                &Self::get_series_name(next_series_id),
                PartitionCreateOptions::default()
                    .block_size(128_000)
                    .compression(fjall::CompressionType::Miniz(6)),
            )?;

            series_lock.insert(next_series_id, series.inner().clone());

            drop(series_lock);

            self.smap.insert(&mut tx, &series_key, next_series_id);

            self.tag_index.index(&mut tx, metric, next_series_id)?;
            for (key, value) in tags {
                let term = format!("{metric}#{key}:{value}");
                self.tag_index.index(&mut tx, &term, next_series_id)?;
            }

            tx.commit()?;

            // NOTE: Get inner because we don't want to insert and read series data in a transactional context
            series.inner().clone()
        };

        // NOTE: Invert timestamp to store in reverse order
        series.insert((!ts).to_be_bytes(), value.to_be_bytes())?;

        Ok(())
    }
}
