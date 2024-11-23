use crate::{MetricName, SeriesId, TagSet};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fjall::{CompressionType, PartitionCreateOptions, TxKeyspace, TxPartition, WriteTransaction};

const PARTITION_NAME: &str = "_talna#v1#tidx";

/// Inverted index, mapping key:value tag pairs to series IDs
pub struct TagIndex {
    keyspace: TxKeyspace,
    partition: TxPartition,
}

impl TagIndex {
    pub fn new(keyspace: &TxKeyspace) -> crate::Result<Self> {
        let opts = PartitionCreateOptions::default()
            .block_size(4_096)
            .compression(CompressionType::Lz4)
            .max_memtable_size(8_000_000);

        let partition = keyspace.open_partition(PARTITION_NAME, opts)?;

        Ok(Self {
            keyspace: keyspace.clone(),
            partition,
        })
    }

    // TODO: could probably use varint encoding + delta encoding here
    // or even bitpacking for blocks of 128, and delta varint for remaining
    fn serialize_postings_list(postings: &[SeriesId]) -> Vec<u8> {
        let mut posting_list = vec![];

        posting_list
            .write_u64::<BigEndian>(postings.len() as u64)
            .expect("should serialize");

        for id in postings {
            posting_list
                .write_u64::<BigEndian>(*id)
                .expect("should serialize");
        }

        posting_list
    }

    pub fn index(
        &self,
        tx: &mut WriteTransaction,
        metric: MetricName,
        tags: &TagSet,
        series_id: SeriesId,
    ) -> crate::Result<()> {
        self.index_term(tx, &metric, series_id)?;

        for (key, value) in tags {
            let term = format!("{metric}#{key}:{value}");
            self.index_term(tx, &term, series_id)?;
        }

        Ok(())
    }

    fn index_term(
        &self,
        tx: &mut WriteTransaction,
        term: &str,
        series_id: SeriesId,
    ) -> crate::Result<()> {
        // log::trace!("Indexing {term:?} => {series_id}");

        tx.fetch_update(&self.partition, term, |bytes| match bytes {
            Some(bytes) => {
                let mut reader = &bytes[..];

                let len = reader.read_u64::<BigEndian>().expect("should deserialize");
                let mut postings = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    postings.push(reader.read_u64::<BigEndian>().expect("should deserialize"));
                }
                postings.push(series_id);

                // log::trace!("posting list {term:?} is now {postings:?}");

                Some(Self::serialize_postings_list(&postings).into())
            }
            None => Some(Self::serialize_postings_list(&[series_id]).into()),
        })?;

        Ok(())
    }

    pub fn format_key(metric_name: &str, key: &str, value: &str) -> String {
        let mut s = String::with_capacity(metric_name.len() + 1 + key.len() + 1 + value.len());
        s.push_str(metric_name);
        s.push('#');
        s.push_str(key);
        s.push(':');
        s.push_str(value);
        s
    }

    pub fn query_eq(&self, term: &str) -> crate::Result<Vec<SeriesId>> {
        Ok(self
            .partition
            .get(term)?
            .map(|bytes| {
                let mut reader = &bytes[..];

                let len = reader.read_u64::<BigEndian>().expect("should deserialize");
                let mut postings = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    postings.push(reader.read_u64::<BigEndian>().expect("should deserialize"));
                }

                postings
            })
            .unwrap_or_default())
    }

    pub fn query_prefix(&self, prefix: &str) -> crate::Result<Vec<SeriesId>> {
        let mut ids = vec![];

        let read_tx = self.keyspace.read_tx();

        for kv in read_tx.prefix(&self.partition, prefix) {
            let (_, v) = kv?;

            let mut reader = &v[..];

            let len = reader.read_u64::<BigEndian>().expect("should deserialize");
            let mut postings = Vec::with_capacity(len as usize);

            for _ in 0..len {
                postings.push(reader.read_u64::<BigEndian>().expect("should deserialize"));
            }

            ids.extend(postings);
        }

        ids.sort_unstable();
        ids.dedup();

        Ok(ids)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test_log::test]
    fn test_tag_index_prefix() -> crate::Result<()> {
        let path = tempfile::tempdir()?;
        let keyspace = fjall::Config::new(&path).open_transactional()?;
        let tag_index = TagIndex::new(&keyspace)?;
        let metric = MetricName::try_from("cpu.total").unwrap();

        let mut tx = keyspace.write_tx();

        {
            let tags = crate::tagset!(
                "service" => "prod-db",
            );
            tag_index.index(&mut tx, metric, tags, 0)?;
        }
        {
            let tags = crate::tagset!(
                "service" => "staging-db",
            );
            tag_index.index(&mut tx, metric, tags, 1)?;
        }
        {
            let tags = crate::tagset!(
                "service" => "test-db",
            );
            tag_index.index(&mut tx, metric, tags, 2)?;
        }
        {
            let tags = crate::tagset!(
                "service" => "prod-ui",
            );
            tag_index.index(&mut tx, metric, tags, 3)?;
        }
        {
            let tags = crate::tagset!(
                "service" => "staging-ui",
            );
            tag_index.index(&mut tx, metric, tags, 4)?;
        }
        {
            let tags = crate::tagset!(
                "service" => "test-ui",
            );
            tag_index.index(&mut tx, metric, tags, 5)?;
        }

        tx.commit()?;

        assert_eq!(
            vec![0, 3],
            tag_index.query_prefix("cpu.total#service:prod-")?
        );

        Ok(())
    }

    #[test_log::test]
    fn test_tag_index_eq() -> crate::Result<()> {
        let path = tempfile::tempdir()?;
        let keyspace = fjall::Config::new(&path).open_transactional()?;
        let tag_index = TagIndex::new(&keyspace)?;
        let metric = MetricName::try_from("cpu.total").unwrap();

        let mut tx = keyspace.write_tx();

        {
            let tags = crate::tagset!(
                "env" => "prod",
                "service" => "db",
            );
            tag_index.index(&mut tx, metric, tags, 0)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "dev",
                "service" => "db",
            );
            tag_index.index(&mut tx, metric, tags, 1)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "test",
                "service" => "db",
            );
            tag_index.index(&mut tx, metric, tags, 2)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "staging",
                "service" => "db",
            );
            tag_index.index(&mut tx, metric, tags, 3)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "prod",
                "service" => "ui",
            );
            tag_index.index(&mut tx, metric, tags, 4)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "dev",
                "service" => "ui",
            );
            tag_index.index(&mut tx, metric, tags, 5)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "test",
                "service" => "ui",
            );
            tag_index.index(&mut tx, metric, tags, 6)?;
        }
        {
            let tags = crate::tagset!(
                "env" => "staging",
                "service" => "ui",
            );
            tag_index.index(&mut tx, metric, tags, 7)?;
        }

        tx.commit()?;

        assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7], tag_index.query_eq(&metric)?);
        assert_eq!(
            vec![0, 4],
            tag_index.query_eq(&format!("{metric}#env:prod"))?
        );
        assert_eq!(
            vec![4, 5, 6, 7],
            tag_index.query_eq(&format!("{metric}#service:ui"))?
        );

        Ok(())
    }
}
