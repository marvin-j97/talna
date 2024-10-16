use crate::SeriesId;
use fjall::{CompressionType, PartitionCreateOptions, TxKeyspace, TxPartition, WriteTransaction};

const PARTITION_NAME: &str = "_talna#tags";

/// Maps Series IDs to their tags
pub struct TagSets {
    partition: TxPartition,
}

impl TagSets {
    pub fn new(keyspace: &TxKeyspace) -> fjall::Result<Self> {
        let opts = PartitionCreateOptions::default()
            .block_size(4_096)
            .compression(CompressionType::Lz4)
            .max_memtable_size(8_000_000);

        let partition = keyspace.open_partition(PARTITION_NAME, opts)?;

        Ok(Self { partition })
    }

    pub fn insert(&self, tx: &mut WriteTransaction, series_id: SeriesId, tags: &str) {
        log::trace!("storing tag set {series_id:?} => {tags:?}");
        tx.insert(&self.partition, series_id.to_be_bytes(), tags);
    }

    pub fn get(&self, series_id: SeriesId) -> fjall::Result<crate::HashMap<String, String>> {
        Ok(self
            .partition
            .get(series_id.to_be_bytes())?
            .filter(|x| !x.is_empty())
            .map(|bytes| {
                let reader = std::str::from_utf8(&bytes).expect("should be utf-8");
                parse_key_value_pairs(reader)
            })
            .unwrap_or_default())
    }
}

fn parse_key_value_pairs(input: &str) -> crate::HashMap<String, String> {
    input
        .split(';')
        .map(|pair| {
            let mut split = pair.splitn(2, ':');

            if let (Some(key), Some(value)) = (split.next(), split.next()) {
                (key.to_string(), value.to_string())
            } else {
                panic!("Invalid parsed tag: {split:?}");
            }
        })
        .collect()
}
