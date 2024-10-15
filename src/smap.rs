use crate::SeriesId;
use byteorder::{BigEndian, ReadBytesExt};
use fjall::{CompressionType, PartitionCreateOptions, TxKeyspace, TxPartition, WriteTransaction};
use std::collections::HashSet;

const PARTITION_NAME: &str = "_talna#smap";

pub struct SeriesMapping {
    pub(crate) partition: TxPartition,
}

impl SeriesMapping {
    pub fn new(keyspace: &TxKeyspace) -> fjall::Result<Self> {
        let opts = PartitionCreateOptions::default()
            .block_size(4_096)
            .compression(CompressionType::Lz4)
            .max_memtable_size(4_000_000);

        let partition = keyspace.open_partition(PARTITION_NAME, opts)?;

        Ok(Self { partition })
    }

    pub fn insert(&self, tx: &mut WriteTransaction, series_key: &str, series_id: SeriesId) {
        tx.insert(&self.partition, series_key, series_id.to_be_bytes());
    }

    pub fn get(&self, series_key: &str) -> fjall::Result<Option<SeriesId>> {
        Ok(self.partition.get(series_key)?.map(|bytes| {
            let mut reader = &bytes[..];
            reader.read_u64::<BigEndian>().expect("should deserialize")
        }))
    }

    pub fn list_all(&self) -> fjall::Result<HashSet<SeriesId>> {
        // TODO: read_tx
        self.partition
            .inner()
            .iter()
            .map(|kv| match kv {
                Ok((_, v)) => {
                    let mut reader = &v[..];
                    Ok(reader.read_u64::<BigEndian>().expect("should deserialize"))
                }
                Err(e) => Err(e),
            })
            .collect::<fjall::Result<HashSet<_>>>()
    }
}
