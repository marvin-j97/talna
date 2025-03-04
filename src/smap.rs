use crate::SeriesId;
use byteorder::{BigEndian, ReadBytesExt};
use fjall::{CompressionType, PartitionCreateOptions, TxKeyspace, TxPartition, WriteTransaction};
use std::collections::HashSet;

const PARTITION_NAME: &str = "_talna#v1#smap";

pub struct SeriesMapping {
    keyspace: TxKeyspace,
    pub(crate) partition: TxPartition,
}

impl SeriesMapping {
    pub fn new(keyspace: &TxKeyspace) -> crate::Result<Self> {
        let opts = PartitionCreateOptions::default()
            .block_size(4_096)
            .compression(CompressionType::Lz4)
            .max_memtable_size(4_000_000);

        let partition = keyspace.open_partition(PARTITION_NAME, opts)?;

        Ok(Self {
            keyspace: keyspace.clone(),
            partition,
        })
    }

    pub fn insert(&self, tx: &mut WriteTransaction, series_key: &str, series_id: SeriesId) {
        tx.insert(&self.partition, series_key, series_id.to_be_bytes());
    }

    pub fn get(&self, series_key: &str) -> crate::Result<Option<SeriesId>> {
        Ok(self.partition.get(series_key)?.map(|bytes| {
            let mut reader = &bytes[..];
            reader.read_u64::<BigEndian>().expect("should deserialize")
        }))
    }

    pub fn list_all(&self) -> crate::Result<HashSet<SeriesId>> {
        let read_tx = self.keyspace.read_tx();

        read_tx
            .iter(&self.partition)
            .map(|kv| match kv {
                Ok((_, v)) => {
                    let mut reader = &v[..];
                    Ok(reader.read_u64::<BigEndian>().expect("should deserialize"))
                }
                Err(e) => Err(e.into()),
            })
            .collect::<crate::Result<HashSet<_>>>()
    }
}
