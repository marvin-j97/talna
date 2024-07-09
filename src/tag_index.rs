use crate::SeriesId;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fjall::{PartitionCreateOptions, TxKeyspace, TxPartition, WriteTransaction};
use std::sync::Arc;

pub struct TagIndex {
    partition: TxPartition,
}

impl TagIndex {
    pub fn new(keyspace: &TxKeyspace) -> fjall::Result<Self> {
        use fjall::{compaction::SizeTiered, CompressionType};

        let opts = PartitionCreateOptions::default()
            .block_size(4_096)
            .compression(CompressionType::Lz4);

        let partition = keyspace.open_partition("tags", opts)?;

        partition.inner().set_max_memtable_size(8_000_000);
        partition
            .inner()
            .set_compaction_strategy(Arc::new(SizeTiered::new(8_000_000)));

        Ok(Self { partition })
    }

    // TODO: could probably use varint encoding + delta encoding here
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
        term: &str,
        series_id: SeriesId,
    ) -> fjall::Result<()> {
        log::trace!("indexing {term:?} => {series_id}");

        tx.fetch_update(&self.partition, term, |bytes| match bytes {
            Some(bytes) => {
                // TODO: can be optimized by not deserializing the prev posting list...
                // TODO: just increment length by 1, memcpy old posting list, then append series_id

                let mut reader = &bytes[..];

                let len = reader.read_u64::<BigEndian>().expect("should deserialize");
                let mut postings = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    postings.push(reader.read_u64::<BigEndian>().expect("should deserialize"));
                }
                postings.push(series_id);

                log::trace!("posting list {term:?} is now {postings:?}");

                Some(Self::serialize_postings_list(&postings).into())
            }
            None => Some(Self::serialize_postings_list(&[series_id]).into()),
        })?;

        Ok(())
    }

    pub fn query(&self, term: &str) -> fjall::Result<Vec<SeriesId>> {
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
}
