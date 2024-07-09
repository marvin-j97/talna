use byteorder::{BigEndian, ReadBytesExt};
use std::{collections::BinaryHeap, io::Cursor};

#[derive(Debug)]
struct HeapItem(usize, u128, f32);

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        (self.0, self.1).eq(&(other.0, other.1))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.1.cmp(&self.1)
    }
}

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(fjall::Error::from(e))),
        }
    };
}

pub struct Merger<I: Iterator<Item = fjall::Result<(fjall::UserKey, fjall::UserValue)>>> {
    readers: Vec<I>,
    heap: BinaryHeap<HeapItem>,
    is_initialized: bool,
}

impl<I: Iterator<Item = fjall::Result<(fjall::UserKey, fjall::UserValue)>>> Merger<I> {
    pub fn new(readers: Vec<I>) -> Self {
        Self {
            readers,
            heap: BinaryHeap::default(),
            is_initialized: false,
        }
    }

    fn advance(&mut self, idx: usize) -> fjall::Result<()> {
        if let Some(kv) = self.readers.get_mut(idx).unwrap().next() {
            let (k, v) = kv?;

            let mut k = Cursor::new(k);
            let k = k.read_u128::<BigEndian>()?;

            let mut v = Cursor::new(v);
            let v = v.read_f32::<BigEndian>()?;

            self.heap.push(HeapItem(idx, k, v));
        }
        Ok(())
    }
}

impl<I: Iterator<Item = fjall::Result<(fjall::UserKey, fjall::UserValue)>>> Iterator for Merger<I> {
    type Item = fjall::Result<(u128, f32)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            for i in 0..self.readers.len() {
                fail_iter!(self.advance(i));
            }
            self.is_initialized = true;
        }

        let head = self.heap.pop()?;

        fail_iter!(self.advance(head.0));

        Some(Ok((head.1, head.2)))
    }
}
