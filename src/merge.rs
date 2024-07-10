use crate::SeriesId;
use std::collections::BinaryHeap;

#[derive(Debug)]
struct HeapItem(usize, StreamItem);

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        (self.0, self.1.ts).eq(&(other.0, other.1.ts))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.1.ts.cmp(&self.1.ts)
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

#[derive(Debug)]
pub struct StreamItem {
    pub series_id: SeriesId,
    pub ts: u128,
    pub value: f64,
}

pub struct Merger<I: Iterator<Item = fjall::Result<StreamItem>>> {
    readers: Vec<I>,
    heap: BinaryHeap<HeapItem>,
    is_initialized: bool,
}

impl<I: Iterator<Item = fjall::Result<StreamItem>>> Merger<I> {
    pub fn new(readers: Vec<I>) -> Self {
        Self {
            readers,
            heap: BinaryHeap::default(),
            is_initialized: false,
        }
    }

    fn advance(&mut self, idx: usize) -> fjall::Result<()> {
        if let Some(item) = self.readers.get_mut(idx).unwrap().next() {
            self.heap.push(HeapItem(idx, item?));
        }
        Ok(())
    }
}

impl<I: Iterator<Item = fjall::Result<StreamItem>>> Iterator for Merger<I> {
    type Item = fjall::Result<StreamItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            for i in 0..self.readers.len() {
                fail_iter!(self.advance(i));
            }
            self.is_initialized = true;
        }

        let mut head = self.heap.pop()?;

        fail_iter!(self.advance(head.0));

        // NOTE: Invert timestamp back to original value
        head.1.ts = !head.1.ts;

        Some(Ok(head.1))
    }
}
