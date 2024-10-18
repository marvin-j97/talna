use crate::db::StreamItem;
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
            Err(e) => return Some(Err(crate::Error::from(e))),
        }
    };
}

pub struct Merger<I: Iterator<Item = crate::Result<StreamItem>>> {
    readers: Vec<I>,
    heap: BinaryHeap<HeapItem>,
    is_initialized: bool,
}

impl<I: Iterator<Item = crate::Result<StreamItem>>> Merger<I> {
    pub fn new(readers: Vec<I>) -> Self {
        Self {
            readers,
            heap: BinaryHeap::default(),
            is_initialized: false,
        }
    }

    fn advance(&mut self, idx: usize) -> crate::Result<()> {
        if let Some(item) = self.readers.get_mut(idx).expect("should exist").next() {
            self.heap.push(HeapItem(idx, item?));
        }
        Ok(())
    }
}

impl<I: Iterator<Item = crate::Result<StreamItem>>> Iterator for Merger<I> {
    type Item = crate::Result<StreamItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            for i in 0..self.readers.len() {
                fail_iter!(self.advance(i));
            }
            self.is_initialized = true;
        }

        let head = self.heap.pop()?;

        fail_iter!(self.advance(head.0));

        Some(Ok(head.1))
    }
}
