pub(crate) mod avg;
pub(crate) mod sum;

use crate::Value;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Bucket {
    pub start: u128,
    pub end: u128,
    pub value: Value,
    pub len: usize,
}

impl Bucket {
    pub fn middle(&self) -> u128 {
        let diff = self.end - self.start;
        self.start + diff / 2
    }
}
