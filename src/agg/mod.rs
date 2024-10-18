mod avg;
mod builder;
mod count;
mod max;
mod min;
mod stream;
mod sum;

use crate::Value;

pub use avg::Average;
pub use builder::Builder;
pub use count::Count;
pub use max::Max;
pub use min::Min;
pub use sum::Sum;

#[derive(Copy, Clone, Default, Debug, PartialEq)]
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
