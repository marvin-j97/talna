mod avg;
mod builder;
mod count;
mod group;
mod max;
mod min;
mod stream;
mod sum;

use crate::Value;

pub use avg::Average;
pub use builder::Builder;
pub use count::Count;
pub use group::GroupedAggregation;
pub use max::Max;
pub use min::Min;
pub use sum::Sum;

/// A data point which spans some time
#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub struct Bucket {
    /// The lower time bound (nanosecond timestamp)
    pub start: u128,

    /// The upper time bound (nanosecond timestamp)
    pub end: u128,

    /// The aggregated value
    pub value: Value,

    /// The amount of raw data points that were contained in this bucket
    pub len: usize,
}

impl Bucket {
    /// Calculates the middle timestamp.
    #[must_use]
    pub fn middle(&self) -> u128 {
        let diff = self.end - self.start;
        self.start + diff / 2
    }
}
