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
