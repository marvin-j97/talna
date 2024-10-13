pub(crate) mod avg;

use crate::Value;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Bucket {
    end: u128,
    value: Value,
    len: usize,
}
