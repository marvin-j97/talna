#[derive(Clone)]
pub struct Average;

impl super::stream::Aggregation for Average {
    fn finish(bucket: &super::Bucket) -> crate::Value {
        bucket.value / bucket.len as crate::Value
    }
}
