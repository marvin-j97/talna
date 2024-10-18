use super::{builder::Builder, Bucket};
use crate::{db::StreamItem, Value};
use std::marker::PhantomData;

/// Defines an aggregation.
///
/// - `transform` defines what to do with each value (default: Add)
///
/// - `finish` can transform the result value (default: Identity)
pub trait Aggregation {
    fn init(value: Value) -> Value {
        value
    }

    fn transform(accu: Value, x: Value) -> Value {
        accu + x
    }

    fn finish(bucket: &Bucket) -> Value {
        bucket.value
    }
}

/// A streaming aggregator
///
/// Takes in a stream of data points, and emits aggregated buckets.
pub struct Aggregator<'a, A: Aggregation + Clone> {
    config: Builder<'a, A>,
    bucket: Bucket,
    reader: Box<dyn Iterator<Item = crate::Result<StreamItem>>>,
    phantom: PhantomData<A>,
}

impl<'a, A: Aggregation + Clone> Aggregator<'a, A> {
    pub fn new(
        builder: Builder<'a, A>,
        reader: Box<dyn Iterator<Item = crate::Result<StreamItem>>>,
    ) -> Self {
        Self {
            config: builder,
            bucket: Bucket::default(),
            reader,
            phantom: PhantomData,
        }
    }
}

impl<'a, A: Aggregation + Clone> Iterator for Aggregator<'a, A> {
    type Item = crate::Result<Bucket>;

    fn next(&mut self) -> Option<Self::Item> {
        for data_point in self.reader.by_ref() {
            let data_point = match data_point {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };

            if self.bucket.len == 0 {
                // NOTE: Initialize bucket
                self.bucket.len = 1;
                self.bucket.start = data_point.ts;
                self.bucket.end = data_point.ts;
                self.bucket.value = A::init(data_point.value);
                continue;
            }

            if (self.bucket.end - data_point.ts) <= self.config.bucket_width {
                // NOTE: Add to bucket
                self.bucket.len += 1;
                self.bucket.value = A::transform(self.bucket.value, data_point.value);
                self.bucket.start = data_point.ts;
            } else {
                // NOTE: Return bucket, and initialize new empty bucket
                let mut bucket = std::mem::take(&mut self.bucket);
                bucket.value = A::finish(&bucket);
                return Some(Ok(bucket));
            }
        }

        if self.bucket.len > 0 {
            // NOTE: Return last bucket
            let mut bucket = std::mem::take(&mut self.bucket);
            bucket.value = A::finish(&bucket);
            Some(Ok(bucket))
        } else {
            None
        }
    }
}
