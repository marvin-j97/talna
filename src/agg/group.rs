use super::{stream::Aggregation, Bucket};
use crate::{agg::stream::Aggregator, db::StreamItem};

/// A dictionary of aggregators that can individually be advanced on demand.
///
/// Call `.collect()` to read all aggregators into one result.
pub struct GroupedAggregation<'a, A, I>(pub(crate) crate::HashMap<String, Aggregator<'a, A, I>>)
where
    A: Aggregation,
    I: Iterator<Item = crate::Result<StreamItem>>;

impl<'a, A, I> std::ops::Deref for GroupedAggregation<'a, A, I>
where
    A: Aggregation,
    I: Iterator<Item = crate::Result<StreamItem>>,
{
    type Target = crate::HashMap<String, Aggregator<'a, A, I>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, A, I> IntoIterator for GroupedAggregation<'a, A, I>
where
    A: Aggregation,
    I: Iterator<Item = crate::Result<StreamItem>>,
{
    type Item = (String, Aggregator<'a, A, I>);
    type IntoIter = std::collections::hash_map::IntoIter<String, Aggregator<'a, A, I>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, A, I> GroupedAggregation<'a, A, I>
where
    A: Aggregation,
    I: Iterator<Item = crate::Result<StreamItem>>,
{
    /// Consumes all groups, returning a dictionary of time series data,
    /// mapping each group to a vector of data points (`Bucket`).
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurred.
    pub fn collect(self) -> crate::Result<crate::HashMap<String, Vec<Bucket>>> {
        let mut map =
            crate::HashMap::with_capacity_and_hasher(self.0.len(), rustc_hash::FxBuildHasher);

        for (group, aggregator) in self.0 {
            let mut buckets = vec![];

            for bucket in aggregator {
                buckets.push(bucket?);
            }

            map.insert(group, buckets);
        }

        Ok(map)
    }
}
