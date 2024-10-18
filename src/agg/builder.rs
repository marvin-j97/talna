use super::{stream::Aggregation, Bucket};
use crate::{agg::stream::Aggregator, db::SeriesStream, merge::Merger, Database};
use std::marker::PhantomData;

pub struct GroupedAggregation<'a, A: Aggregation + Clone>(
    crate::HashMap<String, crate::agg::stream::Aggregator<'a, A>>,
);

impl<'a, A: Aggregation + Clone> std::ops::Deref for GroupedAggregation<'a, A> {
    type Target = crate::HashMap<String, crate::agg::stream::Aggregator<'a, A>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, A: Aggregation + Clone> IntoIterator for GroupedAggregation<'a, A> {
    type Item = (String, crate::agg::stream::Aggregator<'a, A>);
    type IntoIter =
        std::collections::hash_map::IntoIter<String, crate::agg::stream::Aggregator<'a, A>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, A: Aggregation + Clone> GroupedAggregation<'a, A> {
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

#[derive(Clone)]
pub struct Builder<'a, A: Aggregation + Clone> {
    pub(crate) phantom: PhantomData<A>,

    /// The database to access
    pub(crate) database: &'a Database,

    /// Name of metric to scan (e.g. `cpu_usage`)
    pub(crate) metric_name: &'a str,

    /// Filter expression to filter out data points
    pub(crate) filter_expr: &'a str,

    /// Group time series by tag (`host`)
    pub(crate) group_by: &'a str,

    /// Bucket "width" in nanoseconds
    pub(crate) bucket_width: u128,

    /// Minimum timestamp to scan
    pub(crate) min_ts: Option<u128>,

    /// Maximum timestamp to scan
    pub(crate) max_ts: Option<u128>,
}

impl<'a, A: Aggregation + Clone> Builder<'a, A> {
    /// Bucket "width" in nanoseconds
    pub fn bucket(mut self, bucket: u128) -> Self {
        self.bucket_width = bucket;
        self
    }

    /// Sets the filter expression to filter out data points
    ///
    /// e.g. `env:prod AND service:db`
    pub fn filter(mut self, filter_expr: &'a str) -> Self {
        self.filter_expr = filter_expr;
        self
    }

    pub fn start(mut self, ts: u128) -> Self {
        self.min_ts = Some(ts);
        self
    }

    pub fn end(mut self, ts: u128) -> Self {
        self.max_ts = Some(ts);
        self
    }

    pub fn build(self) -> crate::Result<GroupedAggregation<'a, A>> {
        use std::ops::Bound;

        let eligible_series = self.database.start_query(
            self.metric_name,
            self.filter_expr,
            (
                match self.min_ts {
                    Some(ts) => Bound::Included(ts),
                    None => Bound::Unbounded,
                },
                match self.max_ts {
                    Some(ts) => Bound::Included(ts),
                    None => Bound::Unbounded,
                },
            ),
        )?;

        let mut map: crate::HashMap<String, Vec<SeriesStream>> = crate::HashMap::default();

        for series in eligible_series {
            let Some(group) = series.tags.get(self.group_by) else {
                continue;
            };

            if let Some(vec) = map.get_mut(group) {
                vec.push(series);
            } else {
                map.insert(group.to_string(), vec![series]);
            }
        }

        let map = map
            .into_iter()
            .map(|(group, serieses)| {
                let merger = Merger::new(serieses.into_iter().map(|x| x.reader).collect());
                (group, Aggregator::new(self.clone(), Box::new(merger)))
            })
            .collect();

        Ok(GroupedAggregation(map))
    }
}
