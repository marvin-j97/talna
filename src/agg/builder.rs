use super::{stream::Aggregation, Bucket};
use crate::{
    agg::stream::Aggregator,
    db::{SeriesStream, StreamItem},
    merge::Merger,
    Database,
};
use std::marker::PhantomData;

pub struct GroupedAggregation<'a, A, I>(crate::HashMap<String, Aggregator<'a, A, I>>)
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

pub struct Builder<'a, A: Aggregation> {
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

impl<'a, A: Aggregation> Clone for Builder<'a, A> {
    fn clone(&self) -> Self {
        Self {
            phantom: PhantomData,
            database: self.database,
            metric_name: self.metric_name,
            filter_expr: self.filter_expr,
            group_by: self.group_by,
            bucket_width: self.bucket_width,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
        }
    }
}

impl<'a, A: Aggregation> Builder<'a, A> {
    /// Bucket "width" in nanoseconds
    pub fn granularity(mut self, bucket: u128) -> Self {
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

    #[allow(clippy::option_if_let_else)]
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> crate::Result<
        GroupedAggregation<'a, A, Merger<Box<dyn Iterator<Item = crate::Result<StreamItem>>>>>,
    > {
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
                (group, Aggregator::new(self.clone(), merger))
            })
            .collect();

        Ok(GroupedAggregation(map))
    }
}
