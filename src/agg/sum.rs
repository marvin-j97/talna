use super::Bucket;
use crate::db::{Database, SeriesStream};
use std::ops::Bound;

pub struct Aggregator<'a> {
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

impl<'a> Aggregator<'a> {
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

    pub fn run(self) -> fjall::Result<crate::HashMap<String, Vec<Bucket>>> {
        Self::raw(
            self.database.start_query(
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
            )?,
            self.group_by,
            self.bucket_width,
        )
    }

    pub(crate) fn raw(
        streams: Vec<SeriesStream>,
        group_by: &str,
        bucket_width: u128,
    ) -> fjall::Result<crate::HashMap<String, Vec<Bucket>>> {
        let mut result: crate::HashMap<String, Vec<Bucket>> = crate::HashMap::default();

        // TODO: can probably have the bucketing process be another struct
        // and the aggregation just an configuration option (SUM, AVG, MAX, MIN, etc)

        // TODO: need to group all streams with same group
        // TODO: into a MergeReader before
        // TODO: then process each group

        for mut stream in streams {
            let Some(group) = stream.tags.get(group_by) else {
                continue;
            };

            let mut buckets = vec![];

            // NOTE: Initialize first bucket
            if let Some(data_point) = stream.reader.next() {
                let data_point = data_point?;

                buckets.push(Bucket {
                    start: data_point.ts,
                    end: data_point.ts,
                    len: 1,
                    value: data_point.value,
                });
            }

            // NOTE: Read rest of data points
            for data_point in stream.reader {
                let data_point = data_point?;

                // NOTE: Cannot be empty
                let last = buckets.last_mut().unwrap();

                if (last.end - data_point.ts) <= bucket_width {
                    // Add to bucket
                    last.len += 1;
                    last.value += data_point.value;
                    last.start = data_point.ts;
                } else {
                    // Insert next bucket
                    buckets.push(Bucket {
                        start: data_point.ts,
                        end: data_point.ts,
                        len: 1,
                        value: data_point.value,
                    });
                }
            }

            result.insert(group.clone(), buckets);
        }

        // TODO: should probably just return bucket through .next()
        // Iterator
        // the above should become a Builder

        Ok(result)
    }
}
