use super::Bucket;
use crate::{
    db::{Database, QueryStream},
    tag_sets::TagSets,
    Value,
};
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
            &self.database.tag_sets,
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
        tag_sets: &TagSets,
        stream: QueryStream,
        group_by: &str,
        bucket_width: u128,
    ) -> fjall::Result<crate::HashMap<String, Vec<Bucket>>> {
        let tagsets = stream
            .affected_series
            .iter()
            .map(|&x| Ok((x, tag_sets.get(x)?)))
            .collect::<fjall::Result<crate::HashMap<_, _>>>()?;

        let mut result: crate::HashMap<String, Vec<Bucket>> = crate::HashMap::default();

        for data_point in stream.reader {
            let data_point = data_point?;

            let Some(tagset) = tagsets.get(&data_point.series_id) else {
                continue;
            };
            let Some(group) = tagset.get(group_by) else {
                continue;
            };

            result
                .entry(group.clone())
                .and_modify(|buckets| {
                    // NOTE: Cannot be empty
                    let last = buckets.last_mut().unwrap();

                    if (last.end - data_point.ts) < bucket_width {
                        // Add to bucket
                        last.len += 1;
                        last.value += data_point.value;
                    } else {
                        // Insert next bucket
                        buckets.push(Bucket {
                            end: data_point.ts,
                            len: 1,
                            value: data_point.value,
                        });
                    }
                })
                .or_insert_with(|| {
                    vec![Bucket {
                        end: data_point.ts,
                        len: 1,
                        value: data_point.value,
                    }]
                });
        }

        // TODO: can probably have the bucketing process be another struct
        // and the aggregation just an configuration option (AVG, MAX, MIN, MEDIAN, etc)

        for buckets in result.values_mut() {
            for bucket in buckets {
                // NOTE: Do AVG
                bucket.value /= bucket.len as Value;
            }
        }

        // TODO: should probably just return bucket through .next()
        // Iterator
        // the above should become a Builder

        Ok(result)
    }
}
