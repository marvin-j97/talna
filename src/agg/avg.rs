use crate::{
    db::{Database, QueryStream},
    tag_sets::TagSets,
};
use std::{collections::HashMap, ops::Bound};

#[derive(Debug)]
pub struct Bucket {
    start: u128,
    value: f64,
    len: usize,
}

pub struct Aggregator<'a> {
    pub(crate) database: &'a Database,
    pub(crate) metric_name: &'a str,
    pub(crate) filter_expr: &'a str,
    pub(crate) group_by: &'a str,
    pub(crate) bucket_width: u128,
}

impl<'a> Aggregator<'a> {
    pub fn bucket(mut self, bucket: u128) -> Self {
        self.bucket_width = bucket;
        self
    }

    pub fn filter(mut self, filter_expr: &'a str) -> Self {
        self.filter_expr = filter_expr;
        self
    }

    pub fn run(self) -> fjall::Result<HashMap<String, Vec<Bucket>>> {
        Self::raw(
            &self.database.tag_sets,
            self.database.start_query(
                self.metric_name,
                self.filter_expr,
                (Bound::Unbounded, Bound::Unbounded), // TODO: bounds
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
    ) -> fjall::Result<HashMap<String, Vec<Bucket>>> {
        let tagsets = stream
            .affected_series
            .iter()
            .map(|x| Ok((*x, tag_sets.get(*x)?)))
            .collect::<fjall::Result<HashMap<_, _>>>()?;

        let mut result: HashMap<String, Vec<Bucket>> = HashMap::new();

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

                    if (last.start - data_point.ts) < bucket_width {
                        // Add to bucket
                        last.len += 1;
                        last.value += data_point.value;
                    } else {
                        // Insert next bucket
                        buckets.push(Bucket {
                            start: data_point.ts,
                            len: 1,
                            value: data_point.value,
                        });
                    }
                })
                .or_insert_with(|| {
                    vec![Bucket {
                        start: data_point.ts,
                        len: 1,
                        value: data_point.value,
                    }]
                });
        }

        for buckets in result.values_mut() {
            for bucket in buckets {
                // NOTE: Do AVG
                bucket.value /= bucket.len as f64;
            }
        }

        Ok(result)
    }
}
