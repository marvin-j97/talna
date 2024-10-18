use crate::TagSet;

#[doc(hidden)]
pub struct SeriesKey;

impl SeriesKey {
    #[doc(hidden)]
    #[must_use]
    pub fn allocate_string_for_tags(tags: &TagSet, extra_len: usize) -> String {
        let total_len = tags
            .iter()
            .map(|(key, value)| key.len() + value.len() + 1) // +1 for the ':' between key and value
            .sum::<usize>()
            + tags.len().saturating_sub(1); // Add space for the semicolons

        String::with_capacity(total_len + extra_len)
    }

    #[doc(hidden)]
    pub fn join_tags(buf: &mut String, tags: &TagSet) {
        let mut tags = tags.iter().collect::<Vec<_>>();
        tags.sort();

        for (idx, (key, value)) in tags.iter().enumerate() {
            if idx > 0 {
                buf.push(';');
            }
            buf.push_str(key);
            buf.push(':');
            buf.push_str(value);
        }
    }

    #[must_use]
    pub fn format(metric: &str, tags: &TagSet) -> String {
        let mut str = Self::allocate_string_for_tags(tags, metric.len() + 1);
        str.push_str(metric);
        str.push('#');
        Self::join_tags(&mut str, tags);
        str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tagset;

    #[test_log::test]
    fn create_series_key() {
        assert_eq!(
            "cpu.total#service:web",
            SeriesKey::format("cpu.total", tagset!("service" => "web")),
        );
    }

    #[test_log::test]
    fn create_series_key_2() {
        assert_eq!(
            "cpu.total#host:i-187;service:web",
            SeriesKey::format(
                "cpu.total",
                tagset!(
                        "service" => "web",
                        "host" => "i-187",
                ),
            ),
        );
    }

    #[test_log::test]
    fn create_series_key_3() {
        assert_eq!(
            "cpu.total#env:dev;host:i-187;service:web",
            SeriesKey::format(
                "cpu.total",
                tagset!(
                    "service" => "web",
                    "host" => "i-187",
                    "env" => "dev"
                ),
            ),
        );
    }
}
