const METRICS_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyz_.";

/// A metric's name.
///
/// Characters supported: a-z A-Z 0-9 . _
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash, Debug)]
pub struct MetricName<'a>(&'a str);

impl std::fmt::Display for MetricName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> TryFrom<&'a str> for MetricName<'a> {
    type Error = ();

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if value.chars().any(|c| !METRICS_NAME_CHARS.contains(c)) {
            Err(())
        } else {
            Ok(Self(value))
        }
    }
}

impl<'a> std::ops::Deref for MetricName<'a> {
    type Target = &'a str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for MetricName<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
