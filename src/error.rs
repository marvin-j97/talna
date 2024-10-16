/// Error type
#[derive(Debug)]
pub enum Error {
    /// An IO error.
    Io(std::io::Error),

    /// Error in storage engine.
    Storage(fjall::Error),

    /// An invalid filter query was used.
    InvalidQuery,

    /// An invalid metric name was used.
    ///
    /// Characters supported: a-z A-Z 0-9 . _
    InvalidMetricName,
}

impl From<fjall::Error> for Error {
    fn from(value: fjall::Error) -> Self {
        Self::Storage(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(e) => {
                write!(f, "{e}",)
            }
            Self::Io(e) => {
                write!(f, "{e}",)
            }
            Self::InvalidQuery => {
                write!(f, "InvalidQuery",)
            }
            Self::InvalidMetricName => {
                write!(f, "InvalidMetricName",)
            }
        }
    }
}

impl std::error::Error for Error {}

/// Result helper type
pub type Result<T> = std::result::Result<T, Error>;
