use crate::Database;
use fjall::{BlockCache, TxKeyspace};
use std::{path::Path, sync::Arc};

/// Builder for [`Database`].
pub struct Builder {
    cache_size_mib: u64,
    hyper_mode: bool,
}

// TODO: 1.0.0 prefix bloom filters would be *really* nice
// if we can make lsm-tree optimize ranges that have a common prefix

impl Builder {
    pub(crate) fn new() -> Self {
        Self {
            cache_size_mib: 32,
            hyper_mode: false,
        }
    }

    /// Sets the cache size in MiB.
    ///
    /// Default = 32 MiB
    #[must_use]
    pub fn cache_size_mib(mut self, mib: u64) -> Self {
        self.cache_size_mib = mib;
        self
    }

    /// If `true`, writes become faster by skipping the `write()` syscall to OS buffers.
    ///
    /// However, writes are then not application-crash safe.
    #[must_use]
    pub fn hyper_mode(mut self, enabled: bool) -> Self {
        self.hyper_mode = enabled;
        self
    }

    /// Opens or recovers a time series database.
    ///
    /// If you have a keyspace already in your application, you may
    /// want to use `from_keyspace` instead.
    ///
    /// # Errors
    ///
    /// Returns error if an I/O error occurred.
    pub fn open<P: AsRef<Path>>(self, path: P) -> crate::Result<crate::Database> {
        let keyspace = fjall::Config::new(path)
            .block_cache(Arc::new(BlockCache::with_capacity_bytes(
                self.cache_size_mib * 1_024 * 1_024,
            )))
            .open_transactional()?;

        Database::from_keyspace(keyspace, self.hyper_mode)
    }

    /// Uses an existing `fjall` keyspace to open a time series database.
    ///
    /// Partitions are prefixed with `_talna#` to avoid name clashes with other applications.
    ///
    /// # Errors
    ///
    /// Returns error if an I/O error occurred.
    pub fn open_in_keyspace(self, keyspace: TxKeyspace) -> crate::Result<crate::Database> {
        Database::from_keyspace(keyspace, self.hyper_mode)
    }
}
