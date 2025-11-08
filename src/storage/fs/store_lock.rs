use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;

use fs2::FileExt;

use crate::error::{MhinStoreError, StoreResult};
use crate::facade::StoreMode;

const LOCK_FILE_NAME: &str = "rollblock.lock";

pub struct StoreLockGuard {
    file: File,
}

impl StoreLockGuard {
    pub fn acquire(data_dir: &Path, mode: StoreMode) -> StoreResult<Self> {
        // Ensure the base directory exists so we have a place for the lock file.
        if mode.is_read_write() {
            fs::create_dir_all(data_dir)?;
        }

        let lock_path = data_dir.join(LOCK_FILE_NAME);
        let file = match OpenOptions::new()
            .create(mode.is_read_write())
            .read(true)
            .write(mode.is_read_write())
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(err) if !mode.is_read_write() && err.kind() == ErrorKind::NotFound => {
                return Err(MhinStoreError::MissingMetadata("lock file"));
            }
            Err(err) => return Err(err.into()),
        };

        let requested = match mode {
            StoreMode::ReadWrite => "read-write",
            StoreMode::ReadOnly => "read-only",
        };

        let lock_result = match mode {
            StoreMode::ReadWrite => FileExt::try_lock_exclusive(&file),
            StoreMode::ReadOnly => FileExt::try_lock_shared(&file),
        };

        if lock_result.is_err() {
            return Err(MhinStoreError::DataDirLocked {
                path: lock_path,
                requested,
            });
        }

        Ok(Self { file })
    }
}

impl Drop for StoreLockGuard {
    fn drop(&mut self) {
        // Attempt to release the lock; ignore errors because there is not much
        // we can do during drop and the OS will release the lock when the file
        // descriptor is closed.
        let _ = self.file.unlock();
    }
}
