use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;

use fs2::FileExt;

use crate::error::{MhinStoreError, StoreResult};

const LOCK_FILE_NAME: &str = "rollblock.lock";

pub struct StoreLockGuard {
    file: File,
}

impl StoreLockGuard {
    pub fn acquire(data_dir: &Path) -> StoreResult<Self> {
        // Always ensure the base directory exists so that the lock file can live alongside
        // the data folders (metadata, journal, snapshots).
        fs::create_dir_all(data_dir)?;

        let lock_path = data_dir.join(LOCK_FILE_NAME);
        let file = match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    return Err(MhinStoreError::MissingMetadata("lock file"));
                }
                return Err(err.into());
            }
        };

        let requested = "exclusive";
        let lock_result = FileExt::try_lock_exclusive(&file);

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
