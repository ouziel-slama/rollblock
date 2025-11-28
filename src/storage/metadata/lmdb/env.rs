use std::io;
use std::path::{Path, PathBuf};

use heed::byteorder::BigEndian;
use heed::types::{Bytes, SerdeBincode, Str, U64};
use heed::{Database, Env, EnvFlags, EnvOpenOptions, Error as HeedError, MdbError};

use crate::error::StoreResult;
use crate::types::{BlockId, JournalMeta};

pub(super) const DEFAULT_MAP_SIZE: usize = 2 << 30;

pub(super) struct EnvHandles {
    pub(super) env: Env,
    pub(super) path: PathBuf,
    pub(super) state_db: Database<Str, SerdeBincode<BlockId>>,
    pub(super) config_db: Database<Str, Bytes>,
    pub(super) journal_offsets_db: Database<U64<BigEndian>, SerdeBincode<JournalMeta>>,
    pub(super) gc_watermark_db: Database<Str, Bytes>,
    pub(super) snapshot_watermark_db: Database<Str, SerdeBincode<BlockId>>,
}

pub(super) fn open_rw(path: &Path, map_size: usize) -> StoreResult<EnvHandles> {
    std::fs::create_dir_all(path)?;

    let env = match open_env_with_fallback(path, map_size) {
        Ok(env) => env,
        Err(err) => {
            tracing::error!(
                path = ?path,
                map_size,
                ?err,
                "Failed to open LMDB environment (read/write)"
            );
            return Err(err.into());
        }
    };

    let existing_dbs = match env.read_txn() {
        Ok(txn) => {
            let state_db = env.open_database::<Str, SerdeBincode<BlockId>>(&txn, Some("state"))?;
            let config_db = env.open_database::<Str, Bytes>(&txn, Some("config"))?;
            let journal_offsets_db = env
                .open_database::<U64<BigEndian>, SerdeBincode<JournalMeta>>(
                    &txn,
                    Some("journal_offsets"),
                )?;
            let gc_watermark_db = env.open_database::<Str, Bytes>(&txn, Some("gc_watermark"))?;
            let snapshot_watermark_db =
                env.open_database::<Str, SerdeBincode<BlockId>>(&txn, Some("snapshot_watermark"))?;
            if let (
                Some(state_db),
                Some(config_db),
                Some(journal_offsets_db),
                Some(gc_watermark_db),
                Some(snapshot_watermark_db),
            ) = (
                state_db,
                config_db,
                journal_offsets_db,
                gc_watermark_db,
                snapshot_watermark_db,
            ) {
                txn.commit()?;
                Some((
                    state_db,
                    config_db,
                    journal_offsets_db,
                    gc_watermark_db,
                    snapshot_watermark_db,
                ))
            } else {
                None
            }
        }
        Err(err) => {
            tracing::warn!(
                path = ?path,
                ?err,
                "Failed to open read txn when probing metadata databases; will attempt creation"
            );
            None
        }
    };

    let (state_db, config_db, journal_offsets_db, gc_watermark_db, snapshot_watermark_db) =
        if let Some(dbs) = existing_dbs {
            dbs
        } else {
            let mut txn = match env.write_txn() {
                Ok(txn) => txn,
                Err(err) => {
                    tracing::error!(
                        path = ?path,
                        ?err,
                        "Failed to start write txn for metadata init"
                    );
                    return Err(err.into());
                }
            };
            let state_db =
                env.create_database::<Str, SerdeBincode<BlockId>>(&mut txn, Some("state"))?;
            let config_db = env.create_database::<Str, Bytes>(&mut txn, Some("config"))?;
            let journal_offsets_db = env
                .create_database::<U64<BigEndian>, SerdeBincode<JournalMeta>>(
                    &mut txn,
                    Some("journal_offsets"),
                )?;
            let gc_watermark_db =
                env.create_database::<Str, Bytes>(&mut txn, Some("gc_watermark"))?;
            let snapshot_watermark_db = env.create_database::<Str, SerdeBincode<BlockId>>(
                &mut txn,
                Some("snapshot_watermark"),
            )?;
            txn.commit()?;
            (
                state_db,
                config_db,
                journal_offsets_db,
                gc_watermark_db,
                snapshot_watermark_db,
            )
        };

    Ok(EnvHandles {
        env,
        path: path.to_path_buf(),
        state_db,
        config_db,
        journal_offsets_db,
        gc_watermark_db,
        snapshot_watermark_db,
    })
}

const EPERM_CODE: i32 = 1;

fn open_env_with_fallback(path: &Path, map_size: usize) -> Result<Env, HeedError> {
    match try_open_env(path, map_size, EnvFlags::empty()) {
        Ok(env) => Ok(env),
        Err(err) if should_retry_without_locking(&err) => {
            tracing::warn!(
                path = ?path,
                map_size,
                "LMDB file locking denied; retrying without file locks (MDB_NOLOCK)"
            );
            try_open_env(path, map_size, EnvFlags::NO_LOCK)
        }
        Err(err) => Err(err),
    }
}

fn try_open_env(path: &Path, map_size: usize, flags: EnvFlags) -> Result<Env, HeedError> {
    let mut options = EnvOpenOptions::new();
    options.map_size(map_size);
    options.max_dbs(12);
    if !flags.is_empty() {
        // SAFETY: We only opt into NO_LOCK when the platform refuses to honor LMDB locks.
        unsafe {
            options.flags(flags);
        }
    }

    unsafe {
        match options.open(path) {
            Ok(env) => Ok(env),
            Err(HeedError::BadOpenOptions { env, .. }) => Ok(env),
            Err(err) => Err(err),
        }
    }
}

fn should_retry_without_locking(err: &HeedError) -> bool {
    matches!(
        err,
        HeedError::Io(io_err) if io_err.kind() == io::ErrorKind::PermissionDenied
    ) || matches!(err, HeedError::Mdb(MdbError::Other(code)) if *code == EPERM_CODE)
}
