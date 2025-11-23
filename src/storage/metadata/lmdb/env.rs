use std::path::{Path, PathBuf};

use heed::byteorder::BigEndian;
use heed::types::{Bytes, SerdeBincode, Str, U64};
use heed::{Database, Env, EnvOpenOptions, Error as HeedError};

use crate::error::StoreResult;
use crate::types::{BlockId, JournalMeta};

pub(super) const DEFAULT_MAP_SIZE: usize = 2 << 30;

pub(super) struct EnvHandles {
    pub(super) env: Env,
    pub(super) path: PathBuf,
    pub(super) state_db: Database<Str, SerdeBincode<BlockId>>,
    pub(super) config_db: Database<Str, Bytes>,
    pub(super) journal_offsets_db: Database<U64<BigEndian>, SerdeBincode<JournalMeta>>,
}

pub(super) fn open_rw(path: &Path, map_size: usize) -> StoreResult<EnvHandles> {
    std::fs::create_dir_all(path)?;

    let mut options = EnvOpenOptions::new();
    options.map_size(map_size);
    options.max_dbs(8);

    let env = unsafe {
        match options.open(path) {
            Ok(env) => env,
            Err(HeedError::BadOpenOptions { env, .. }) => env,
            Err(err) => {
                tracing::error!(
                    path = ?path,
                    map_size,
                    ?err,
                    "Failed to open LMDB environment (read/write)"
                );
                return Err(err.into());
            }
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
            if let (Some(state_db), Some(config_db), Some(journal_offsets_db)) =
                (state_db, config_db, journal_offsets_db)
            {
                txn.commit()?;
                Some((state_db, config_db, journal_offsets_db))
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

    let (state_db, config_db, journal_offsets_db) = if let Some(dbs) = existing_dbs {
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
        let journal_offsets_db = env.create_database::<U64<BigEndian>, SerdeBincode<JournalMeta>>(
            &mut txn,
            Some("journal_offsets"),
        )?;
        txn.commit()?;
        (state_db, config_db, journal_offsets_db)
    };

    Ok(EnvHandles {
        env,
        path: path.to_path_buf(),
        state_db,
        config_db,
        journal_offsets_db,
    })
}
