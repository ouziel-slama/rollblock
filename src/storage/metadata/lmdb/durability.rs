use heed::types::{Bytes, Str};
use heed::Database;

use crate::error::StoreResult;
use crate::runtime::orchestrator::DurabilityMode;

pub(super) fn load_mode(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
) -> StoreResult<Option<DurabilityMode>> {
    let txn = env.read_txn()?;
    if let Some(bytes) = config_db.get(&txn, key)? {
        Ok(Some(bincode::deserialize(bytes)?))
    } else {
        Ok(None)
    }
}

pub(super) fn store_mode(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
    mode: &DurabilityMode,
) -> StoreResult<()> {
    let mut txn = env.write_txn()?;
    let bytes = bincode::serialize(mode)?;
    config_db.put(&mut txn, key, &bytes)?;
    txn.commit()?;
    Ok(())
}

pub(super) fn load_map_size(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
) -> StoreResult<Option<usize>> {
    let txn = env.read_txn()?;
    if let Some(bytes) = config_db.get(&txn, key)? {
        let stored: u64 = bincode::deserialize(bytes)?;
        Ok(Some(stored as usize))
    } else {
        Ok(None)
    }
}

pub(super) fn store_map_size(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
    map_size: usize,
) -> StoreResult<()> {
    let mut txn = env.write_txn()?;
    let encoded = bincode::serialize(&(map_size as u64))?;
    config_db.put(&mut txn, key, &encoded)?;
    txn.commit()?;
    Ok(())
}

pub(super) fn ensure_map_size(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
    map_size: usize,
) -> StoreResult<()> {
    if let Some(stored) = load_map_size(env, config_db, key)? {
        if stored == map_size {
            return Ok(());
        }
    }
    store_map_size(env, config_db, key, map_size)
}
