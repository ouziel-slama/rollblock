use heed::types::{Bytes, Str};
use heed::Database;

use crate::error::StoreResult;
use crate::storage::metadata::ShardLayout;

pub(super) fn load(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
) -> StoreResult<Option<ShardLayout>> {
    let txn = env.read_txn()?;
    if let Some(bytes) = config_db.get(&txn, key)? {
        Ok(Some(bincode::deserialize(bytes)?))
    } else {
        Ok(None)
    }
}

pub(super) fn store(
    env: &heed::Env,
    config_db: &Database<Str, Bytes>,
    key: &str,
    layout: &ShardLayout,
) -> StoreResult<()> {
    let mut txn = env.write_txn()?;
    let bytes = bincode::serialize(layout)?;
    config_db.put(&mut txn, key, &bytes)?;
    txn.commit()?;
    Ok(())
}
