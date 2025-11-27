use std::fs;
use std::path::{Path, PathBuf};

use crate::error::StoreResult;

pub(crate) const JOURNAL_CHUNK_PREFIX: &str = "journal.";
pub(crate) const JOURNAL_CHUNK_SUFFIX: &str = ".bin";
const JOURNAL_CHUNK_DIGITS: usize = 8;

pub(crate) fn chunk_file_name(chunk_id: u32) -> String {
    format!(
        "{prefix}{chunk:0width$}{suffix}",
        prefix = JOURNAL_CHUNK_PREFIX,
        chunk = chunk_id,
        width = JOURNAL_CHUNK_DIGITS,
        suffix = JOURNAL_CHUNK_SUFFIX
    )
}

pub(crate) fn chunk_file_path(dir: &Path, chunk_id: u32) -> PathBuf {
    dir.join(chunk_file_name(chunk_id))
}

pub(crate) fn parse_chunk_id(file_name: &str) -> Option<u32> {
    let suffix_stripped = file_name.strip_prefix(JOURNAL_CHUNK_PREFIX)?;
    let digits = suffix_stripped.strip_suffix(JOURNAL_CHUNK_SUFFIX)?;
    if digits.len() != JOURNAL_CHUNK_DIGITS || digits.chars().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u32>().ok()
}

pub(crate) fn enumerate_chunk_files(dir: &Path) -> StoreResult<Vec<(u32, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        if let Some(name) = entry.file_name().to_str() {
            if let Some(chunk_id) = parse_chunk_id(name) {
                entries.push((chunk_id, entry.path()));
            }
        }
    }

    entries.sort_by_key(|(chunk_id, _)| *chunk_id);
    Ok(entries)
}
