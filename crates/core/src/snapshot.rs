//! Generic JSON snapshot persistence with atomic writes.

use std::{
    fs, io,
    path::Path,
    sync::{Mutex, OnceLock},
};

use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("reading snapshot {path}")]
    Read {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("writing snapshot {path}")]
    Write {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("serializing snapshot")]
    Serialize(#[source] serde_json::Error),
    #[error("parsing snapshot {path}")]
    Parse {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Serializes writes across the process so concurrent renames never interleave.
fn write_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Load a snapshot, returning `None` when the file does not exist.
///
/// # Errors
/// Returns [`SnapshotError`] on IO failures other than a missing file, or when
/// the file contents fail to parse.
pub fn load<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, SnapshotError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(SnapshotError::Read {
                path: path.display().to_string(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| SnapshotError::Parse {
            path: path.display().to_string(),
            source,
        })
}

/// Atomically write a snapshot: serialize to a temp file in the same directory,
/// then rename over the target.
///
/// # Errors
/// Returns [`SnapshotError`] on serialization or IO failure.
pub fn store<T: Serialize>(path: &Path, value: &T) -> Result<(), SnapshotError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(SnapshotError::Serialize)?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = path.with_extension("tmp");

    let _guard = write_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let map_write = |source: io::Error| SnapshotError::Write {
        path: path.display().to_string(),
        source,
    };

    fs::create_dir_all(dir).map_err(map_write)?;
    fs::write(&tmp, &bytes).map_err(map_write)?;
    fs::rename(&tmp, path).map_err(|source| {
        let _ = fs::remove_file(&tmp);
        map_write(source)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn temp_dir() -> std::path::PathBuf {
        let base = std::env::temp_dir().join(format!(
            "weave-snapshot-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn store_then_load_round_trips() {
        let dir = temp_dir();
        let path = dir.join("round-trip.json");
        let value: BTreeMap<String, u32> =
            BTreeMap::from([("a".to_string(), 1), ("b".to_string(), 2)]);

        store(&path, &value).unwrap();
        let loaded: Option<BTreeMap<String, u32>> = load(&path).unwrap();
        assert_eq!(loaded, Some(value));
    }

    #[test]
    fn load_missing_file_is_none() {
        let dir = temp_dir();
        let path = dir.join("does-not-exist.json");
        let loaded: Option<BTreeMap<String, u32>> = load(&path).unwrap();
        assert_eq!(loaded, None);
    }

    #[test]
    fn load_corrupt_json_is_error() {
        let dir = temp_dir();
        let path = dir.join("corrupt.json");
        fs::write(&path, b"{ not json").unwrap();
        let result: Result<Option<BTreeMap<String, u32>>, _> = load(&path);
        assert!(matches!(result, Err(SnapshotError::Parse { .. })));
    }

    #[test]
    fn store_leaves_no_tmp_file() {
        let dir = temp_dir();
        let path = dir.join("no-tmp.json");
        let value: BTreeMap<String, u32> = BTreeMap::from([("x".to_string(), 9)]);

        store(&path, &value).unwrap();

        let leftover: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "tmp"))
            .collect();
        assert!(leftover.is_empty(), "no .tmp files should remain");
    }
}
