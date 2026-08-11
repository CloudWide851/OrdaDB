use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ordadb_types::{DbError, Result};
use windows_sys::Win32::Storage::FileSystem::{
    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Publishes bytes through a same-directory, write-through atomic replacement.
pub fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("atomic file destination has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("failed to create atomic file destination", error))?;
    let temporary = temporary_path(path);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| io_error("failed to create atomic temporary file", error))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error("failed to synchronize atomic temporary file", error))?;
        move_file_replace(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("tmp-{}-{sequence}", std::process::id()))
}

fn move_file_replace(source: &Path, destination: &Path) -> Result<()> {
    let source = wide(source);
    let destination = wide(destination);
    // SAFETY: both buffers are live NUL-terminated UTF-16 paths. The
    // temporary file is created beside the destination, so the replacement is
    // same-volume and MOVEFILE_WRITE_THROUGH makes publication durable.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(io_error(
            "failed to publish atomic file",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomically_creates_and_replaces_a_file_without_leaving_temporary_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("settings.json");

        write_file_atomic(&path, b"first").expect("create");
        write_file_atomic(&path, b"second").expect("replace");

        assert_eq!(fs::read(&path).expect("read"), b"second");
        assert_eq!(
            fs::read_dir(directory.path())
                .expect("entries")
                .filter_map(std::result::Result::ok)
                .count(),
            1
        );
    }
}
