use std::{
    collections::BTreeSet,
    env,
    ffi::OsString,
    fmt::{Debug, Formatter},
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::windows::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use ordadb_types::{DbError, Result};

const ORACLE_CLIENT_DLL: &str = "oci.dll";
const AMD64_PE_MACHINE: u16 = 0x8664;
const I386_PE_MACHINE: u16 = 0x014c;
const MAX_ENVIRONMENT_UNITS: usize = 32_767;
const MAX_CANDIDATE_DIRECTORIES: usize = 128;
const MAX_PE_HEADER_OFFSET: u64 = 16 * 1024 * 1024;

pub struct OracleClientLocation {
    directory: PathBuf,
    candidates_examined: usize,
}

impl OracleClientLocation {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub const fn candidates_examined(&self) -> usize {
        self.candidates_examined
    }
}

impl Debug for OracleClientLocation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OracleClientLocation")
            .field("directory", &"<redacted>")
            .field("candidates_examined", &self.candidates_examined)
            .finish()
    }
}

pub fn discover_amd64_oracle_client(
    helper_directory: Option<&Path>,
) -> Result<OracleClientLocation> {
    let mut directories = Vec::new();
    if let Some(directory) = helper_directory {
        directories.push(directory.to_path_buf());
    }
    if let Some(home) = bounded_environment("ORACLE_HOME")? {
        let home = PathBuf::from(home);
        directories.push(home.join("bin"));
        directories.push(home);
    }
    if let Some(path) = bounded_environment("PATH")? {
        directories.extend(env::split_paths(&path).take(MAX_CANDIDATE_DIRECTORIES));
    }
    inspect_amd64_oracle_client_directories(directories)
}

pub fn inspect_amd64_oracle_client_directories(
    directories: impl IntoIterator<Item = PathBuf>,
) -> Result<OracleClientLocation> {
    let mut unique = BTreeSet::new();
    let mut candidates_examined = 0_usize;
    let mut wrong_architecture = None;
    let mut invalid_pe = false;

    for directory in directories.into_iter().take(MAX_CANDIDATE_DIRECTORIES) {
        if !unique.insert(normalize_directory_key(&directory)) {
            continue;
        }
        let candidate = directory.join(ORACLE_CLIENT_DLL);
        if !candidate.is_file() {
            continue;
        }
        candidates_examined = candidates_examined.saturating_add(1);
        match pe_machine(&candidate) {
            Ok(AMD64_PE_MACHINE) => {
                let directory = directory.canonicalize().map_err(|_| {
                    DbError::new(
                        "58000",
                        "Oracle Instant Client directory could not be resolved",
                    )
                    .with_detail(format!(
                        "an AMD64 OCI candidate was found; candidates checked: {candidates_examined}"
                    ))
                    .with_hint(
                        "Repair the Windows x64 Oracle Instant Client installation and restart OrdaDB.",
                    )
                })?;
                return Ok(OracleClientLocation {
                    directory,
                    candidates_examined,
                });
            }
            Ok(machine) => {
                wrong_architecture.get_or_insert(machine);
            }
            Err(()) => invalid_pe = true,
        };
    }

    if let Some(machine) = wrong_architecture {
        let architecture = if machine == I386_PE_MACHINE {
            "x86"
        } else {
            "non-AMD64"
        };
        return Err(DbError::new(
            "0A000",
            "Oracle Instant Client architecture is incompatible",
        )
        .with_detail(format!(
            "detected {architecture} OCI; OrdaDB requires AMD64; candidates checked: {candidates_examined}"
        ))
        .with_hint(
            "Install the Windows x64 Oracle Instant Client and add its directory to PATH.",
        ));
    }
    if invalid_pe {
        return Err(
            DbError::new("58000", "Oracle Instant Client installation is invalid")
                .with_detail(format!(
                    "oci.dll is not a valid PE image; candidates checked: {candidates_examined}"
                ))
                .with_hint(
                    "Repair the Windows x64 Oracle Instant Client installation and restart OrdaDB.",
                ),
        );
    }
    Err(DbError::new(
        "58000",
        "Oracle Instant Client is not available",
    )
    .with_detail(format!(
        "no usable oci.dll was found; candidates checked: {candidates_examined}"
    ))
    .with_hint(
        "Install the Windows x64 Oracle Instant Client and add its directory to PATH; OrdaDB does not bundle Oracle DLLs.",
    ))
}

fn bounded_environment(name: &str) -> Result<Option<OsString>> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    if value.encode_wide().take(MAX_ENVIRONMENT_UNITS + 1).count() > MAX_ENVIRONMENT_UNITS {
        return Err(
            DbError::new("54000", "Oracle client environment is too large")
                .with_detail(format!("{name} exceeds the Windows environment limit")),
        );
    }
    Ok(Some(value))
}

fn normalize_directory_key(directory: &Path) -> Vec<u16> {
    directory
        .as_os_str()
        .encode_wide()
        .map(|unit| {
            if unit <= 0x7f {
                u16::from((unit as u8).to_ascii_lowercase())
            } else {
                unit
            }
        })
        .collect()
}

fn pe_machine(path: &Path) -> std::result::Result<u16, ()> {
    let mut file = File::open(path).map_err(|_| ())?;
    let length = file.metadata().map_err(|_| ())?.len();
    if length < 64 {
        return Err(());
    }
    let mut dos_header = [0_u8; 64];
    file.read_exact(&mut dos_header).map_err(|_| ())?;
    if &dos_header[..2] != b"MZ" {
        return Err(());
    }
    let pe_offset = u64::from(u32::from_le_bytes(
        dos_header[0x3c..0x40].try_into().map_err(|_| ())?,
    ));
    if pe_offset > MAX_PE_HEADER_OFFSET || pe_offset.saturating_add(6) > length {
        return Err(());
    }
    file.seek(SeekFrom::Start(pe_offset)).map_err(|_| ())?;
    let mut pe_header = [0_u8; 6];
    file.read_exact(&mut pe_header).map_err(|_| ())?;
    if &pe_header[..4] != b"PE\0\0" {
        return Err(());
    }
    Ok(u16::from_le_bytes([pe_header[4], pe_header[5]]))
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use uuid::Uuid;

    use super::*;

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            let path = env::temp_dir().join(format!("ordadb-oracle-oci-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("create temporary OCI directory");
            Self(path)
        }

        fn write_pe(&self, machine: u16) {
            let mut bytes = vec![0_u8; 0x86];
            bytes[..2].copy_from_slice(b"MZ");
            bytes[0x3c..0x40].copy_from_slice(&0x80_u32.to_le_bytes());
            bytes[0x80..0x84].copy_from_slice(b"PE\0\0");
            bytes[0x84..0x86].copy_from_slice(&machine.to_le_bytes());
            let mut file = File::create(self.0.join(ORACLE_CLIENT_DLL)).expect("create fake OCI");
            file.write_all(&bytes).expect("write fake OCI");
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn discovers_amd64_without_exposing_the_directory_in_debug() {
        let directory = TempDirectory::new();
        directory.write_pe(AMD64_PE_MACHINE);
        let location = inspect_amd64_oracle_client_directories([directory.0.clone()])
            .expect("discover AMD64 OCI");
        assert_eq!(
            location.directory(),
            directory
                .0
                .canonicalize()
                .expect("canonical test directory")
        );
        assert_eq!(location.candidates_examined(), 1);
        let debug = format!("{location:?}");
        assert!(!debug.contains(directory.0.to_string_lossy().as_ref()));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn missing_invalid_and_x86_clients_return_sanitized_diagnostics() {
        let missing = TempDirectory::new();
        let error = inspect_amd64_oracle_client_directories([missing.0.clone()])
            .expect_err("missing OCI must fail");
        assert_eq!(error.sql_state, "58000");
        assert!(!format!("{error:?}").contains(missing.0.to_string_lossy().as_ref()));

        fs::write(missing.0.join(ORACLE_CLIENT_DLL), b"not a PE").expect("write invalid OCI");
        let error = inspect_amd64_oracle_client_directories([missing.0.clone()])
            .expect_err("invalid OCI must fail");
        assert_eq!(error.sql_state, "58000");

        let x86 = TempDirectory::new();
        x86.write_pe(I386_PE_MACHINE);
        let error = inspect_amd64_oracle_client_directories([x86.0.clone()])
            .expect_err("x86 OCI must fail");
        assert_eq!(error.sql_state, "0A000");
        assert!(
            error
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("x86"))
        );
        assert!(!format!("{error:?}").contains(x86.0.to_string_lossy().as_ref()));
    }
}
