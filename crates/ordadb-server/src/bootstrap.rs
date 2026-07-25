use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use ordadb_admin::AuthStore;
use ordadb_types::{DbError, Result};

const BOOTSTRAP_FORMAT_VERSION: u32 = 1;
const MAX_BOOTSTRAP_BYTES: usize = 4096;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(120);
const BOOTSTRAP_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_RETRY_COUNT: usize = 50;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);
const BOOTSTRAP_ACK: &[u8] = b"ack";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapRequest {
    format_version: u32,
    username: String,
    password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapResponse {
    pub success: bool,
    pub user: Option<String>,
    pub error: Option<DbError>,
}

#[must_use]
pub fn bootstrap_pipe_name(data_dir: &Path) -> String {
    let digest = Sha256::digest(data_dir.as_os_str().to_string_lossy().as_bytes());
    let suffix = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(r"\\.\pipe\ordadb-bootstrap-{suffix}")
}

pub async fn run_bootstrap_listener(
    pipe_name: String,
    auth: Arc<AuthStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    run_bootstrap_listener_with_ready(pipe_name, auth, shutdown, None).await
}

pub(crate) async fn run_bootstrap_listener_with_ready(
    pipe_name: String,
    auth: Arc<AuthStore>,
    shutdown: CancellationToken,
    mut ready: Option<oneshot::Sender<Result<()>>>,
) -> Result<()> {
    if auth.has_users()? {
        if let Some(ready) = ready.take() {
            let _ = ready.send(Ok(()));
        }
        shutdown.cancelled().await;
        return Ok(());
    }
    loop {
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let mut pipe = match options.create(&pipe_name) {
            Ok(pipe) => pipe,
            Err(error) => {
                let error = io_error("failed to create administrator bootstrap pipe", error);
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.clone()));
                }
                return Err(error);
            }
        };
        if let Err(error) = restrict_pipe_acl(&pipe) {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error.clone()));
            }
            return Err(error);
        }
        if let Some(ready) = ready.take() {
            let _ = ready.send(Ok(()));
        }
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            connected = pipe.connect() => {
                connected.map_err(|error| io_error("administrator bootstrap pipe connect failed", error))?;
            }
        }
        let response = match tokio::time::timeout(BOOTSTRAP_TIMEOUT, read_request(&mut pipe)).await
        {
            Ok(Ok(request)) => {
                if request.format_version != BOOTSTRAP_FORMAT_VERSION {
                    BootstrapResponse {
                        success: false,
                        user: None,
                        error: Some(
                            DbError::new(
                                "0A000",
                                format!(
                                    "bootstrap protocol version {} is unsupported",
                                    request.format_version
                                ),
                            )
                            .with_hint("upgrade ordadb-cli to the server version"),
                        ),
                    }
                } else {
                    let password = Zeroizing::new(request.password.into_bytes());
                    match auth.bootstrap_admin(&request.username, &password) {
                        Ok(principal) => BootstrapResponse {
                            success: true,
                            user: Some(principal.user),
                            error: None,
                        },
                        Err(error) => BootstrapResponse {
                            success: false,
                            user: None,
                            error: Some(error),
                        },
                    }
                }
            }
            Ok(Err(error)) => BootstrapResponse {
                success: false,
                user: None,
                error: Some(error),
            },
            Err(_) => BootstrapResponse {
                success: false,
                user: None,
                error: Some(DbError::new("57014", "administrator bootstrap timed out")),
            },
        };
        write_response(&mut pipe, &response).await?;
        let _ = tokio::time::timeout(BOOTSTRAP_ACK_TIMEOUT, async {
            let acknowledgement = read_frame(&mut pipe).await?;
            if acknowledgement != BOOTSTRAP_ACK {
                return Err(protocol("bootstrap acknowledgement is invalid"));
            }
            Ok(())
        })
        .await;
        pipe.disconnect().map_err(|error| {
            io_error("failed to disconnect administrator bootstrap pipe", error)
        })?;
        if response.success || auth.has_users()? {
            shutdown.cancelled().await;
            return Ok(());
        }
    }
}

pub async fn request_bootstrap(
    pipe_name: &str,
    username: String,
    password: Zeroizing<String>,
) -> Result<BootstrapResponse> {
    let mut pipe = None;
    for attempt in 0..CONNECT_RETRY_COUNT {
        match ClientOptions::new().open(pipe_name) {
            Ok(client) => {
                pipe = Some(client);
                break;
            }
            Err(error)
                if matches!(error.raw_os_error(), Some(2 | 5 | 231 | 233))
                    && attempt + 1 < CONNECT_RETRY_COUNT =>
            {
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            }
            Err(error) if matches!(error.raw_os_error(), Some(2 | 5 | 231 | 233)) => {
                return Err(io_error(
                    "administrator bootstrap pipe did not become available",
                    error,
                ));
            }
            Err(error) => {
                return Err(io_error(
                    "failed to connect to administrator bootstrap pipe",
                    error,
                ));
            }
        }
    }
    let mut pipe =
        pipe.ok_or_else(|| DbError::new("08001", "administrator bootstrap pipe is busy"))?;
    let request = serde_json::to_vec(&BootstrapRequestWire {
        format_version: BOOTSTRAP_FORMAT_VERSION,
        username,
        password: password.to_string(),
    })
    .map_err(|error| internal(format!("failed to encode bootstrap request: {error}")))?;
    if request.len() > MAX_BOOTSTRAP_BYTES {
        return Err(invalid("bootstrap request exceeds its size limit"));
    }
    write_frame(&mut pipe, &request).await?;
    let response = read_frame(&mut pipe).await?;
    let response = serde_json::from_slice(&response)
        .map_err(|error| protocol(format!("bootstrap response is invalid JSON: {error}")))?;
    write_frame(&mut pipe, BOOTSTRAP_ACK).await?;
    Ok(response)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapRequestWire {
    format_version: u32,
    username: String,
    password: String,
}

async fn read_request(pipe: &mut NamedPipeServer) -> Result<BootstrapRequest> {
    let bytes = read_frame(pipe).await?;
    serde_json::from_slice(&bytes)
        .map_err(|error| protocol(format!("bootstrap request is invalid JSON: {error}")))
}

async fn write_response(pipe: &mut NamedPipeServer, response: &BootstrapResponse) -> Result<()> {
    let bytes = serde_json::to_vec(response)
        .map_err(|error| internal(format!("failed to encode bootstrap response: {error}")))?;
    write_frame(pipe, &bytes).await
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let length = reader
        .read_u32()
        .await
        .map_err(|error| io_error("failed to read bootstrap frame length", error))?;
    let length = usize::try_from(length)
        .map_err(|_| protocol("bootstrap frame length cannot fit in memory"))?;
    if length == 0 || length > MAX_BOOTSTRAP_BYTES {
        return Err(protocol(format!(
            "bootstrap frame length must be 1..={MAX_BOOTSTRAP_BYTES}"
        )));
    }
    let mut bytes = vec![0_u8; length];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|error| io_error("failed to read bootstrap frame", error))?;
    Ok(bytes)
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8]) -> Result<()> {
    let length =
        u32::try_from(bytes.len()).map_err(|_| protocol("bootstrap frame length overflowed"))?;
    writer
        .write_u32(length)
        .await
        .map_err(|error| io_error("failed to write bootstrap frame length", error))?;
    writer
        .write_all(bytes)
        .await
        .map_err(|error| io_error("failed to write bootstrap frame", error))?;
    writer
        .flush()
        .await
        .map_err(|error| io_error("failed to flush bootstrap frame", error))
}

#[cfg(windows)]
fn restrict_pipe_acl(pipe: &NamedPipeServer) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr;

    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SetKernelObjectSecurity,
    };

    let user_sid = current_process_user_sid()?;
    let sddl = bootstrap_pipe_sddl(&user_sid);
    let mut wide: Vec<u16> = sddl.encode_utf16().collect();
    wide.push(0);
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: `wide` is a valid NUL-terminated SDDL string and `descriptor`
    // points to writable storage owned until LocalFree below.
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            wide.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(io_error(
            "failed to build bootstrap pipe security descriptor",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: the pipe was opened with WRITE_DAC and descriptor is the valid
    // allocation returned by ConvertStringSecurityDescriptor... above.
    let applied = unsafe {
        SetKernelObjectSecurity(pipe.as_raw_handle(), DACL_SECURITY_INFORMATION, descriptor)
    };
    // SAFETY: descriptor is the LocalAlloc allocation returned by the Win32
    // conversion function and is released exactly once.
    unsafe {
        LocalFree(descriptor.cast());
    }
    if applied == 0 {
        return Err(io_error(
            "failed to apply bootstrap pipe security descriptor",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn current_process_user_sid() -> Result<String> {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::ptr;
    use std::slice;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
    use windows_sys::core::PWSTR;

    struct TokenHandle(HANDLE);

    impl Drop for TokenHandle {
        fn drop(&mut self) {
            // SAFETY: this handle is returned by OpenProcessToken and is closed
            // exactly once when its guard leaves scope.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    struct LocalWideString(PWSTR);

    impl Drop for LocalWideString {
        fn drop(&mut self) {
            // SAFETY: this pointer is the LocalAlloc allocation returned by
            // ConvertSidToStringSidW and is freed exactly once.
            unsafe {
                LocalFree(self.0.cast());
            }
        }
    }

    let mut raw_token: HANDLE = ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a valid pseudo-handle and raw_token
    // points to writable storage for the opened token handle.
    let opened = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw_token) };
    if opened == 0 {
        return Err(io_error(
            "failed to open the current process token for bootstrap pipe security",
            std::io::Error::last_os_error(),
        ));
    }
    let token = TokenHandle(raw_token);

    let mut required = 0_u32;
    // SAFETY: a null information buffer with length zero is the documented
    // sizing call; required points to writable storage.
    unsafe {
        GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut required);
    }
    if required
        < u32::try_from(size_of::<TOKEN_USER>())
            .map_err(|_| internal("TOKEN_USER size cannot fit in a Windows buffer length"))?
    {
        return Err(io_error(
            "failed to size the current process user token",
            std::io::Error::last_os_error(),
        ));
    }

    let required =
        usize::try_from(required).map_err(|_| internal("token information length overflowed"))?;
    let words = required.div_ceil(size_of::<usize>());
    let mut buffer = vec![0_usize; words];
    let mut returned = u32::try_from(buffer.len() * size_of::<usize>())
        .map_err(|_| internal("token information buffer length overflowed"))?;
    // SAFETY: buffer is aligned for TOKEN_USER and contains at least the byte
    // length requested by the sizing call; returned points to writable storage.
    let retrieved = unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            buffer.as_mut_ptr().cast::<c_void>(),
            returned,
            &mut returned,
        )
    };
    if retrieved == 0 {
        return Err(io_error(
            "failed to read the current process user token",
            std::io::Error::last_os_error(),
        ));
    }

    // SAFETY: GetTokenInformation successfully initialized the aligned buffer
    // with a TOKEN_USER value whose SID remains valid while buffer is alive.
    let token_user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut raw_sid: PWSTR = ptr::null_mut();
    // SAFETY: token_user.User.Sid is valid for buffer's lifetime and raw_sid
    // points to writable storage for the LocalAlloc string.
    let converted = unsafe { ConvertSidToStringSidW(token_user.User.Sid, &mut raw_sid) };
    if converted == 0 {
        return Err(io_error(
            "failed to encode the current process user SID",
            std::io::Error::last_os_error(),
        ));
    }
    let sid = LocalWideString(raw_sid);
    let mut length = 0_usize;
    // SAFETY: ConvertSidToStringSidW returns a valid NUL-terminated UTF-16
    // string. This loop reads only through its terminator.
    while unsafe { *sid.0.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: the preceding loop established the initialized string length,
    // excluding the NUL terminator, and the LocalWideString guard is alive.
    let units = unsafe { slice::from_raw_parts(sid.0, length) };
    String::from_utf16(units).map_err(|error| {
        internal(format!(
            "current process user SID is invalid UTF-16: {error}"
        ))
    })
}

#[cfg(windows)]
fn bootstrap_pipe_sddl(user_sid: &str) -> String {
    format!("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;LS)(A;;GA;;;OW)(A;;GA;;;{user_sid})")
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn protocol(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message)
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipe_name_is_stable_and_scoped_to_the_data_directory() {
        let first = bootstrap_pipe_name(Path::new(r"C:\ProgramData\OrdaDB\data"));
        let second = bootstrap_pipe_name(Path::new(r"C:\ProgramData\OrdaDB\other"));
        assert!(first.starts_with(r"\\.\pipe\ordadb-bootstrap-"));
        assert_ne!(first, second);
        assert_eq!(
            first,
            bootstrap_pipe_name(Path::new(r"C:\ProgramData\OrdaDB\data"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn bootstrap_pipe_acl_explicitly_allows_the_current_user() {
        let user_sid = current_process_user_sid().expect("current process SID");
        assert!(user_sid.starts_with("S-1-"));
        assert!(bootstrap_pipe_sddl(&user_sid).ends_with(&format!("(A;;GA;;;{user_sid})")));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn bootstrap_pipe_acl_accepts_a_current_process_client() {
        let pipe_name = format!(
            r"\\.\pipe\ordadb-bootstrap-acl-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let pipe = options.create(&pipe_name).expect("create test pipe");
        restrict_pipe_acl(&pipe).expect("restrict test pipe");
        let client = ClientOptions::new()
            .open(&pipe_name)
            .expect("current process client");
        drop(client);
        drop(pipe);
    }

    #[tokio::test]
    async fn bootstrap_frames_round_trip_and_reject_oversized_lengths() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        let writing = tokio::spawn(async move {
            write_frame(&mut writer, b"bootstrap").await.expect("write");
        });
        assert_eq!(read_frame(&mut reader).await.expect("read"), b"bootstrap");
        writing.await.expect("writer");

        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_u32(u32::try_from(MAX_BOOTSTRAP_BYTES + 1).expect("length"))
            .await
            .expect("oversized frame");
        assert_eq!(
            read_frame(&mut reader).await.expect_err("size").sql_state,
            "08P01"
        );
    }
}
