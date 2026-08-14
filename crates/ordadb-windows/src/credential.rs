use std::fmt::{Debug, Formatter};
use std::ptr;
use std::slice;

use ordadb_types::{DbError, Result};
use windows_sys::Win32::Foundation::{ERROR_CANCELLED, ERROR_NOT_FOUND, GetLastError, NO_ERROR};
use windows_sys::Win32::Security::Credentials::{
    CRED_MAX_CREDENTIAL_BLOB_SIZE, CRED_MAX_GENERIC_TARGET_NAME_LENGTH, CRED_MAX_USERNAME_LENGTH,
    CRED_PERSIST_LOCAL_MACHINE, CRED_TYPE_GENERIC, CREDENTIALW, CREDUI_FLAGS_ALWAYS_SHOW_UI,
    CREDUI_FLAGS_DO_NOT_PERSIST, CREDUI_FLAGS_EXCLUDE_CERTIFICATES,
    CREDUI_FLAGS_GENERIC_CREDENTIALS, CREDUI_INFOW, CREDUI_MAX_CAPTION_LENGTH,
    CREDUI_MAX_MESSAGE_LENGTH, CredDeleteW, CredFree, CredReadW, CredUIPromptForCredentialsW,
    CredWriteW,
};
use zeroize::{Zeroize, Zeroizing};

const CREDUI_PASSWORD_BUFFER_UNITS: usize = 257;

#[derive(Clone)]
pub struct StoredCredential {
    pub username: Zeroizing<String>,
    pub password: Zeroizing<String>,
}

impl Debug for StoredCredential {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredCredential")
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}

pub struct PromptedCredential {
    pub username: String,
    pub password: Zeroizing<String>,
}

impl Debug for PromptedCredential {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PromptedCredential")
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}

pub fn prompt_for_credential(
    target_name: &str,
    suggested_username: &str,
    caption: &str,
    message: &str,
) -> Result<Option<PromptedCredential>> {
    let mut target = prompt_text(
        target_name,
        CRED_MAX_GENERIC_TARGET_NAME_LENGTH as usize,
        "credential prompt target",
    )?;
    let mut caption = prompt_text(
        caption,
        CREDUI_MAX_CAPTION_LENGTH as usize,
        "credential prompt caption",
    )?;
    let mut message = prompt_text(
        message,
        CREDUI_MAX_MESSAGE_LENGTH as usize,
        "credential prompt message",
    )?;
    let suggested = prompt_text(
        suggested_username,
        CRED_MAX_USERNAME_LENGTH as usize,
        "suggested credential username",
    )?;
    let mut username = vec![0_u16; CRED_MAX_USERNAME_LENGTH as usize + 1];
    username[..suggested.len()].copy_from_slice(&suggested[..suggested.len()]);
    let mut password = Zeroizing::new(vec![0_u16; CREDUI_PASSWORD_BUFFER_UNITS]);
    let mut save = 0;
    let info = CREDUI_INFOW {
        cbSize: u32::try_from(std::mem::size_of::<CREDUI_INFOW>())
            .map_err(|_| invalid("credential prompt structure size overflowed"))?,
        hwndParent: ptr::null_mut(),
        pszMessageText: message.as_ptr(),
        pszCaptionText: caption.as_ptr(),
        hbmBanner: ptr::null_mut(),
    };
    // SAFETY: all input strings are live NUL-terminated UTF-16 buffers,
    // output buffers are writable for the declared lengths, and no pointers
    // escape this call.
    let result = unsafe {
        CredUIPromptForCredentialsW(
            &info,
            target.as_ptr(),
            ptr::null(),
            0,
            username.as_mut_ptr(),
            username.len() as u32,
            password.as_mut_ptr(),
            password.len() as u32,
            &mut save,
            CREDUI_FLAGS_ALWAYS_SHOW_UI
                | CREDUI_FLAGS_DO_NOT_PERSIST
                | CREDUI_FLAGS_EXCLUDE_CERTIFICATES
                | CREDUI_FLAGS_GENERIC_CREDENTIALS,
        )
    };
    target.zeroize();
    caption.zeroize();
    message.zeroize();
    if result == ERROR_CANCELLED {
        username.zeroize();
        return Ok(None);
    }
    if result != NO_ERROR {
        username.zeroize();
        return Err(DbError::new("58030", "Windows credential prompt failed")
            .with_detail(format!("Windows error {result}")));
    }
    let prompted_username = string_from_wide_buffer(
        &username,
        CRED_MAX_USERNAME_LENGTH as usize,
        "prompted credential username",
    )?;
    username.zeroize();
    let prompted_password = string_from_wide_buffer(
        &password,
        CREDUI_PASSWORD_BUFFER_UNITS - 1,
        "prompted credential password",
    )?;
    Ok(Some(PromptedCredential {
        username: prompted_username,
        password: Zeroizing::new(prompted_password),
    }))
}

#[derive(Debug, Clone)]
pub struct CredentialVault {
    namespace: String,
}

impl CredentialVault {
    pub fn new(namespace: impl Into<String>) -> Result<Self> {
        let namespace = namespace.into();
        if namespace.trim().is_empty()
            || namespace.len() > 128
            || namespace.contains(['\0', '\\', ':'])
            || namespace.chars().any(|character| character.is_control())
        {
            return Err(invalid(
                "credential namespace must be a printable Windows Credential Manager prefix",
            ));
        }
        Ok(Self { namespace })
    }

    pub fn store(
        &self,
        credential_id: &str,
        username: &str,
        password: &Zeroizing<String>,
    ) -> Result<()> {
        let mut target = wide(&self.target(credential_id)?);
        let mut username_wide = wide(username);
        if username_wide.len().saturating_sub(1) > CRED_MAX_USERNAME_LENGTH as usize {
            return Err(invalid("credential username exceeds the Windows limit"));
        }
        let mut blob = password.as_bytes().to_vec();
        if blob.is_empty() || blob.len() > CRED_MAX_CREDENTIAL_BLOB_SIZE as usize {
            blob.zeroize();
            return Err(invalid(
                "credential password must be non-empty and fit Windows Credential Manager",
            ));
        }
        let blob_size = u32::try_from(blob.len())
            .map_err(|_| invalid("credential password size overflowed"))?;
        let credential = CREDENTIALW {
            Type: CRED_TYPE_GENERIC,
            TargetName: target.as_mut_ptr(),
            CredentialBlobSize: blob_size,
            CredentialBlob: blob.as_mut_ptr(),
            Persist: CRED_PERSIST_LOCAL_MACHINE,
            UserName: username_wide.as_mut_ptr(),
            ..CREDENTIALW::default()
        };
        // SAFETY: every pointer in `credential` references a live mutable
        // buffer for the duration of CredWriteW; lengths are validated above.
        let written = unsafe { CredWriteW(&credential, 0) };
        blob.zeroize();
        target.zeroize();
        username_wide.zeroize();
        if written == 0 {
            return Err(last_error("failed to store connector credential"));
        }
        Ok(())
    }

    pub fn load(&self, credential_id: &str) -> Result<StoredCredential> {
        let mut target = wide(&self.target(credential_id)?);
        let mut pointer: *mut CREDENTIALW = ptr::null_mut();
        // SAFETY: target is a valid NUL-terminated UTF-16 string and pointer
        // refers to writable storage for the API-owned allocation.
        let read = unsafe { CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut pointer) };
        target.zeroize();
        if read == 0 {
            return Err(last_error("failed to load connector credential"));
        }
        let guard = CredentialGuard(pointer);
        // SAFETY: CredReadW succeeded and returned a valid CREDENTIALW
        // allocation that remains owned by `guard`.
        let credential = unsafe { &*guard.0 };
        let username = wide_pointer_to_string(
            credential.UserName,
            CRED_MAX_USERNAME_LENGTH as usize,
            "credential username",
        )?;
        let blob_size = usize::try_from(credential.CredentialBlobSize)
            .map_err(|_| invalid("credential password size overflowed"))?;
        if blob_size == 0 || blob_size > CRED_MAX_CREDENTIAL_BLOB_SIZE as usize {
            return Err(invalid(
                "stored connector credential has an invalid password size",
            ));
        }
        // SAFETY: Windows guarantees CredentialBlob points to
        // CredentialBlobSize readable bytes while the credential is alive.
        let password_bytes =
            unsafe { slice::from_raw_parts(credential.CredentialBlob, blob_size) }.to_vec();
        let password = String::from_utf8(password_bytes).map_err(|error| {
            invalid("stored connector credential is not valid UTF-8").with_detail(error.to_string())
        })?;
        Ok(StoredCredential {
            username: Zeroizing::new(username),
            password: Zeroizing::new(password),
        })
    }

    pub fn delete(&self, credential_id: &str) -> Result<()> {
        let mut target = wide(&self.target(credential_id)?);
        // SAFETY: target is a valid NUL-terminated UTF-16 string.
        let deleted = unsafe { CredDeleteW(target.as_ptr(), CRED_TYPE_GENERIC, 0) };
        target.zeroize();
        if deleted == 0 {
            return Err(last_error("failed to delete connector credential"));
        }
        Ok(())
    }

    fn target(&self, credential_id: &str) -> Result<String> {
        validate_credential_id(credential_id)?;
        let target = format!("{}/{}", self.namespace, credential_id);
        if target.encode_utf16().count() > CRED_MAX_GENERIC_TARGET_NAME_LENGTH as usize {
            return Err(invalid("credential target exceeds the Windows limit"));
        }
        Ok(target)
    }
}

struct CredentialGuard(*mut CREDENTIALW);

impl Drop for CredentialGuard {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        // SAFETY: the pointer is the allocation returned by CredReadW. Its
        // blob is writable API-owned memory and is cleared before CredFree.
        unsafe {
            let credential = &mut *self.0;
            if !credential.CredentialBlob.is_null() && credential.CredentialBlobSize > 0 {
                ptr::write_bytes(
                    credential.CredentialBlob,
                    0,
                    credential.CredentialBlobSize as usize,
                );
            }
            CredFree(self.0.cast());
        }
    }
}

fn validate_credential_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('.')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid(
            "credential ID must use 1-128 ASCII letters, digits, dots, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn prompt_text(value: &str, maximum: usize, context: &str) -> Result<Vec<u16>> {
    if value.contains('\0') || value.encode_utf16().count() > maximum {
        return Err(invalid(format!(
            "{context} must be NUL-free and fit the Windows limit"
        )));
    }
    Ok(wide(value))
}

fn string_from_wide_buffer(buffer: &[u16], maximum: usize, context: &str) -> Result<String> {
    let length = buffer
        .iter()
        .take(maximum + 1)
        .position(|unit| *unit == 0)
        .ok_or_else(|| invalid(format!("{context} exceeds the Windows limit")))?;
    String::from_utf16(&buffer[..length]).map_err(|error| {
        invalid(format!("{context} is invalid UTF-16")).with_detail(error.to_string())
    })
}

fn wide_pointer_to_string(pointer: *const u16, maximum: usize, context: &str) -> Result<String> {
    if pointer.is_null() {
        return Ok(String::new());
    }
    let mut length = 0_usize;
    // SAFETY: caller provides a Windows-owned NUL-terminated UTF-16 pointer;
    // the bounded loop never scans beyond the documented field maximum.
    while length <= maximum && unsafe { *pointer.add(length) } != 0 {
        length += 1;
    }
    if length > maximum {
        return Err(invalid(format!("{context} exceeds the Windows limit")));
    }
    // SAFETY: the loop established the initialized length before the NUL.
    let units = unsafe { slice::from_raw_parts(pointer, length) };
    String::from_utf16(units).map_err(|error| {
        invalid(format!("{context} is invalid UTF-16")).with_detail(error.to_string())
    })
}

fn last_error(context: &str) -> DbError {
    // SAFETY: GetLastError has no preconditions and is read immediately after
    // the failed credential API call.
    let code = unsafe { GetLastError() };
    if code == ERROR_NOT_FOUND {
        DbError::new("42704", "connector credential does not exist")
    } else {
        DbError::new("58030", context).with_detail(format!("Windows error {code}"))
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    struct Cleanup<'a> {
        vault: &'a CredentialVault,
        id: &'a str,
    }

    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = self.vault.delete(self.id);
        }
    }

    #[test]
    fn credential_manager_round_trip_is_redacted_and_deleted() {
        let namespace = format!("OrdaDB/test/{}", Uuid::new_v4());
        let vault = CredentialVault::new(namespace).expect("vault");
        let id = "connection";
        let _cleanup = Cleanup { vault: &vault, id };
        let password = Zeroizing::new("credential-test-secret".to_owned());
        vault.store(id, "dba", &password).expect("store");
        let loaded = vault.load(id).expect("load");
        assert_eq!(loaded.username.as_str(), "dba");
        assert_eq!(loaded.password.as_str(), "credential-test-secret");
        let debug = format!("{loaded:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("dba"));
        assert!(!debug.contains("credential-test-secret"));
        vault.delete(id).expect("delete");
        assert_eq!(vault.load(id).expect_err("deleted").sql_state, "42704");
    }

    #[test]
    fn credential_prompt_helpers_are_bounded_and_secret_safe() {
        let prompted = PromptedCredential {
            username: "dba".to_owned(),
            password: Zeroizing::new("prompt-secret".to_owned()),
        };
        let debug = format!("{prompted:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("dba"));
        assert!(!debug.contains("prompt-secret"));

        assert_eq!(
            string_from_wide_buffer(&[b'd' as u16, b'b' as u16, b'a' as u16, 0], 3, "username")
                .expect("bounded username"),
            "dba"
        );
        assert!(string_from_wide_buffer(&[b'd' as u16; 4], 3, "username").is_err());
        assert!(prompt_text("contains\0nul", 32, "message").is_err());
        assert!(prompt_text("too-long", 3, "message").is_err());
    }
}
