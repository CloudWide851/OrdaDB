use std::fmt::{Debug, Formatter};
use std::ptr;
use std::slice;

use ordadb_types::{DbError, Result};
use windows_sys::Win32::Foundation::{ERROR_NOT_FOUND, GetLastError};
use windows_sys::Win32::Security::Credentials::{
    CRED_MAX_CREDENTIAL_BLOB_SIZE, CRED_MAX_GENERIC_TARGET_NAME_LENGTH, CRED_MAX_USERNAME_LENGTH,
    CRED_PERSIST_LOCAL_MACHINE, CRED_TYPE_GENERIC, CREDENTIALW, CredDeleteW, CredFree, CredReadW,
    CredWriteW,
};
use zeroize::{Zeroize, Zeroizing};

#[derive(Clone)]
pub struct StoredCredential {
    pub username: String,
    pub password: Zeroizing<String>,
}

impl Debug for StoredCredential {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredCredential")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
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
            username,
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
        assert_eq!(loaded.username, "dba");
        assert_eq!(loaded.password.as_str(), "credential-test-secret");
        let debug = format!("{loaded:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("credential-test-secret"));
        vault.delete(id).expect("delete");
        assert_eq!(vault.load(id).expect_err("deleted").sql_state, "42704");
    }
}
