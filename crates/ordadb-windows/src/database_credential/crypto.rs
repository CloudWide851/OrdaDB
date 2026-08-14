use std::ptr;
use std::slice;

use ordadb_types::{DbError, Result};
use windows_sys::Win32::Foundation::{GetLastError, LocalFree};
use windows_sys::Win32::Security::Cryptography::{
    CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
};
#[cfg(test)]
use zeroize::Zeroize;
use zeroize::Zeroizing;

use super::{MAX_PASSWORD_BYTES, MAX_USERNAME_BYTES, invalid};
use crate::StoredCredential;

const PAYLOAD_VERSION: u32 = 1;
const ENTROPY: &[u8] = b"OrdaDB/database-credential-store/v1";
const MAX_PAYLOAD_BYTES: usize = 4 + 4 + MAX_USERNAME_BYTES + 4 + MAX_PASSWORD_BYTES;
const MAX_PROTECTED_BYTES: usize = 16 * 1024;

pub(super) fn encrypt(username: &str, password: &Zeroizing<String>) -> Result<Zeroizing<Vec<u8>>> {
    let payload = encode_payload(username, password)?;
    protect(&payload, ENTROPY)
}

pub(super) fn decrypt(ciphertext: &[u8]) -> Result<StoredCredential> {
    let plaintext = unprotect(ciphertext, ENTROPY)?;
    decode_payload(&plaintext)
}

fn encode_payload(username: &str, password: &str) -> Result<Zeroizing<Vec<u8>>> {
    super::validate_secret_text(username, MAX_USERNAME_BYTES, "credential username")?;
    super::validate_secret_text(password, MAX_PASSWORD_BYTES, "credential password")?;
    let username_len =
        u32::try_from(username.len()).map_err(|_| invalid("username is too long"))?;
    let password_len =
        u32::try_from(password.len()).map_err(|_| invalid("password is too long"))?;
    let mut payload = Zeroizing::new(Vec::with_capacity(
        12_usize
            .saturating_add(username.len())
            .saturating_add(password.len()),
    ));
    payload.extend_from_slice(&PAYLOAD_VERSION.to_le_bytes());
    payload.extend_from_slice(&username_len.to_le_bytes());
    payload.extend_from_slice(username.as_bytes());
    payload.extend_from_slice(&password_len.to_le_bytes());
    payload.extend_from_slice(password.as_bytes());
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(invalid("credential payload exceeds its byte limit"));
    }
    Ok(payload)
}

fn decode_payload(payload: &[u8]) -> Result<StoredCredential> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(invalid(
            "decrypted credential payload exceeds its byte limit",
        ));
    }
    let mut offset = 0_usize;
    let version = take_u32(payload, &mut offset)?;
    if version != PAYLOAD_VERSION {
        return Err(DbError::new(
            "0A000",
            "unsupported database credential payload version",
        ));
    }
    let username_len = take_length(payload, &mut offset, MAX_USERNAME_BYTES)?;
    let username = take_text(payload, &mut offset, username_len, "credential username")?;
    let password_len = take_length(payload, &mut offset, MAX_PASSWORD_BYTES)?;
    let password = take_text(payload, &mut offset, password_len, "credential password")?;
    if offset != payload.len() {
        return Err(invalid("credential payload contains trailing bytes"));
    }
    Ok(StoredCredential {
        username: Zeroizing::new(username),
        password: Zeroizing::new(password),
    })
}

fn take_u32(payload: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| invalid("credential payload length overflowed"))?;
    let bytes = payload
        .get(*offset..end)
        .ok_or_else(|| invalid("credential payload is truncated"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("credential payload is invalid"))?,
    ))
}

fn take_length(payload: &[u8], offset: &mut usize, maximum: usize) -> Result<usize> {
    let length = usize::try_from(take_u32(payload, offset)?)
        .map_err(|_| invalid("credential payload length overflowed"))?;
    if length == 0 || length > maximum {
        return Err(invalid("credential payload field exceeds its byte limit"));
    }
    Ok(length)
}

fn take_text(payload: &[u8], offset: &mut usize, length: usize, label: &str) -> Result<String> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("credential payload length overflowed"))?;
    let bytes = payload
        .get(*offset..end)
        .ok_or_else(|| invalid("credential payload is truncated"))?;
    *offset = end;
    String::from_utf8(bytes.to_vec()).map_err(|_| invalid(format!("{label} is not UTF-8")))
}

fn protect(plaintext: &[u8], entropy: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if plaintext.is_empty() || plaintext.len() > MAX_PAYLOAD_BYTES {
        return Err(invalid("credential plaintext is outside its byte limit"));
    }
    let input = blob(plaintext)?;
    let entropy = blob(entropy)?;
    let mut output = CRYPT_INTEGER_BLOB::default();
    // SAFETY: input and entropy reference live buffers for this call; output is
    // writable and receives a LocalAlloc buffer owned by `copy_local_blob`.
    let protected = unsafe {
        CryptProtectData(
            &input,
            ptr::null(),
            &entropy,
            ptr::null(),
            ptr::null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    };
    if protected == 0 {
        return Err(dpapi_error("failed to protect database credential"));
    }
    copy_local_blob(output, MAX_PROTECTED_BYTES)
}

fn unprotect(ciphertext: &[u8], entropy: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if ciphertext.is_empty() || ciphertext.len() > MAX_PROTECTED_BYTES {
        return Err(invalid("protected credential is outside its byte limit"));
    }
    let input = blob(ciphertext)?;
    let entropy = blob(entropy)?;
    let mut output = CRYPT_INTEGER_BLOB::default();
    // SAFETY: input and entropy reference live buffers for this call; output is
    // writable and receives a LocalAlloc buffer owned by `copy_local_blob`.
    let unprotected = unsafe {
        CryptUnprotectData(
            &input,
            ptr::null_mut(),
            &entropy,
            ptr::null(),
            ptr::null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    };
    if unprotected == 0 {
        return Err(dpapi_error("failed to unprotect database credential"));
    }
    copy_local_blob(output, MAX_PAYLOAD_BYTES)
}

fn blob(bytes: &[u8]) -> Result<CRYPT_INTEGER_BLOB> {
    Ok(CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(bytes.len())
            .map_err(|_| invalid("credential buffer is too large"))?,
        pbData: bytes.as_ptr().cast_mut(),
    })
}

fn copy_local_blob(output: CRYPT_INTEGER_BLOB, maximum: usize) -> Result<Zeroizing<Vec<u8>>> {
    struct LocalBlob(CRYPT_INTEGER_BLOB);
    impl Drop for LocalBlob {
        fn drop(&mut self) {
            if self.0.pbData.is_null() {
                return;
            }
            unsafe {
                ptr::write_bytes(self.0.pbData, 0, self.0.cbData as usize);
                LocalFree(self.0.pbData.cast());
            }
        }
    }
    let guard = LocalBlob(output);
    let length =
        usize::try_from(guard.0.cbData).map_err(|_| invalid("DPAPI output length overflowed"))?;
    if length == 0 || length > maximum || guard.0.pbData.is_null() {
        return Err(invalid("DPAPI returned an invalid credential buffer"));
    }
    let bytes = unsafe { slice::from_raw_parts(guard.0.pbData, length) };
    Ok(Zeroizing::new(bytes.to_vec()))
}

fn dpapi_error(context: &'static str) -> DbError {
    let code = unsafe { GetLastError() };
    DbError::new("58030", context).with_detail(format!("Windows error {code}"))
}

#[cfg(test)]
pub(super) fn unprotect_with_wrong_context(ciphertext: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let mut entropy = ENTROPY.to_vec();
    entropy.push(0xff);
    let result = unprotect(ciphertext, &entropy);
    entropy.zeroize();
    result
}
