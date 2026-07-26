use std::mem::size_of;
use std::os::windows::io::AsRawHandle;

use ordadb_types::{DbError, Result};

pub fn restrict_named_pipe_acl(pipe: &impl AsRawHandle) -> Result<()> {
    use std::ptr;

    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SetKernelObjectSecurity,
    };

    let user_sid = current_process_user_sid()?;
    let sddl = named_pipe_sddl(&user_sid);
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
            "failed to build named-pipe security descriptor",
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
            "failed to apply named-pipe security descriptor",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

pub fn current_process_user_sid() -> Result<String> {
    use std::ffi::c_void;
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
            "failed to open the current process token for named-pipe security",
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
            .map_err(|_| DbError::internal("TOKEN_USER size cannot fit in a Windows buffer"))?
    {
        return Err(io_error(
            "failed to size the current process user token",
            std::io::Error::last_os_error(),
        ));
    }

    let required = usize::try_from(required)
        .map_err(|_| DbError::internal("token information length overflowed"))?;
    let words = required.div_ceil(size_of::<usize>());
    let mut buffer = vec![0_usize; words];
    let mut returned = u32::try_from(buffer.len() * size_of::<usize>())
        .map_err(|_| DbError::internal("token information buffer length overflowed"))?;
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
        DbError::internal("current process user SID is invalid UTF-16")
            .with_detail(error.to_string())
    })
}

#[must_use]
pub fn named_pipe_sddl(user_sid: &str) -> String {
    format!("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;LS)(A;;GA;;;OW)(A;;GA;;;{user_sid})")
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_pipe_acl_explicitly_allows_the_current_user() {
        let user_sid = current_process_user_sid().expect("current process SID");
        assert!(user_sid.starts_with("S-1-"));
        assert!(named_pipe_sddl(&user_sid).ends_with(&format!("(A;;GA;;;{user_sid})")));
    }
}
