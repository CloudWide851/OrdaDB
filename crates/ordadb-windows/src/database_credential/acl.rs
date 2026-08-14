use std::fs;
use std::path::Path;
use std::ptr;

use ordadb_types::{DbError, Result};
use windows_sys::Win32::Foundation::{LocalFree, NO_ERROR};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::{
    DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
    SetFileSecurityW,
};

use crate::current_process_user_sid;

pub(super) fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|_| io_error("failed to create credential directory"))?;
    restrict_path(path, true)
}

pub(super) fn restrict_private_file(path: &Path) -> Result<()> {
    restrict_path(path, false)
}

fn restrict_path(path: &Path, directory: bool) -> Result<()> {
    let sid = current_process_user_sid()?;
    let sddl = if directory {
        private_directory_sddl(&sid)
    } else {
        private_file_sddl(&sid)
    };
    let mut wide_sddl = sddl
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            wide_sddl.as_mut_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(io_error(
            "failed to build credential directory security descriptor",
        ));
    }
    struct Descriptor(PSECURITY_DESCRIPTOR);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            unsafe {
                LocalFree(self.0.cast());
            }
        }
    }
    let descriptor = Descriptor(descriptor);
    let mut wide_path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let applied = unsafe {
        SetFileSecurityW(
            wide_path.as_mut_ptr(),
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            descriptor.0,
        )
    };
    if applied == 0 {
        return Err(io_error("failed to restrict credential directory access"));
    }
    Ok(())
}

#[must_use]
pub(super) fn private_directory_sddl(user_sid: &str) -> String {
    format!("D:P(A;OICI;GA;;;SY)(A;OICI;GA;;;{user_sid})")
}

#[must_use]
pub(super) fn private_file_sddl(user_sid: &str) -> String {
    format!("D:P(A;;GA;;;SY)(A;;GA;;;{user_sid})")
}

fn io_error(message: &'static str) -> DbError {
    let error = std::io::Error::last_os_error();
    let code = error.raw_os_error().unwrap_or(NO_ERROR as i32);
    DbError::new("58030", message).with_detail(format!("Windows error {code}"))
}

use std::os::windows::ffi::OsStrExt;
