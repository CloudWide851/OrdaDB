#![cfg(windows)]

mod credential;
mod pipe_security;

pub use credential::{CredentialVault, StoredCredential};
pub use pipe_security::{current_process_user_sid, named_pipe_sddl, restrict_named_pipe_acl};
