#![cfg(windows)]

mod atomic_file;
mod credential;
mod oracle_client;
mod pipe_security;

pub use atomic_file::write_file_atomic;
pub use credential::{
    CredentialVault, PromptedCredential, StoredCredential, prompt_for_credential,
};
pub use oracle_client::{
    OracleClientLocation, discover_amd64_oracle_client, inspect_amd64_oracle_client_directories,
};
pub use pipe_security::{current_process_user_sid, named_pipe_sddl, restrict_named_pipe_acl};
