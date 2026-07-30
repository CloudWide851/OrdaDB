#![cfg(windows)]

mod credential;
mod pipe_security;

pub use credential::{
    CredentialVault, PromptedCredential, StoredCredential, prompt_for_credential,
};
pub use pipe_security::{current_process_user_sid, named_pipe_sddl, restrict_named_pipe_acl};
