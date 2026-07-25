use std::collections::BTreeMap;
use std::io::{Read, Write};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use rand::RngCore;
use rand::rngs::OsRng;

use ordadb_admin::{AuthStore, Principal, ScramVerifier};
use ordadb_types::{DbError, Result};

use crate::codec::{FrontendMessage, protocol, read_frontend, write_authentication};

const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
const MIN_NONCE_BYTES: usize = 8;
const MAX_NONCE_BYTES: usize = 256;
const MAX_SASL_BYTES: usize = 16 * 1024;

pub fn authenticate<S: Read + Write>(
    stream: &mut S,
    username: &str,
    auth: &AuthStore,
    max_frame_bytes: usize,
) -> Result<Principal> {
    if !auth.has_users()? {
        return Err(
            DbError::new("55000", "OrdaDB administrator bootstrap is required")
                .with_hint("run `ordadb-cli bootstrap` on the local machine"),
        );
    }
    let found = auth.scram_verifier(username).ok().flatten();
    let (principal, verifier, user_exists) = match found {
        Some((principal, verifier)) => (Some(principal), verifier, true),
        None => (
            None,
            ScramVerifier::derive(b"ordadb invalid authentication surrogate")?,
            false,
        ),
    };

    let mut mechanisms = Vec::new();
    mechanisms.extend_from_slice(SCRAM_SHA_256.as_bytes());
    mechanisms.extend_from_slice(&[0, 0]);
    write_authentication(stream, 10, &mechanisms)?;

    let initial = read_frontend(stream, max_frame_bytes)?
        .ok_or_else(|| protocol("connection closed during SASL initial response"))?;
    let FrontendMessage::Password(initial) = initial else {
        return Err(protocol(
            "expected PasswordMessage containing SASL initial response",
        ));
    };
    let initial = parse_initial(&initial)?;
    if initial.mechanism != SCRAM_SHA_256 {
        return Err(protocol("client selected an unsupported SASL mechanism"));
    }
    let client_first = std::str::from_utf8(initial.response)
        .map_err(|_| protocol("SCRAM client-first message is not UTF-8"))?;
    let Some(client_first_bare) = client_first.strip_prefix("n,,") else {
        return Err(protocol(
            "SCRAM channel binding flag must be `n` for SCRAM-SHA-256",
        ));
    };
    let client_attributes = parse_attributes(client_first_bare)?;
    if client_attributes.contains_key(&'m') {
        return Err(protocol("SCRAM mandatory extensions are unsupported"));
    }
    let client_user = unescape_username(required(&client_attributes, 'n')?)?;
    let client_nonce = required(&client_attributes, 'r')?;
    validate_nonce(client_nonce)?;
    if client_user != username {
        return Err(authentication_failed());
    }

    let mut server_nonce_bytes = [0_u8; 18];
    OsRng.fill_bytes(&mut server_nonce_bytes);
    let server_nonce = URL_SAFE_NO_PAD.encode(server_nonce_bytes);
    let combined_nonce = format!("{client_nonce}{server_nonce}");
    let salt = STANDARD.encode(verifier.salt()?);
    let server_first = format!("r={combined_nonce},s={salt},i={}", verifier.iterations());
    write_authentication(stream, 11, server_first.as_bytes())?;

    let final_message = read_frontend(stream, max_frame_bytes)?
        .ok_or_else(|| protocol("connection closed during SASL final response"))?;
    let FrontendMessage::Password(final_message) = final_message else {
        return Err(protocol(
            "expected PasswordMessage containing SASL final response",
        ));
    };
    if final_message.len() > MAX_SASL_BYTES {
        return Err(protocol("SCRAM final response exceeds its size limit"));
    }
    let final_message = std::str::from_utf8(&final_message)
        .map_err(|_| protocol("SCRAM client-final message is not UTF-8"))?;
    let proof_separator = final_message
        .rfind(",p=")
        .ok_or_else(|| protocol("SCRAM client-final message is missing proof"))?;
    let client_final_without_proof = &final_message[..proof_separator];
    let attributes = parse_attributes(final_message)?;
    if required(&attributes, 'c')? != "biws" {
        return Err(protocol("SCRAM channel binding value is invalid"));
    }
    if required(&attributes, 'r')? != combined_nonce {
        return Err(authentication_failed());
    }
    let proof = STANDARD
        .decode(required(&attributes, 'p')?)
        .map_err(|_| protocol("SCRAM client proof is not valid base64"))?;
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let proof_valid = verifier.verify_client_proof(auth_message.as_bytes(), &proof)?;
    if !user_exists || !proof_valid {
        return Err(authentication_failed());
    }
    let signature = verifier.server_signature(auth_message.as_bytes())?;
    let final_response = format!("v={}", STANDARD.encode(signature));
    write_authentication(stream, 12, final_response.as_bytes())?;
    write_authentication(stream, 0, &[])?;
    principal.ok_or_else(authentication_failed)
}

struct InitialResponse<'a> {
    mechanism: &'a str,
    response: &'a [u8],
}

fn parse_initial(bytes: &[u8]) -> Result<InitialResponse<'_>> {
    if bytes.len() > MAX_SASL_BYTES {
        return Err(protocol("SCRAM initial response exceeds its size limit"));
    }
    let mechanism_end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| protocol("SASL mechanism has no NUL terminator"))?;
    let mechanism = std::str::from_utf8(&bytes[..mechanism_end])
        .map_err(|_| protocol("SASL mechanism is not UTF-8"))?;
    let length_start = mechanism_end + 1;
    let length_end = length_start
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| protocol("SASL initial response length is truncated"))?;
    let declared = i32::from_be_bytes(
        bytes[length_start..length_end]
            .try_into()
            .expect("checked four-byte slice"),
    );
    let declared = usize::try_from(declared)
        .map_err(|_| protocol("SASL initial response length is negative"))?;
    let response = &bytes[length_end..];
    if response.len() != declared {
        return Err(protocol(
            "SASL initial response length does not match its payload",
        ));
    }
    Ok(InitialResponse {
        mechanism,
        response,
    })
}

fn parse_attributes(value: &str) -> Result<BTreeMap<char, &str>> {
    let mut attributes = BTreeMap::new();
    for part in value.split(',') {
        let bytes = part.as_bytes();
        if bytes.len() < 3 || bytes[1] != b'=' || !bytes[0].is_ascii_alphabetic() {
            return Err(protocol("SCRAM attribute is malformed"));
        }
        let key = char::from(bytes[0]);
        if attributes.insert(key, &part[2..]).is_some() {
            return Err(protocol(format!("SCRAM attribute {key} is duplicated")));
        }
    }
    Ok(attributes)
}

fn required<'a>(attributes: &'a BTreeMap<char, &'a str>, key: char) -> Result<&'a str> {
    attributes
        .get(&key)
        .copied()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| protocol(format!("SCRAM attribute {key} is required")))
}

fn unescape_username(username: &str) -> Result<String> {
    let mut result = String::with_capacity(username.len());
    let mut index = 0;
    while index < username.len() {
        let remaining = &username[index..];
        if let Some(rest) = remaining.strip_prefix("=2C") {
            result.push(',');
            index = username.len() - rest.len();
        } else if let Some(rest) = remaining.strip_prefix("=3D") {
            result.push('=');
            index = username.len() - rest.len();
        } else {
            let character = remaining
                .chars()
                .next()
                .ok_or_else(|| protocol("SCRAM username is malformed"))?;
            if character == '=' {
                return Err(protocol("SCRAM username contains an invalid escape"));
            }
            result.push(character);
            index += character.len_utf8();
        }
    }
    Ok(result)
}

fn validate_nonce(nonce: &str) -> Result<()> {
    if !(MIN_NONCE_BYTES..=MAX_NONCE_BYTES).contains(&nonce.len())
        || !nonce
            .bytes()
            .all(|byte| (b'!'..=b'~').contains(&byte) && byte != b',')
    {
        return Err(protocol("SCRAM nonce is invalid"));
    }
    Ok(())
}

fn authentication_failed() -> DbError {
    DbError::new("28P01", "authentication failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_response_and_username_escaping_are_strict() {
        let response = b"n,,n=dba,r=abcdefgh";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"SCRAM-SHA-256\0");
        bytes.extend_from_slice(&i32::try_from(response.len()).expect("length").to_be_bytes());
        bytes.extend_from_slice(response);
        let parsed = parse_initial(&bytes).expect("initial");
        assert_eq!(parsed.mechanism, SCRAM_SHA_256);
        assert_eq!(parsed.response, response);
        assert_eq!(unescape_username("a=2Cb=3Dc").expect("escape"), "a,b=c");
        assert!(unescape_username("a=4Fb").is_err());
    }

    #[test]
    fn duplicate_attributes_and_weak_nonces_are_rejected() {
        assert!(parse_attributes("n=dba,n=other").is_err());
        assert!(validate_nonce("short").is_err());
        assert!(validate_nonce("valid,not").is_err());
        assert!(validate_nonce("valid-nonce-value").is_ok());
    }
}
