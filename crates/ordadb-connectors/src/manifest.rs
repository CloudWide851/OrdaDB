use std::collections::BTreeSet;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use ordadb_types::{DbError, Result};
use reqwest::Url;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::{
    CONNECTOR_API_VERSION, CONNECTOR_MANIFEST_VERSION, MIN_CONNECTOR_API_VERSION, invalid,
    security_error,
};

const SIGNATURE_DOMAIN: &[u8] = b"OrdaDB connector manifest v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorArchitecture {
    WindowsX64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorDialect {
    PostgreSql,
    MySql,
    Sqlite,
    SqlServer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorPermission {
    Network,
    LocalDatabaseFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginManifestV1 {
    pub schema_version: u32,
    pub id: String,
    pub display_name: String,
    pub version: String,
    pub api_version: u32,
    pub architecture: ConnectorArchitecture,
    pub dialect: ConnectorDialect,
    pub publisher: String,
    pub permissions: Vec<ConnectorPermission>,
    pub entry: String,
    pub size: u64,
    pub sha256: String,
    pub signature: String,
    pub minimum_host_version: String,
    pub download_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistryCatalogV1 {
    pub schema_version: u32,
    pub plugins: Vec<PluginManifestV1>,
}

#[derive(Debug, Clone)]
pub struct ManifestPolicy {
    pub host_version: Version,
    pub minimum_api_version: u32,
    pub supported_api_version: u32,
    pub maximum_artifact_bytes: u64,
    pub allowed_origin: Url,
    pub verifying_key: VerifyingKey,
}

impl ManifestPolicy {
    pub fn new(
        host_version: &str,
        maximum_artifact_bytes: u64,
        allowed_origin: Url,
        verifying_key: VerifyingKey,
    ) -> Result<Self> {
        let host_version = Version::parse(host_version)
            .map_err(|error| invalid(format!("host version is invalid: {error}")))?;
        validate_https_origin(&allowed_origin)?;
        if maximum_artifact_bytes == 0 {
            return Err(invalid("maximum connector artifact size must be positive"));
        }
        Ok(Self {
            host_version,
            minimum_api_version: MIN_CONNECTOR_API_VERSION,
            supported_api_version: CONNECTOR_API_VERSION,
            maximum_artifact_bytes,
            allowed_origin,
            verifying_key,
        })
    }
}

pub fn decode_public_key(encoded: &str) -> Result<VerifyingKey> {
    let bytes = BASE64
        .decode(encoded)
        .map_err(|_| invalid("connector Registry public key is not valid base64"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| invalid("connector Registry public key must contain exactly 32 bytes"))?;
    VerifyingKey::from_bytes(&bytes)
        .map_err(|_| invalid("connector Registry public key is not a valid Ed25519 key"))
}

pub fn validate_manifest(manifest: &PluginManifestV1, policy: &ManifestPolicy) -> Result<()> {
    if manifest.schema_version != CONNECTOR_MANIFEST_VERSION {
        return Err(DbError::unsupported(format!(
            "connector manifest version {}",
            manifest.schema_version
        ))
        .with_hint("Install a connector manifest supported by this OrdaDB host."));
    }
    if manifest.api_version < policy.minimum_api_version
        || manifest.api_version > policy.supported_api_version
    {
        return Err(DbError::unsupported(format!(
            "connector API version {}",
            manifest.api_version
        ))
        .with_hint("Install a connector built for this OrdaDB host."));
    }
    validate_plugin_id(&manifest.id)?;
    validate_display_field("display name", &manifest.display_name)?;
    validate_display_field("publisher", &manifest.publisher)?;
    validate_entry(&manifest.entry)?;

    let version = Version::parse(&manifest.version)
        .map_err(|error| invalid(format!("connector version is invalid: {error}")))?;
    let minimum_host_version = Version::parse(&manifest.minimum_host_version)
        .map_err(|error| invalid(format!("minimum host version is invalid: {error}")))?;
    if minimum_host_version > policy.host_version {
        return Err(DbError::unsupported(format!(
            "connector {} {} requires OrdaDB {}",
            manifest.id, version, minimum_host_version
        ))
        .with_hint("Upgrade OrdaDB before installing this connector."));
    }
    if manifest.size == 0 || manifest.size > policy.maximum_artifact_bytes {
        return Err(invalid(format!(
            "connector artifact size must be between 1 and {} bytes",
            policy.maximum_artifact_bytes
        )));
    }
    decode_sha256(&manifest.sha256)?;
    validate_permissions(&manifest.permissions)?;

    let download_url = Url::parse(&manifest.download_url)
        .map_err(|error| invalid(format!("connector download URL is invalid: {error}")))?;
    validate_https_origin(&download_url)?;
    if !same_origin(&download_url, &policy.allowed_origin) {
        return Err(security_error(
            "connector download URL is outside the official Registry origin",
        )
        .with_hint("Use only artifacts published by the configured OrdaDB Registry."));
    }

    let signature_bytes = BASE64
        .decode(&manifest.signature)
        .map_err(|_| security_error("connector manifest signature is not valid base64"))?;
    let signature = Signature::from_slice(&signature_bytes)
        .map_err(|_| security_error("connector manifest signature must contain 64 bytes"))?;
    policy
        .verifying_key
        .verify(&manifest_signing_payload(manifest)?, &signature)
        .map_err(|_| {
            security_error("connector manifest signature verification failed")
                .with_hint("Refresh the official Registry and retry.")
        })?;
    Ok(())
}

pub fn manifest_signing_payload(manifest: &PluginManifestV1) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(512);
    payload.extend_from_slice(SIGNATURE_DOMAIN);
    push_u32(&mut payload, manifest.schema_version);
    push_string(&mut payload, &manifest.id)?;
    push_string(&mut payload, &manifest.display_name)?;
    push_string(&mut payload, &manifest.version)?;
    push_u32(&mut payload, manifest.api_version);
    push_string(
        &mut payload,
        match manifest.architecture {
            ConnectorArchitecture::WindowsX64 => "windowsX64",
        },
    )?;
    push_string(
        &mut payload,
        match manifest.dialect {
            ConnectorDialect::PostgreSql => "postgreSql",
            ConnectorDialect::MySql => "mySql",
            ConnectorDialect::Sqlite => "sqlite",
            ConnectorDialect::SqlServer => "sqlServer",
        },
    )?;
    push_string(&mut payload, &manifest.publisher)?;
    push_u32(
        &mut payload,
        u32::try_from(manifest.permissions.len())
            .map_err(|_| invalid("connector permission count overflowed"))?,
    );
    for permission in &manifest.permissions {
        push_string(
            &mut payload,
            match permission {
                ConnectorPermission::Network => "network",
                ConnectorPermission::LocalDatabaseFile => "localDatabaseFile",
            },
        )?;
    }
    push_string(&mut payload, &manifest.entry)?;
    payload.extend_from_slice(&manifest.size.to_le_bytes());
    push_string(&mut payload, &manifest.sha256)?;
    push_string(&mut payload, &manifest.minimum_host_version)?;
    push_string(&mut payload, &manifest.download_url)?;
    Ok(payload)
}

pub(crate) fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            "connector SHA-256 must be 64 lowercase hexadecimal characters",
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(chunk[0]) << 4) | hex_nibble(chunk[1]);
    }
    Ok(decoded)
}

pub(crate) fn validate_plugin_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || value.starts_with('-')
        || value.ends_with('-')
        || value.contains("--")
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && byte != b'-')
    {
        return Err(invalid(
            "connector ID must use 1-64 lowercase letters, digits, and single hyphens",
        ));
    }
    Ok(())
}

fn validate_display_field(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > 128
        || value.chars().any(|character| character.is_control())
    {
        return Err(invalid(format!(
            "connector {name} must contain 1-128 printable characters"
        )));
    }
    Ok(())
}

fn validate_entry(value: &str) -> Result<()> {
    let normalized = value.to_ascii_lowercase();
    let stem = normalized
        .strip_suffix(".exe")
        .ok_or_else(|| invalid("connector entry must be a single Windows executable filename"))?;
    let reserved = matches!(stem, "con" | "prn" | "aux" | "nul")
        || stem
            .strip_prefix("com")
            .or_else(|| stem.strip_prefix("lpt"))
            .is_some_and(|number| {
                matches!(number, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
            });
    if value.is_empty()
        || value.len() > 120
        || value != value.trim()
        || value.contains(['/', '\\', ':'])
        || value.contains("..")
        || reserved
        || value.chars().any(|character| character.is_control())
    {
        return Err(invalid(
            "connector entry must be a safe non-reserved .exe filename",
        ));
    }
    Ok(())
}

fn validate_permissions(permissions: &[ConnectorPermission]) -> Result<()> {
    let unique = permissions.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != permissions.len() {
        return Err(invalid("connector permissions must not contain duplicates"));
    }
    Ok(())
}

fn validate_https_origin(url: &Url) -> Result<()> {
    if url.scheme() != "https" || url.host_str().is_none() || !url.username().is_empty() {
        return Err(invalid(
            "connector Registry URLs must use HTTPS with a valid host and no user info",
        ));
    }
    Ok(())
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn push_string(target: &mut Vec<u8>, value: &str) -> Result<()> {
    let length =
        u32::try_from(value.len()).map_err(|_| invalid("connector manifest field overflowed"))?;
    push_u32(target, length);
    target.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_u32(target: &mut Vec<u8>, value: u32) {
    target.extend_from_slice(&value.to_le_bytes());
}

const fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use ed25519_dalek::{Signer, SigningKey};
    use reqwest::Url;

    use super::*;

    fn signed_manifest() -> (PluginManifestV1, ManifestPolicy) {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let mut manifest = PluginManifestV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            id: "ordadb-postgresql".into(),
            display_name: "OrdaDB / PostgreSQL".into(),
            version: "1.2.3".into(),
            api_version: CONNECTOR_API_VERSION,
            architecture: ConnectorArchitecture::WindowsX64,
            dialect: ConnectorDialect::PostgreSql,
            publisher: "OrdaDB".into(),
            permissions: vec![ConnectorPermission::Network],
            entry: "ordadb-postgresql.exe".into(),
            size: 128,
            sha256: "01".repeat(32),
            signature: String::new(),
            minimum_host_version: "0.1.0".into(),
            download_url: "https://plugins.ordadb.test/v1/ordadb-postgresql.exe".into(),
        };
        let signature =
            signing_key.sign(&manifest_signing_payload(&manifest).expect("signing payload"));
        manifest.signature = BASE64.encode(signature.to_bytes());
        let policy = ManifestPolicy::new(
            "0.1.0",
            1024,
            Url::parse("https://plugins.ordadb.test/catalog.json").expect("origin"),
            signing_key.verifying_key(),
        )
        .expect("policy");
        (manifest, policy)
    }

    #[test]
    fn validates_an_official_windows_x64_manifest() {
        let (manifest, policy) = signed_manifest();
        validate_manifest(&manifest, &policy).expect("valid manifest");
    }

    #[test]
    fn signing_payload_is_independent_of_signature_and_json_layout() {
        let (mut manifest, _) = signed_manifest();
        let first = manifest_signing_payload(&manifest).expect("payload");
        manifest.signature = "different".into();
        let second = manifest_signing_payload(&manifest).expect("payload");
        assert_eq!(first, second);
    }

    #[test]
    fn rejects_tampering_and_a_foreign_origin() {
        let (mut manifest, policy) = signed_manifest();
        manifest.size += 1;
        let error = validate_manifest(&manifest, &policy).expect_err("tampered manifest");
        assert_eq!(error.sql_state, "28000");

        let (mut manifest, policy) = signed_manifest();
        manifest.download_url = "https://evil.example/connector.exe".into();
        let error = validate_manifest(&manifest, &policy).expect_err("foreign origin");
        assert_eq!(error.sql_state, "28000");
    }

    #[test]
    fn rejects_path_traversal_reserved_names_and_bad_hashes() {
        let (mut manifest, policy) = signed_manifest();
        manifest.entry = "../connector.exe".into();
        let error = validate_manifest(&manifest, &policy).expect_err("traversal");
        assert_eq!(error.sql_state, "22023");

        let (mut manifest, policy) = signed_manifest();
        manifest.entry = "CON.exe".into();
        let error = validate_manifest(&manifest, &policy).expect_err("reserved name");
        assert_eq!(error.sql_state, "22023");

        assert!(decode_sha256(&"AA".repeat(32)).is_err());
    }

    #[test]
    fn rejects_unknown_versions_oversize_and_duplicate_permissions() {
        let (mut manifest, policy) = signed_manifest();
        manifest.schema_version = 2;
        assert_eq!(
            validate_manifest(&manifest, &policy)
                .expect_err("schema version")
                .sql_state,
            "0A000"
        );

        let (mut manifest, policy) = signed_manifest();
        manifest.size = 2048;
        assert_eq!(
            validate_manifest(&manifest, &policy)
                .expect_err("oversize")
                .sql_state,
            "22023"
        );

        let (mut manifest, policy) = signed_manifest();
        manifest.permissions.push(ConnectorPermission::Network);
        assert_eq!(
            validate_manifest(&manifest, &policy)
                .expect_err("duplicate permission")
                .sql_state,
            "22023"
        );
    }
}
