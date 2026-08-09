use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{SecondsFormat, Utc};
use ed25519_dalek::{Signer, SigningKey};
use ordadb_connectors::{
    CONNECTOR_MANIFEST_VERSION, ConnectorArchitecture, CredentialVault,
    OFFICIAL_CONNECTOR_DESCRIPTORS, PluginManifestV1, RegistryCatalogV1, manifest_signing_payload,
};
use rand::RngCore as _;
use rand::rngs::OsRng;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

const CREDENTIAL_NAMESPACE: &str = "OrdaDB/Release";
const DEFAULT_CREDENTIAL_ID: &str = "connector-registry-ed25519-v1";
const SIGNING_KEY_ENV: &str = "ORDADB_CONNECTOR_SIGNING_KEY";
const HISTORY_VERSION: u32 = 1;

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
struct SignOptions {
    artifacts: PathBuf,
    bundle_output: PathBuf,
    site_output: Option<PathBuf>,
    previous_history: Option<PathBuf>,
    public_key: PathBuf,
    version: Version,
    base_url: String,
    credential_id: String,
}

#[derive(Debug)]
struct GenerateOptions {
    public_key: PathBuf,
    credential_id: String,
}

#[derive(Debug)]
struct SyncSecretOptions {
    repository: String,
    secret_name: String,
    credential_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectorHistoryV1 {
    schema_version: u32,
    generated_at: String,
    versions: Vec<ConnectorHistoryVersionV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectorHistoryVersionV1 {
    version: String,
    published_at: String,
    plugins: Vec<ConnectorHistoryPluginV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectorHistoryPluginV1 {
    id: String,
    version: String,
    size: u64,
    sha256: String,
    download_url: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("connector publisher failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> AppResult<()> {
    let mut arguments = env::args().skip(1);
    let command = arguments
        .next()
        .ok_or("expected generate-key, sign-bundle, or sync-github-secret")?;
    let remaining = arguments.collect::<Vec<_>>();
    match command.as_str() {
        "generate-key" => generate_key(parse_generate_options(&remaining)?)?,
        "sign-bundle" => sign_bundle(parse_sign_options(&remaining)?)?,
        "sync-github-secret" => sync_github_secret(parse_sync_secret_options(&remaining)?)?,
        _ => return Err(format!("unknown connector publisher command: {command}").into()),
    }
    Ok(())
}

fn generate_key(options: GenerateOptions) -> AppResult<()> {
    if options.public_key.exists() {
        return Err("refusing to overwrite an existing production public key".into());
    }
    let mut secret_bytes = Zeroizing::new([0_u8; 32]);
    OsRng.fill_bytes(secret_bytes.as_mut());
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let encoded_secret = Zeroizing::new(BASE64.encode(signing_key.to_bytes()));
    CredentialVault::new(CREDENTIAL_NAMESPACE)?.store(
        &options.credential_id,
        "ed25519-v1",
        &encoded_secret,
    )?;
    write_new_synced(
        &options.public_key,
        BASE64
            .encode(signing_key.verifying_key().to_bytes())
            .as_bytes(),
    )?;
    println!("Generated the production connector trust root and protected its backup.");
    Ok(())
}

fn sign_bundle(options: SignOptions) -> AppResult<()> {
    let signing_key = load_signing_key(&options.credential_id)?;
    verify_expected_public_key(&options.public_key, &signing_key)?;
    let base_url = normalized_base_url(&options.base_url)?;
    let mut manifests = Vec::with_capacity(OFFICIAL_CONNECTOR_DESCRIPTORS.len());
    let mut artifact_sources = BTreeMap::new();
    for descriptor in OFFICIAL_CONNECTOR_DESCRIPTORS {
        let entry = format!("{}.exe", descriptor.package);
        let source = options.artifacts.join(&entry);
        let (size, sha256) = hash_file(&source)?;
        let mut manifest = PluginManifestV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            id: descriptor.id.into(),
            display_name: descriptor.display_name.into(),
            version: options.version.to_string(),
            api_version: descriptor.api_version,
            architecture: ConnectorArchitecture::WindowsX64,
            dialect: descriptor.dialect,
            publisher: "OrdaDB".into(),
            permissions: descriptor.permissions.to_vec(),
            entry: entry.clone(),
            size,
            sha256,
            signature: String::new(),
            minimum_host_version: options.version.to_string(),
            download_url: format!("{base_url}artifacts/{}/{entry}", options.version),
        };
        manifest.signature = BASE64.encode(
            signing_key
                .sign(&manifest_signing_payload(&manifest)?)
                .to_bytes(),
        );
        artifact_sources.insert(entry, source);
        manifests.push(manifest);
    }
    manifests.sort_by(|left, right| left.id.cmp(&right.id));
    let catalog = RegistryCatalogV1 {
        schema_version: CONNECTOR_MANIFEST_VERSION,
        plugins: manifests.clone(),
    };

    replace_directory(&options.bundle_output, |temporary| {
        for (entry, source) in &artifact_sources {
            copy_file_synced(source, &temporary.join(entry))?;
        }
        write_json_synced(&temporary.join("catalog-v1.json"), &catalog)?;
        Ok(())
    })?;

    if let Some(site_output) = &options.site_output {
        let history = build_history(
            options.previous_history.as_deref(),
            &options.version,
            &manifests,
        )?;
        replace_directory(site_output, |temporary| {
            let artifacts = temporary
                .join("artifacts")
                .join(options.version.to_string());
            fs::create_dir_all(&artifacts)?;
            for (entry, source) in &artifact_sources {
                copy_file_synced(source, &artifacts.join(entry))?;
            }
            write_json_synced(&temporary.join("catalog-v1.json"), &catalog)?;
            write_json_synced(&temporary.join("history-v1.json"), &history)?;
            Ok(())
        })?;
    }
    println!("Signed and staged nine Windows x64 connector helpers.");
    Ok(())
}

fn sync_github_secret(options: SyncSecretOptions) -> AppResult<()> {
    validate_simple_name(&options.secret_name, "GitHub secret name")?;
    validate_repository(&options.repository)?;
    let credential = CredentialVault::new(CREDENTIAL_NAMESPACE)?.load(&options.credential_id)?;
    let mut child = Command::new("gh")
        .args([
            "secret",
            "set",
            &options.secret_name,
            "--repo",
            &options.repository,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or("failed to open GitHub CLI input")?;
    stdin.write_all(credential.password.as_bytes())?;
    drop(stdin);
    let status = child.wait()?;
    if !status.success() {
        return Err("GitHub CLI did not accept the protected connector signing key".into());
    }
    println!("Updated the protected GitHub connector signing secret.");
    Ok(())
}

fn load_signing_key(credential_id: &str) -> AppResult<SigningKey> {
    let encoded = match env::var(SIGNING_KEY_ENV) {
        Ok(value) => Zeroizing::new(value),
        Err(env::VarError::NotPresent) => {
            CredentialVault::new(CREDENTIAL_NAMESPACE)?
                .load(credential_id)?
                .password
        }
        Err(env::VarError::NotUnicode(_)) => {
            return Err("connector signing key environment value is not UTF-8".into());
        }
    };
    let mut decoded = BASE64
        .decode(encoded.as_bytes())
        .map_err(|_| "connector signing key is not valid base64")?;
    if decoded.len() != 32 {
        decoded.zeroize();
        return Err("connector signing key must contain exactly 32 bytes".into());
    }
    let mut bytes = Zeroizing::new([0_u8; 32]);
    bytes.copy_from_slice(&decoded);
    decoded.zeroize();
    Ok(SigningKey::from_bytes(&bytes))
}

fn verify_expected_public_key(path: &Path, signing_key: &SigningKey) -> AppResult<()> {
    let bytes = read_bounded(path, 1024)?;
    let encoded = std::str::from_utf8(&bytes)
        .map_err(|_| "tracked connector public key is not UTF-8")?
        .trim();
    let decoded = BASE64
        .decode(encoded)
        .map_err(|_| "tracked connector public key is not valid base64")?;
    if decoded.as_slice() != signing_key.verifying_key().to_bytes() {
        return Err(
            "connector signing key does not match the tracked production trust root".into(),
        );
    }
    Ok(())
}

fn build_history(
    previous_path: Option<&Path>,
    version: &Version,
    manifests: &[PluginManifestV1],
) -> AppResult<ConnectorHistoryV1> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut history = if let Some(path) = previous_path.filter(|path| path.is_file()) {
        let bytes = read_bounded(path, 4 * 1024 * 1024)?;
        serde_json::from_slice::<ConnectorHistoryV1>(&bytes)?
    } else {
        ConnectorHistoryV1 {
            schema_version: HISTORY_VERSION,
            generated_at: now.clone(),
            versions: Vec::new(),
        }
    };
    if history.schema_version != HISTORY_VERSION {
        return Err("previous connector history version is unsupported".into());
    }
    let plugins = manifests
        .iter()
        .map(|manifest| ConnectorHistoryPluginV1 {
            id: manifest.id.clone(),
            version: manifest.version.clone(),
            size: manifest.size,
            sha256: manifest.sha256.clone(),
            download_url: manifest.download_url.clone(),
        })
        .collect::<Vec<_>>();
    history
        .versions
        .retain(|item| item.version != version.to_string());
    history.versions.push(ConnectorHistoryVersionV1 {
        version: version.to_string(),
        published_at: now.clone(),
        plugins,
    });
    history.versions.sort_by(|left, right| {
        Version::parse(&right.version)
            .ok()
            .cmp(&Version::parse(&left.version).ok())
    });
    history.generated_at = now;
    Ok(history)
}

fn replace_directory(
    destination: &Path,
    populate: impl FnOnce(&Path) -> AppResult<()>,
) -> AppResult<()> {
    let parent = destination
        .parent()
        .ok_or("connector output directory has no parent")?;
    fs::create_dir_all(parent)?;
    let name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or("connector output directory has no valid name")?;
    let temporary = parent.join(format!("{name}.staging-{}", std::process::id()));
    if temporary.exists() {
        fs::remove_dir_all(&temporary)?;
    }
    fs::create_dir(&temporary)?;
    if let Err(error) = populate(&temporary) {
        let _ = fs::remove_dir_all(&temporary);
        return Err(error);
    }
    let previous = parent.join(format!("{name}.previous-{}", std::process::id()));
    if previous.exists() {
        fs::remove_dir_all(&previous)?;
    }
    if destination.exists() {
        fs::rename(destination, &previous)?;
    }
    if let Err(error) = fs::rename(&temporary, destination) {
        if previous.exists() {
            let _ = fs::rename(&previous, destination);
        }
        return Err(error.into());
    }
    if previous.exists() {
        fs::remove_dir_all(previous)?;
    }
    Ok(())
}

fn hash_file(path: &Path) -> AppResult<(u64, String)> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        return Err(format!("connector artifact is empty: {}", path.display()).into());
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let sha256 = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((metadata.len(), sha256))
}

fn read_bounded(path: &Path, maximum_bytes: u64) -> AppResult<Vec<u8>> {
    let metadata = fs::metadata(path)?;
    if metadata.len() == 0 || metadata.len() > maximum_bytes {
        return Err("connector metadata file is empty or exceeds its limit".into());
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len())?);
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn copy_file_synced(source: &Path, destination: &Path) -> AppResult<()> {
    fs::copy(source, destination)?;
    OpenOptions::new()
        .write(true)
        .open(destination)?
        .sync_all()?;
    Ok(())
}

fn write_json_synced(path: &Path, value: &impl Serialize) -> AppResult<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_new_synced(path, &bytes)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> AppResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn normalized_base_url(value: &str) -> AppResult<String> {
    if !value.starts_with("https://")
        || value.contains(['?', '#', '\\'])
        || value.split('/').nth(2).is_none_or(str::is_empty)
    {
        return Err("connector base URL must be an absolute HTTPS URL".into());
    }
    Ok(format!("{}/", value.trim_end_matches('/')))
}

fn parse_generate_options(arguments: &[String]) -> AppResult<GenerateOptions> {
    let values = parse_pairs(arguments)?;
    Ok(GenerateOptions {
        public_key: required_path(&values, "public-key")?,
        credential_id: values
            .get("credential-id")
            .cloned()
            .unwrap_or_else(|| DEFAULT_CREDENTIAL_ID.into()),
    })
}

fn parse_sign_options(arguments: &[String]) -> AppResult<SignOptions> {
    let values = parse_pairs(arguments)?;
    Ok(SignOptions {
        artifacts: required_path(&values, "artifacts")?,
        bundle_output: required_path(&values, "bundle-output")?,
        site_output: values.get("site-output").map(PathBuf::from),
        previous_history: values.get("previous-history").map(PathBuf::from),
        public_key: required_path(&values, "public-key")?,
        version: Version::parse(required(&values, "version")?)?,
        base_url: required(&values, "base-url")?.into(),
        credential_id: values
            .get("credential-id")
            .cloned()
            .unwrap_or_else(|| DEFAULT_CREDENTIAL_ID.into()),
    })
}

fn parse_sync_secret_options(arguments: &[String]) -> AppResult<SyncSecretOptions> {
    let values = parse_pairs(arguments)?;
    Ok(SyncSecretOptions {
        repository: required(&values, "repository")?.into(),
        secret_name: values
            .get("secret-name")
            .cloned()
            .unwrap_or_else(|| SIGNING_KEY_ENV.into()),
        credential_id: values
            .get("credential-id")
            .cloned()
            .unwrap_or_else(|| DEFAULT_CREDENTIAL_ID.into()),
    })
}

fn parse_pairs(arguments: &[String]) -> AppResult<BTreeMap<String, String>> {
    if !arguments.len().is_multiple_of(2) {
        return Err("connector publisher options must be --name value pairs".into());
    }
    let mut values = BTreeMap::new();
    for pair in arguments.chunks_exact(2) {
        let name = pair[0]
            .strip_prefix("--")
            .ok_or("connector publisher option names must start with --")?;
        validate_simple_name(name, "option name")?;
        if values.insert(name.into(), pair[1].clone()).is_some() {
            return Err(format!("duplicate connector publisher option: --{name}").into());
        }
    }
    Ok(values)
}

fn required<'a>(values: &'a BTreeMap<String, String>, name: &str) -> AppResult<&'a str> {
    values
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing required connector publisher option --{name}").into())
}

fn required_path(values: &BTreeMap<String, String>, name: &str) -> AppResult<PathBuf> {
    required(values, name).map(PathBuf::from)
}

fn validate_simple_name(value: &str, label: &str) -> AppResult<()> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_'))
    {
        return Err(format!("{label} contains unsupported characters").into());
    }
    Ok(())
}

fn validate_repository(value: &str) -> AppResult<()> {
    let parts = value.split('/').collect::<Vec<_>>();
    if parts.len() != 2 || parts.iter().any(|part| part.is_empty()) {
        return Err("GitHub repository must use owner/name form".into());
    }
    let unique = parts.into_iter().collect::<BTreeSet<_>>();
    if unique.iter().any(|part| {
        part.bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    }) {
        return Err("GitHub repository contains unsupported characters".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_only_absolute_https_base_urls() {
        assert_eq!(
            normalized_base_url("https://example.test/connectors/v1").expect("valid"),
            "https://example.test/connectors/v1/"
        );
        assert!(normalized_base_url("http://example.test").is_err());
        assert!(normalized_base_url("https://example.test/path?token=secret").is_err());
    }

    #[test]
    fn rejects_duplicate_and_unpaired_options() {
        assert!(
            parse_pairs(&[
                "--version".into(),
                "1.0.0".into(),
                "--version".into(),
                "2.0.0".into()
            ])
            .is_err()
        );
        assert!(parse_pairs(&["--version".into()]).is_err());
    }

    #[test]
    fn descriptors_cover_nine_unique_external_connectors() {
        assert_eq!(OFFICIAL_CONNECTOR_DESCRIPTORS.len(), 9);
        assert_eq!(
            OFFICIAL_CONNECTOR_DESCRIPTORS
                .iter()
                .map(|descriptor| descriptor.id)
                .collect::<BTreeSet<_>>()
                .len(),
            9
        );
        assert!(
            OFFICIAL_CONNECTOR_DESCRIPTORS
                .iter()
                .take(4)
                .all(|descriptor| descriptor.api_version == 2)
        );
        assert!(
            OFFICIAL_CONNECTOR_DESCRIPTORS
                .iter()
                .skip(4)
                .all(|descriptor| descriptor.api_version == 3)
        );
    }

    #[test]
    fn signing_key_must_match_the_tracked_public_key() {
        let path = env::temp_dir().join(format!(
            "ordadb-publisher-public-key-{}.txt",
            std::process::id()
        ));
        let signing_key = SigningKey::from_bytes(&[31_u8; 32]);
        fs::write(&path, BASE64.encode(signing_key.verifying_key().to_bytes()))
            .expect("public key");
        verify_expected_public_key(&path, &signing_key).expect("matching key");
        assert!(verify_expected_public_key(&path, &SigningKey::from_bytes(&[32_u8; 32])).is_err());
        fs::remove_file(path).expect("cleanup public key");
    }
}
