use std::collections::BTreeMap;
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;

use ordadb_backup::{MigrationRunOptionsV2, migrate_v1_to_v2, plan_v1_to_v2, rollback_v2_to_v1};
use ordadb_protocol::{ClientConfig, PgClient};
use ordadb_server::{ServerConfig, bootstrap_pipe_name, request_bootstrap};
use ordadb_types::{DbError, Result};
use serde_json::{Value, json};
use zeroize::Zeroizing;

mod installer_storage;

const MAX_STDIN_SECRET_BYTES: u64 = 1024;

#[tokio::main]
async fn main() {
    if let Err(error) = run(std::env::args().skip(1).collect()).await {
        let output = json!({
            "ok": false,
            "error": error,
        });
        eprintln!(
            "{}",
            serde_json::to_string(&output).unwrap_or_else(|_| {
                r#"{"ok":false,"error":{"sql_state":"XX000","message":"failed to encode CLI error"}}"#
                    .into()
            })
        );
        std::process::exit(1);
    }
}

async fn run(arguments: Vec<String>) -> Result<()> {
    let Some(command) = arguments.first() else {
        return Err(usage());
    };
    let options = parse_options(&arguments[1..])?;
    let output = match command.as_str() {
        "bootstrap" => bootstrap(options).await?,
        "sql" => sql(options)?,
        "health" => health(options).await?,
        "checkpoint" => checkpoint(options).await?,
        "backup" => start_path_operation(options, "backups").await?,
        "restore" => start_path_operation(options, "restores").await?,
        "import" => start_transfer_operation(options, "imports").await?,
        "export" => start_transfer_operation(options, "exports").await?,
        "operations" => operations(options).await?,
        "service" => service(options).await?,
        "storage-migrate" => storage_migrate(options)?,
        "storage-rollback" => storage_rollback(options)?,
        "installer-storage" => installer_storage::run(options)?,
        "validate-config" => validate_config(options)?,
        _ => return Err(usage()),
    };
    println!(
        "{}",
        serde_json::to_string(&json!({ "ok": true, "data": output }))
            .map_err(|error| internal(format!("failed to encode CLI output: {error}")))?
    );
    Ok(())
}

async fn bootstrap(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    require_password_stdin(&mut options)?;
    let data_dir = PathBuf::from(
        take(&mut options, "--data-dir")
            .unwrap_or_else(|| ordadb_server::default_data_dir().display().to_string()),
    );
    let pipe = take(&mut options, "--pipe").unwrap_or_else(|| bootstrap_pipe_name(&data_dir));
    let user = take_required(&mut options, "--user")?;
    ensure_empty(&options)?;
    let password = read_secret_from_stdin()?;
    let response = request_bootstrap(&pipe, user, password).await?;
    if let Some(error) = response.error {
        return Err(error);
    }
    Ok(json!({
        "user": response.user,
        "bootstrapPipe": pipe,
        "closed": response.success,
    }))
}

fn sql(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    require_password_stdin(&mut options)?;
    let address: SocketAddr = take(&mut options, "--addr")
        .unwrap_or_else(|| "127.0.0.1:54329".into())
        .parse()
        .map_err(|_| invalid("--addr must be an IP socket address"))?;
    let user = take_required(&mut options, "--user")?;
    let database = take(&mut options, "--database").unwrap_or_else(|| "ordadb".into());
    let sql = take_required(&mut options, "--sql")?;
    ensure_empty(&options)?;
    let password = read_secret_from_stdin()?;
    let mut client = PgClient::connect(ClientConfig {
        address,
        user,
        database,
        password,
        application_name: "ordadb-cli".into(),
    })?;
    let result = client.query(&sql)?;
    Ok(json!({
        "columns": result.columns,
        "rows": result.rows,
        "commandTags": result.command_tags,
    }))
}

async fn health(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let url = take(&mut options, "--url")
        .unwrap_or_else(|| "http://127.0.0.1:9080/v1/health/ready".into());
    ensure_empty(&options)?;
    get_json(&url, None).await
}

async fn checkpoint(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let base = take(&mut options, "--url").unwrap_or_else(|| "http://127.0.0.1:9080".into());
    let (client, token) = management_session(&mut options, &base).await?;
    ensure_empty(&options)?;
    post_json(&client, &format!("{base}/v1/checkpoint"), &token, None).await
}

async fn start_path_operation(
    mut options: BTreeMap<String, Option<String>>,
    endpoint: &str,
) -> Result<Value> {
    let base = take(&mut options, "--url").unwrap_or_else(|| "http://127.0.0.1:9080".into());
    let path = take_required(&mut options, "--path")?;
    let (client, token) = management_session(&mut options, &base).await?;
    ensure_empty(&options)?;
    post_json(
        &client,
        &format!("{base}/v1/{endpoint}"),
        &token,
        Some(&json!({ "path": path })),
    )
    .await
}

async fn start_transfer_operation(
    mut options: BTreeMap<String, Option<String>>,
    endpoint: &str,
) -> Result<Value> {
    let base = take(&mut options, "--url").unwrap_or_else(|| "http://127.0.0.1:9080".into());
    let schema = take(&mut options, "--schema").unwrap_or_else(|| "public".into());
    let table = take_required(&mut options, "--table")?;
    let path = take_required(&mut options, "--path")?;
    let format = match take(&mut options, "--format")
        .unwrap_or_else(|| "csv".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "csv" => "csv",
        "json" | "jsonl" | "json-lines" => "jsonLines",
        _ => return Err(invalid("--format must be csv or json-lines")),
    };
    let (client, token) = management_session(&mut options, &base).await?;
    ensure_empty(&options)?;
    post_json(
        &client,
        &format!("{base}/v1/{endpoint}"),
        &token,
        Some(&json!({
            "schema": schema,
            "table": table,
            "path": path,
            "format": format,
        })),
    )
    .await
}

async fn operations(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let base = take(&mut options, "--url").unwrap_or_else(|| "http://127.0.0.1:9080".into());
    let operation_id = take(&mut options, "--id");
    let cancel = options.remove("--cancel").is_some();
    let (client, token) = management_session(&mut options, &base).await?;
    ensure_empty(&options)?;
    match (operation_id, cancel) {
        (None, false) => get_json(&format!("{base}/v1/operations"), Some(&token)).await,
        (Some(operation_id), false) => {
            get_json(
                &format!("{base}/v1/operations/{operation_id}"),
                Some(&token),
            )
            .await
        }
        (Some(operation_id), true) => {
            post_json(
                &client,
                &format!("{base}/v1/operations/{operation_id}/cancel"),
                &token,
                None,
            )
            .await
        }
        (None, true) => Err(invalid("--cancel requires --id")),
    }
}

async fn service(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let base = take(&mut options, "--url").unwrap_or_else(|| "http://127.0.0.1:9080".into());
    let (_client, token) = management_session(&mut options, &base).await?;
    ensure_empty(&options)?;
    get_json(&format!("{base}/v1/service"), Some(&token)).await
}

fn storage_migrate(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let data_dir = PathBuf::from(
        take(&mut options, "--data-dir")
            .unwrap_or_else(|| ordadb_server::default_data_dir().display().to_string()),
    );
    let dry_run = options.remove("--dry-run").is_some();
    ensure_empty(&options)?;
    if dry_run {
        serde_json::to_value(plan_v1_to_v2(&data_dir, MigrationRunOptionsV2::default())?)
    } else {
        serde_json::to_value(migrate_v1_to_v2(
            &data_dir,
            MigrationRunOptionsV2::default(),
        )?)
    }
    .map_err(|error| {
        internal(format!(
            "failed to encode storage migration result: {error}"
        ))
    })
}

fn storage_rollback(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let data_dir = PathBuf::from(
        take(&mut options, "--data-dir")
            .unwrap_or_else(|| ordadb_server::default_data_dir().display().to_string()),
    );
    ensure_empty(&options)?;
    rollback_v2_to_v1(&data_dir)?;
    Ok(json!({
        "dataDir": data_dir,
        "rolledBack": true,
    }))
}

async fn management_session(
    options: &mut BTreeMap<String, Option<String>>,
    base: &str,
) -> Result<(reqwest::Client, Zeroizing<String>)> {
    require_password_stdin(options)?;
    let user = take_required(options, "--user")?;
    let password = read_secret_from_stdin()?;
    let client = reqwest::Client::new();
    let token: Value = client
        .post(format!("{base}/v1/auth/token"))
        .json(&json!({
            "username": user,
            "password": password.as_str(),
        }))
        .send()
        .await
        .map_err(|error| network("failed to request management token", error))?
        .error_for_status()
        .map_err(|error| network("management authentication failed", error))?
        .json()
        .await
        .map_err(|error| network("management token response is invalid", error))?;
    let access_token = token["data"]["accessToken"]
        .as_str()
        .ok_or_else(|| protocol("management token response has no access token"))?;
    Ok((client, Zeroizing::new(access_token.to_owned())))
}

async fn post_json(
    client: &reqwest::Client,
    url: &str,
    bearer: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let mut request = client.post(url).bearer_auth(bearer);
    if let Some(body) = body {
        request = request.json(body);
    }
    request
        .send()
        .await
        .map_err(|error| network("management request failed", error))?
        .error_for_status()
        .map_err(|error| network("management request returned an error", error))?
        .json()
        .await
        .map_err(|error| network("management response is invalid", error))
}

fn validate_config(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let data_dir = PathBuf::from(
        take(&mut options, "--data-dir")
            .unwrap_or_else(|| ordadb_server::default_data_dir().display().to_string()),
    );
    let mut config = ServerConfig::new(data_dir);
    if let Some(value) = take(&mut options, "--pg-bind") {
        config.pg_bind = value
            .parse()
            .map_err(|_| invalid("--pg-bind must be an IP socket address"))?;
    }
    if let Some(value) = take(&mut options, "--admin-bind") {
        config.admin_bind = value
            .parse()
            .map_err(|_| invalid("--admin-bind must be an IP socket address"))?;
    }
    ensure_empty(&options)?;
    config.validate()?;
    Ok(json!({
        "dataDir": config.data_dir,
        "pgBind": config.pg_bind,
        "adminBind": config.admin_bind,
        "bootstrapPipe": config.bootstrap_pipe,
        "remoteTlsConfigured": config.tls.is_some(),
    }))
}

async fn get_json(url: &str, bearer: Option<&str>) -> Result<Value> {
    let client = reqwest::Client::new();
    let mut request = client.get(url);
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    request
        .send()
        .await
        .map_err(|error| network("management request failed", error))?
        .error_for_status()
        .map_err(|error| network("management request returned an error", error))?
        .json()
        .await
        .map_err(|error| network("management response is invalid", error))
}

fn parse_options(arguments: &[String]) -> Result<BTreeMap<String, Option<String>>> {
    let mut options = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        let option = &arguments[index];
        if !option.starts_with("--") {
            return Err(invalid(format!("unexpected positional argument {option}")));
        }
        if matches!(
            option.as_str(),
            "--password-stdin" | "--cancel" | "--dry-run"
        ) {
            if options.insert(option.clone(), None).is_some() {
                return Err(invalid(format!("{option} was provided more than once")));
            }
            index += 1;
            continue;
        }
        let value = arguments
            .get(index + 1)
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| invalid(format!("{option} requires a value")))?;
        if options
            .insert(option.clone(), Some(value.clone()))
            .is_some()
        {
            return Err(invalid(format!("{option} was provided more than once")));
        }
        index += 2;
    }
    Ok(options)
}

fn require_password_stdin(options: &mut BTreeMap<String, Option<String>>) -> Result<()> {
    if options.remove("--password-stdin").is_none() {
        return Err(invalid(
            "password input requires the explicit --password-stdin flag",
        ));
    }
    if options.contains_key("--password") {
        return Err(invalid("passwords are forbidden in command-line arguments"));
    }
    Ok(())
}

fn read_secret_from_stdin() -> Result<Zeroizing<String>> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(MAX_STDIN_SECRET_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            DbError::new("58030", "failed to read password from stdin")
                .with_detail(error.to_string())
        })?;
    if bytes.len() as u64 > MAX_STDIN_SECRET_BYTES {
        return Err(invalid("stdin password exceeds 1024 bytes"));
    }
    while matches!(bytes.last(), Some(b'\r' | b'\n')) {
        bytes.pop();
    }
    String::from_utf8(bytes)
        .map(Zeroizing::new)
        .map_err(|_| DbError::new("22021", "stdin password must be valid UTF-8"))
}

fn take(options: &mut BTreeMap<String, Option<String>>, name: &str) -> Option<String> {
    options.remove(name).flatten()
}

fn take_required(options: &mut BTreeMap<String, Option<String>>, name: &str) -> Result<String> {
    take(options, name).ok_or_else(|| invalid(format!("{name} is required")))
}

fn ensure_empty(options: &BTreeMap<String, Option<String>>) -> Result<()> {
    if let Some(option) = options.keys().next() {
        return Err(invalid(format!("unknown option {option}")));
    }
    Ok(())
}

fn usage() -> DbError {
    invalid(
        "usage: ordadb-cli <bootstrap|sql|health|checkpoint|backup|restore|import|export|operations|service|storage-migrate|storage-rollback|installer-storage|validate-config> [options]",
    )
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn protocol(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message)
}

fn network(context: &str, error: reqwest::Error) -> DbError {
    DbError::new("08006", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_must_use_stdin_and_cannot_be_an_option_value() {
        let mut missing = parse_options(&["--user".into(), "dba".into()]).expect("parse");
        assert!(require_password_stdin(&mut missing).is_err());
        let direct = parse_options(&[
            "--user".into(),
            "dba".into(),
            "--password".into(),
            "secret".into(),
            "--password-stdin".into(),
        ])
        .expect("parse");
        let mut direct = direct;
        assert!(require_password_stdin(&mut direct).is_err());
    }

    #[test]
    fn duplicate_and_positional_options_are_rejected() {
        assert!(
            parse_options(&[
                "--user".into(),
                "dba".into(),
                "--user".into(),
                "other".into()
            ])
            .is_err()
        );
        assert!(parse_options(&["dba".into()]).is_err());
    }

    #[test]
    fn dry_run_is_parsed_as_a_flag_without_consuming_data_dir() {
        let mut options = parse_options(&[
            "--data-dir".into(),
            "C:\\ordadb-data".into(),
            "--dry-run".into(),
        ])
        .expect("parse");

        assert!(options.remove("--dry-run").is_some());
        assert_eq!(
            take(&mut options, "--data-dir").as_deref(),
            Some("C:\\ordadb-data")
        );
        ensure_empty(&options).expect("all options consumed");
    }

    #[test]
    fn offline_storage_commands_plan_migrate_and_rollback_without_a_service() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(ordadb_storage::DatabaseStore::open(&root).expect("legacy v1 store"));
        let data_dir = root.display().to_string();

        let dry_run = storage_migrate(BTreeMap::from([
            ("--data-dir".into(), Some(data_dir.clone())),
            ("--dry-run".into(), None),
        ]))
        .expect("dry-run");
        assert_eq!(dry_run["sourceFormatVersion"], 1);
        assert_eq!(dry_run["targetFormatVersion"], 2);
        assert!(!root.join("active-cluster-v2.json").exists());

        let migrated = storage_migrate(BTreeMap::from([(
            "--data-dir".into(),
            Some(data_dir.clone()),
        )]))
        .expect("migrate");
        assert_eq!(migrated["finalPhase"], "completed");
        assert!(root.join("active-cluster-v2.json").is_file());

        let rolled_back = storage_rollback(BTreeMap::from([("--data-dir".into(), Some(data_dir))]))
            .expect("rollback");
        assert_eq!(rolled_back["rolledBack"], true);
        assert!(!root.join("active-cluster-v2.json").exists());
    }
}
