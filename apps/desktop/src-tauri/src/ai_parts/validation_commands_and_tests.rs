
fn required_string<'a>(call: &'a ValidatedAiToolCall, field: &str) -> Result<&'a str> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| DbError::new("22023", format!("AI tool field {field} must be a string")))?;
    validate_text(value, 1024 * 1024, &format!("AI tool field {field}"))?;
    Ok(value)
}

fn required_string_allow_empty<'a>(
    call: &'a ValidatedAiToolCall,
    field: &str,
    maximum: usize,
) -> Result<&'a str> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| DbError::new("22023", format!("AI tool field {field} must be a string")))?;
    if value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(DbError::new(
            "22023",
            format!("AI tool field {field} is invalid"),
        ));
    }
    Ok(value)
}

fn required_usize(call: &ValidatedAiToolCall, field: &str) -> Result<usize> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| DbError::new("22023", format!("AI tool field {field} must be positive")))?;
    usize::try_from(value).map_err(|_| DbError::new("22023", "AI integer is too large"))
}

fn parse_transfer_format(value: &str) -> Result<AdministrationTransferFormat> {
    match value {
        "csv" => Ok(AdministrationTransferFormat::Csv),
        "jsonLines" => Ok(AdministrationTransferFormat::JsonLines),
        _ => Err(DbError::new(
            "22023",
            "transfer format must be csv or jsonLines",
        )),
    }
}

async fn choose_operation_file(
    root: &Path,
    kind: AdministrationOperationKind,
    format: Option<AdministrationTransferFormat>,
) -> Result<PathBuf> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut dialog = rfd::FileDialog::new().set_directory(&root);
        dialog = match (kind, format) {
            (AdministrationOperationKind::Backup, _) => dialog
                .add_filter("OrdaDB backup", &["ordbak"])
                .set_file_name("ordadb-backup.ordbak"),
            (AdministrationOperationKind::Restore, _) => {
                dialog.add_filter("OrdaDB backup", &["ordbak"])
            }
            (
                AdministrationOperationKind::Import | AdministrationOperationKind::Export,
                Some(AdministrationTransferFormat::Csv),
            ) => dialog
                .add_filter("CSV", &["csv"])
                .set_file_name("ordadb-data.csv"),
            (
                AdministrationOperationKind::Import | AdministrationOperationKind::Export,
                Some(AdministrationTransferFormat::JsonLines),
            ) => dialog
                .add_filter("JSON Lines", &["jsonl", "ndjson"])
                .set_file_name("ordadb-data.jsonl"),
            _ => dialog,
        };
        let selected = if matches!(
            kind,
            AdministrationOperationKind::Restore | AdministrationOperationKind::Import
        ) {
            dialog.pick_file()
        } else {
            dialog.save_file()
        };
        selected.ok_or_else(|| DbError::new("57014", "native file selection was cancelled"))
    })
    .await
    .map_err(join_error)?
}

fn relative_operation_path(
    root: &Path,
    selected: &Path,
    kind: AdministrationOperationKind,
) -> Result<String> {
    let root = fs::canonicalize(root)
        .map_err(|error| io_error("failed to resolve operations root", error))?;
    let selected = if matches!(
        kind,
        AdministrationOperationKind::Restore | AdministrationOperationKind::Import
    ) {
        fs::canonicalize(selected)
            .map_err(|error| io_error("failed to resolve selected operation source", error))?
    } else {
        let parent = selected
            .parent()
            .ok_or_else(|| DbError::new("22023", "selected destination has no parent"))?;
        fs::canonicalize(parent)
            .map_err(|error| io_error("failed to resolve selected destination directory", error))?
            .join(
                selected.file_name().ok_or_else(|| {
                    DbError::new("22023", "selected destination has no file name")
                })?,
            )
    };
    let relative = selected.strip_prefix(&root).map_err(|_| {
        DbError::new(
            "22023",
            "selected operation file must stay within the server operations root",
        )
        .with_hint(format!("choose a file under {}", root.display()))
    })?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DbError::new("22023", "selected operation path is invalid"));
    }
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn local_output(content: JsonValue, summary: impl Into<String>) -> Result<AiToolOutput> {
    let bytes_retained = encoded_len(&content)?;
    Ok(AiToolOutput {
        content,
        rows_retained: 0,
        total_rows: 0,
        bytes_retained,
        truncated: false,
        summary: bounded_text(&summary.into(), 8 * 1024),
        disclosure: None,
    })
}

fn append_history(
    console: &ConsoleRuntime,
    persistence: &Mutex<AiPersistenceV1>,
    entry: AiHistoryEntry,
) -> Result<()> {
    update_persistence(console, persistence, |state| state.history.push(entry))
}

fn update_persistence(
    console: &ConsoleRuntime,
    persistence: &Mutex<AiPersistenceV1>,
    update: impl FnOnce(&mut AiPersistenceV1),
) -> Result<()> {
    let mut state = mutex_lock(persistence, "AI persistence")?;
    let mut candidate = state.clone();
    update(&mut candidate);
    let candidate = project_persistence(candidate.history, candidate.audit)?;
    console.save_ai_state(&candidate)?;
    *state = candidate;
    Ok(())
}

fn sensitive_columns(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| {
            let normalized = column.to_ascii_lowercase();
            [
                "password",
                "secret",
                "token",
                "credential",
                "api_key",
                "apikey",
            ]
            .iter()
            .any(|marker| normalized.contains(marker))
        })
        .cloned()
        .collect()
}

fn mask_positional_columns(
    mut content: JsonValue,
    columns: &[String],
    masked_columns: &[String],
) -> JsonValue {
    let masked = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            masked_columns
                .iter()
                .any(|masked| masked.eq_ignore_ascii_case(column))
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if let Some(items) = content.get_mut("items").and_then(JsonValue::as_array_mut) {
        for item in items {
            if let Some(row) = item.as_array_mut() {
                for index in &masked {
                    if let Some(value) = row.get_mut(*index)
                        && !value.is_null()
                    {
                        *value = JsonValue::String("<redacted>".to_owned());
                    }
                }
            }
        }
    }
    content
}

fn ensure_approved(approved: bool) -> Result<()> {
    if approved {
        Ok(())
    } else {
        Err(DbError::new(
            "42501",
            "AI mutation requires a fresh payload-bound approval",
        ))
    }
}

fn bounded_history_text(value: &str) -> String {
    bounded_text(value, MAX_HISTORY_TEXT_BYTES)
}

fn bounded_text(value: &str, maximum: usize) -> String {
    let mut end = value.len().min(maximum);
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value[..end]
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\t') {
                ' '
            } else {
                character
            }
        })
        .collect()
}

fn encoded_len(value: &JsonValue) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(json_error)
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn validate_identifier(value: &str, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(DbError::new("22023", format!("{context} is invalid")));
    }
    Ok(())
}

fn validate_text(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(DbError::new("22023", format!("{context} is invalid")));
    }
    Ok(())
}

fn mutex_lock<'a, T>(mutex: &'a Mutex<T>, context: &str) -> Result<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::new("XX000", format!("{context} lock was poisoned")))
}

fn join_error(error: tokio::task::JoinError) -> DbError {
    DbError::new("XX000", "AI desktop task failed").with_detail(error.to_string())
}

fn json_error(error: serde_json::Error) -> DbError {
    DbError::new("XX000", "failed to encode AI desktop projection").with_detail(error.to_string())
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_start_run(
    app: AppHandle,
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    request: AiRunRequest,
) -> std::result::Result<AiRunStarted, DbmsError> {
    runtime.start_run(app, request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_cancel_run(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    run_id: String,
) -> std::result::Result<(), DbmsError> {
    runtime.cancel_run(&run_id).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_decide(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    decision: AiApprovalDecision,
) -> std::result::Result<(), DbmsError> {
    runtime.decide(decision).map_err(Into::into)
}

#[tauri::command]
pub fn ai_state(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
) -> std::result::Result<AiPersistenceV1, DbmsError> {
    runtime.state().map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn ai_prompt_credential(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    request: AiCredentialPromptRequest,
) -> std::result::Result<Option<AiCredentialStatus>, DbmsError> {
    runtime.prompt_credential(request).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_credential_status(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    credential_id: String,
) -> std::result::Result<AiCredentialStatus, DbmsError> {
    runtime
        .credential_status(&credential_id)
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_delete_credential(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    credential_id: String,
) -> std::result::Result<(), DbmsError> {
    runtime
        .delete_credential(&credential_id)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use ordadb_ai::{AiToolCall, validate_tool_arguments};

    use super::*;

    #[test]
    fn fixed_tool_registry_is_strict_and_complete() {
        let registry = tool_registry().expect("registry");
        let definitions = registry.definitions();
        assert_eq!(definitions.len(), 14);
        assert!(definitions.iter().all(|definition| {
            definition.parameters["additionalProperties"] == JsonValue::Bool(false)
        }));
        let query = definitions
            .iter()
            .find(|definition| definition.name == TOOL_QUERY)
            .expect("query tool");
        assert!(
            validate_tool_arguments(
                query,
                AiToolCall {
                    call_id: "call-1".to_owned(),
                    name: TOOL_QUERY.to_owned(),
                    arguments: json!({"command": "SELECT 1", "extra": true}),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn automatic_sql_reads_fail_closed_for_access_and_effect() {
        let native = AiConnectionPolicy {
            connector_kind: "sql".to_owned(),
            command_language: "postgresql".to_owned(),
            credential_access: CredentialAccess::ReadWrite,
            native: true,
        };
        assert!(command_is_auto_read(&native, "SELECT 1"));
        assert!(!command_is_auto_read(&native, "DELETE FROM public.items"));

        let external = AiConnectionPolicy {
            native: false,
            credential_access: CredentialAccess::Unspecified,
            ..native.clone()
        };
        assert!(!command_is_auto_read(&external, "SELECT 1"));
        let read_only = AiConnectionPolicy {
            credential_access: CredentialAccess::ReadOnly,
            ..external
        };
        assert!(command_is_auto_read(&read_only, "SELECT 1"));
        assert!(!command_is_auto_read(
            &read_only,
            "SELECT nextval('items_id_seq')"
        ));
    }

    #[test]
    fn redis_like_arguments_are_parsed_without_invoking_a_shell() {
        assert_eq!(
            split_command_arguments(r#"SET "display name" "Ada Lovelace""#).expect("arguments"),
            vec!["SET", "display name", "Ada Lovelace"]
        );
        assert!(split_command_arguments("SET 'unterminated").is_err());
    }

    #[test]
    fn operation_paths_must_remain_under_the_server_root() {
        let root = tempfile::tempdir().expect("root");
        let inside = root.path().join("backup.ordbak");
        assert_eq!(
            relative_operation_path(root.path(), &inside, AdministrationOperationKind::Backup)
                .expect("relative"),
            "backup.ordbak"
        );
        let outside = tempfile::tempdir().expect("outside");
        assert!(
            relative_operation_path(
                root.path(),
                &outside.path().join("backup.ordbak"),
                AdministrationOperationKind::Backup,
            )
            .is_err()
        );
    }
}
