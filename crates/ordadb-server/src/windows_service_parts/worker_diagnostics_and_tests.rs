
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceActionSnapshotV1 {
    action_type: i32,
    delay_milliseconds: u64,
}

fn run_service_worker(
    status_handle: ServiceStatusHandle,
    stop_receiver: mpsc::Receiver<()>,
    data_dir: PathBuf,
) -> std::result::Result<(), ServiceRunFailure> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            ServiceRunFailure::new(
                ServiceStartupPhase::CreateRuntime,
                DbError::new("XX000", "failed to create service runtime")
                    .with_detail(error.to_string()),
            )
        })?;
    runtime.block_on(async move {
        let server = start_server(ServerConfig::new(data_dir))
            .await
            .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::StartServer, error))?;
        set_status(
            &status_handle,
            ServiceState::Running,
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            0,
            Duration::ZERO,
            ServiceExitCode::Win32(0),
        )
        .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::ReportRunning, error))?;
        tokio::task::spawn_blocking(move || stop_receiver.recv())
            .await
            .map_err(|error| {
                ServiceRunFailure::new(
                    ServiceStartupPhase::WaitForStop,
                    DbError::new("XX000", "service stop task failed")
                        .with_detail(error.to_string()),
                )
            })?
            .map_err(|error| {
                ServiceRunFailure::new(
                    ServiceStartupPhase::WaitForStop,
                    DbError::new("XX000", "service stop channel closed")
                        .with_detail(error.to_string()),
                )
            })?;
        set_status(
            &status_handle,
            ServiceState::StopPending,
            ServiceControlAccept::empty(),
            1,
            Duration::from_secs(30),
            ServiceExitCode::Win32(0),
        )
        .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::ReportStopPending, error))?;
        server
            .shutdown()
            .await
            .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::Shutdown, error))?;
        set_status(
            &status_handle,
            ServiceState::Stopped,
            ServiceControlAccept::empty(),
            0,
            Duration::ZERO,
            ServiceExitCode::Win32(0),
        )
        .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::ReportStopped, error))
    })
}

fn configured_process_data_dir() -> Option<PathBuf> {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    arguments.windows(2).find_map(|pair| {
        (pair[0] == "--data-dir" && !pair[1].is_empty()).then(|| PathBuf::from(&pair[1]))
    })
}

fn set_status(
    handle: &ServiceStatusHandle,
    current_state: ServiceState,
    controls_accepted: ServiceControlAccept,
    checkpoint: u32,
    wait_hint: Duration,
    exit_code: ServiceExitCode,
) -> Result<()> {
    handle
        .set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state,
            controls_accepted,
            exit_code,
            checkpoint,
            wait_hint,
            process_id: None,
        })
        .map_err(|error| service_error("failed to update Windows service status", error))
}

fn report_unregistered_service_failure(data_dir: &Path, failure: ServiceRunFailure) -> Result<()> {
    let error = attach_startup_diagnostic(data_dir, &failure);
    Err(error)
}

fn report_registered_service_failure(
    handle: &ServiceStatusHandle,
    data_dir: &Path,
    failure: ServiceRunFailure,
) -> Result<()> {
    let mut error = attach_startup_diagnostic(data_dir, &failure);
    if let Err(status_error) = set_status(
        handle,
        ServiceState::Stopped,
        ServiceControlAccept::empty(),
        0,
        Duration::ZERO,
        ServiceExitCode::ServiceSpecific(SERVICE_SPECIFIC_STARTUP_FAILURE),
    ) {
        error = append_error_context(
            error,
            format!(
                "failed to publish the service-specific startup exit: {}",
                status_error.message
            ),
        );
    }
    Err(error)
}

fn attach_startup_diagnostic(data_dir: &Path, failure: &ServiceRunFailure) -> DbError {
    let diagnostic = ServiceStartupFailureV1::from_error(failure.phase, failure.error.as_ref());
    match write_service_startup_failure(data_dir, &diagnostic) {
        Ok(path) => {
            let diagnostic_hint = format!("inspect the startup diagnostic at {}", path.display());
            let hint = match failure.error.hint.as_deref() {
                Some(hint) => format!("{hint}; {diagnostic_hint}"),
                None => diagnostic_hint,
            };
            failure.error.as_ref().clone().with_hint(hint)
        }
        Err(diagnostic_error) => append_error_context(
            failure.error.as_ref().clone(),
            format!(
                "failed to persist startup diagnostic: {}",
                diagnostic_error.message
            ),
        ),
    }
}

impl ServiceStartupFailureV1 {
    fn from_error(phase: ServiceStartupPhase, error: &DbError) -> Self {
        Self {
            schema_version: STARTUP_FAILURE_SCHEMA_VERSION,
            occurred_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| {
                    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
                }),
            sql_state: bounded_one_line(&error.sql_state, 5),
            phase,
            reason: bounded_one_line(&error.message, MAX_STARTUP_FAILURE_TEXT_BYTES),
            hint: error
                .hint
                .as_deref()
                .map(|hint| bounded_one_line(hint, MAX_STARTUP_FAILURE_TEXT_BYTES)),
            exit_code: SERVICE_SPECIFIC_STARTUP_FAILURE,
        }
    }
}

fn service_startup_failure_path(data_dir: &Path) -> PathBuf {
    let parent = data_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    parent.join("diagnostics").join(STARTUP_FAILURE_FILE)
}

fn write_service_startup_failure(
    data_dir: &Path,
    failure: &ServiceStartupFailureV1,
) -> Result<PathBuf> {
    validate_service_startup_failure(failure)?;
    let bytes = serde_json::to_vec_pretty(failure).map_err(|error| {
        DbError::internal("failed to encode service startup diagnostic")
            .with_detail(error.to_string())
    })?;
    if bytes.len() as u64 > MAX_STARTUP_FAILURE_BYTES {
        return Err(DbError::new(
            "54000",
            "service startup diagnostic exceeds its size limit",
        ));
    }
    let path = service_startup_failure_path(data_dir);
    let parent = path
        .parent()
        .ok_or_else(|| DbError::internal("service startup diagnostic path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| {
        io_error(
            "failed to create service startup diagnostic directory",
            error,
        )
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary = parent.join(format!(
        ".{STARTUP_FAILURE_FILE}.{}.{}.tmp",
        std::process::id(),
        nonce
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                io_error(
                    "failed to create service startup diagnostic temporary file",
                    error,
                )
            })?;
        file.write_all(&bytes).map_err(|error| {
            io_error(
                "failed to write service startup diagnostic temporary file",
                error,
            )
        })?;
        file.sync_all().map_err(|error| {
            io_error(
                "failed to synchronize service startup diagnostic temporary file",
                error,
            )
        })?;
        drop(file);
        atomic_replace(&temporary, &path)
    })();
    if let Err(error) = result {
        if let Err(cleanup_error) = fs::remove_file(&temporary)
            && cleanup_error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(append_error_context(
                error,
                format!("also failed to remove temporary diagnostic: {cleanup_error}"),
            ));
        }
        return Err(error);
    }
    Ok(path)
}

fn read_service_startup_failure(path: &Path) -> Result<ServiceStartupFailureV1> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_error("failed to inspect service startup diagnostic", error))?;
    if metadata.len() > MAX_STARTUP_FAILURE_BYTES {
        return Err(DbError::new(
            "XX001",
            "service startup diagnostic exceeds its size limit",
        ));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| DbError::new("54000", "service startup diagnostic is too large"))?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| io_error("failed to read service startup diagnostic", error))?;
    let failure: ServiceStartupFailureV1 = serde_json::from_slice(&bytes).map_err(|error| {
        DbError::new("XX001", "service startup diagnostic is invalid JSON")
            .with_detail(error.to_string())
    })?;
    validate_service_startup_failure(&failure)?;
    Ok(failure)
}

fn validate_service_startup_failure(failure: &ServiceStartupFailureV1) -> Result<()> {
    if failure.schema_version != STARTUP_FAILURE_SCHEMA_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "service startup diagnostic schema version {} is unsupported",
                failure.schema_version
            ),
        ));
    }
    if failure.sql_state.len() != 5
        || failure.reason.is_empty()
        || failure.reason.len() > MAX_STARTUP_FAILURE_TEXT_BYTES
        || failure
            .hint
            .as_ref()
            .is_some_and(|hint| hint.len() > MAX_STARTUP_FAILURE_TEXT_BYTES)
        || failure.exit_code == 0
    {
        return Err(DbError::new(
            "XX001",
            "service startup diagnostic violates its bounded schema",
        ));
    }
    Ok(())
}

fn bounded_one_line(value: &str, maximum_bytes: usize) -> String {
    let mut output = String::with_capacity(value.len().min(maximum_bytes));
    for character in value.chars() {
        let character = if character.is_control() {
            ' '
        } else {
            character
        };
        if output.len().saturating_add(character.len_utf8()) > maximum_bytes {
            break;
        }
        output.push(character);
    }
    output
}

fn service_exit_code_value(exit_code: ServiceExitCode) -> Option<u32> {
    match exit_code {
        ServiceExitCode::Win32(0) | ServiceExitCode::ServiceSpecific(0) => None,
        ServiceExitCode::Win32(code) | ServiceExitCode::ServiceSpecific(code) => Some(code),
    }
}

fn append_error_context(mut error: DbError, context: impl AsRef<str>) -> DbError {
    let context = bounded_one_line(context.as_ref(), MAX_STARTUP_FAILURE_TEXT_BYTES);
    error.detail = Some(
        match error.detail.take() {
            Some(detail) => format!("{detail}; {context}"),
            None => context,
        }
        .into_boxed_str(),
    );
    error
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let mut source: Vec<u16> = source.as_os_str().encode_wide().collect();
    source.push(0);
    let mut destination: Vec<u16> = destination.as_os_str().encode_wide().collect();
    destination.push(0);
    let flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;
    // SAFETY: both owned buffers are valid NUL-terminated UTF-16 paths for the
    // duration of this same-volume, write-through replacement.
    let moved = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) };
    if moved == 0 {
        return Err(io_error(
            "failed to atomically replace service startup diagnostic",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn service_error(context: &str, error: windows_service::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desired_service_configuration_is_per_machine_local_service() {
        let executable = Path::new(r"C:\Program Files\OrdaDB\ordadb-server.exe");
        let data = Path::new(r"C:\ProgramData\OrdaDB\data");
        let info = desired_service_info(executable, data);
        assert_eq!(info.name, SERVICE_NAME);
        assert_eq!(info.display_name, SERVICE_DISPLAY_NAME);
        assert_eq!(info.service_type, ServiceType::OWN_PROCESS);
        assert_eq!(info.start_type, ServiceStartType::AutoStart);
        assert_eq!(
            info.account_name.as_deref(),
            Some(OsStr::new(SERVICE_ACCOUNT))
        );
        assert_eq!(
            info.launch_arguments,
            vec![
                OsString::from("--service"),
                OsString::from("--data-dir"),
                data.as_os_str().to_owned(),
            ]
        );
    }

    #[test]
    fn service_commands_and_states_have_stable_machine_names() {
        assert_eq!(
            "install".parse::<ServiceCommand>().expect("install"),
            ServiceCommand::Install
        );
        assert_eq!(service_state_name(ServiceState::Running), "running");
        assert_eq!(
            "remove"
                .parse::<ServiceCommand>()
                .expect_err("invalid")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn installer_service_commit_requires_prepared_and_running() {
        validate_installer_commit(
            InstallerServiceTransactionState::Prepared,
            ServiceState::Running,
        )
        .expect("prepared running service");
        assert_eq!(
            validate_installer_commit(
                InstallerServiceTransactionState::Captured,
                ServiceState::Running,
            )
            .expect_err("captured cannot commit")
            .sql_state,
            "25000"
        );
        assert_eq!(
            validate_installer_commit(
                InstallerServiceTransactionState::Prepared,
                ServiceState::Stopped,
            )
            .expect_err("stopped cannot commit")
            .sql_state,
            "55000"
        );
    }

    #[test]
    fn installer_service_rejects_accounts_that_cannot_be_restored_without_a_password() {
        validate_restorable_service_account(Some(OsStr::new("nt authority\\localservice")))
            .expect("LocalService is recoverable");
        assert_eq!(
            validate_restorable_service_account(None)
                .expect_err("LocalSystem cannot be restored explicitly")
                .sql_state,
            "0A000"
        );
        assert_eq!(
            validate_restorable_service_account(Some(OsStr::new("CONTOSO\\database-user")))
                .expect_err("custom account password is unavailable")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn installer_service_rollback_is_upgrade_aware_and_idempotent() {
        assert_eq!(
            installer_rollback_strategy(InstallerServiceTransactionState::Prepared, false)
                .expect("first install"),
            InstallerServiceRollbackStrategy::RemovePreparedService
        );
        assert_eq!(
            installer_rollback_strategy(InstallerServiceTransactionState::Prepared, true)
                .expect("upgrade"),
            InstallerServiceRollbackStrategy::RestorePreviousService
        );
        assert_eq!(
            installer_rollback_strategy(InstallerServiceTransactionState::RolledBack, true)
                .expect("repeated rollback"),
            InstallerServiceRollbackStrategy::AlreadyRolledBack
        );
        assert_eq!(
            installer_rollback_strategy(InstallerServiceTransactionState::Committed, true)
                .expect_err("commit is final")
                .sql_state,
            "25000"
        );
    }

    #[test]
    fn installer_service_transaction_is_bounded_versioned_and_atomic() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("installer-service-transaction-v1.json");
        let mut transaction = InstallerServiceTransactionV1 {
            schema_version: INSTALLER_TRANSACTION_SCHEMA_VERSION,
            state: InstallerServiceTransactionState::Captured,
            originally_existed: false,
            original: None,
            prepared_executable: os_units(OsStr::new(r"C:\Program Files\OrdaDB\ordadb-server.exe")),
            prepared_data_dir: os_units(OsStr::new(r"C:\ProgramData\OrdaDB\data")),
        };
        write_installer_transaction(&path, &transaction).expect("capture");
        transaction.state = InstallerServiceTransactionState::Prepared;
        write_installer_transaction(&path, &transaction).expect("replace");
        assert_eq!(
            read_installer_transaction(&path).expect("read"),
            transaction
        );
        assert!(fs::metadata(&path).expect("metadata").len() <= MAX_INSTALLER_TRANSACTION_BYTES);

        transaction.schema_version += 1;
        assert_eq!(
            validate_installer_transaction(&transaction)
                .expect_err("unknown schema")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn startup_failure_diagnostic_is_bounded_atomic_and_outside_data_authority() {
        let root = tempfile::tempdir().expect("root");
        let data_dir = root.path().join("data");
        fs::create_dir_all(&data_dir).expect("data");
        let error = DbError::new(
            "58030",
            format!(
                "failed\r\nto start {}",
                "x".repeat(MAX_STARTUP_FAILURE_TEXT_BYTES * 2)
            ),
        )
        .with_hint(format!(
            "repair\r\n{}",
            "y".repeat(MAX_STARTUP_FAILURE_TEXT_BYTES * 2)
        ));
        let mut failure =
            ServiceStartupFailureV1::from_error(ServiceStartupPhase::StartServer, &error);
        let path = write_service_startup_failure(&data_dir, &failure).expect("first write");
        assert_eq!(
            path,
            root.path().join("diagnostics").join(STARTUP_FAILURE_FILE)
        );
        assert!(!path.starts_with(&data_dir));

        failure.phase = ServiceStartupPhase::CreateRuntime;
        write_service_startup_failure(&data_dir, &failure).expect("atomic replace");
        let decoded = read_service_startup_failure(&path).expect("read");
        assert_eq!(decoded.phase, ServiceStartupPhase::CreateRuntime);
        assert!(decoded.reason.len() <= MAX_STARTUP_FAILURE_TEXT_BYTES);
        assert!(!decoded.reason.contains('\r'));
        assert!(!decoded.reason.contains('\n'));
        assert!(
            decoded
                .hint
                .as_deref()
                .is_some_and(|hint| hint.len() <= MAX_STARTUP_FAILURE_TEXT_BYTES)
        );
        assert!(fs::metadata(path).expect("metadata").len() <= MAX_STARTUP_FAILURE_BYTES);
    }

    #[test]
    fn startup_failure_schema_and_service_exit_codes_are_strict() {
        let error = DbError::new("XX000", "runtime failed");
        let mut failure =
            ServiceStartupFailureV1::from_error(ServiceStartupPhase::CreateRuntime, &error);
        assert_eq!(
            service_exit_code_value(ServiceExitCode::ServiceSpecific(
                SERVICE_SPECIFIC_STARTUP_FAILURE
            )),
            Some(SERVICE_SPECIFIC_STARTUP_FAILURE)
        );
        assert_eq!(service_exit_code_value(ServiceExitCode::Win32(0)), None);
        failure.schema_version += 1;
        assert_eq!(
            validate_service_startup_failure(&failure)
                .expect_err("unknown schema")
                .sql_state,
            "0A000"
        );
    }
}
