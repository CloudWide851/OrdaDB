
pub fn rollback_installer_service(
    transaction_path: impl AsRef<Path>,
) -> Result<InstallerServiceTransactionStatus> {
    let transaction_path = transaction_path.as_ref();
    let mut transaction = read_installer_transaction(transaction_path)?;
    let manager = installer_service_manager()?;
    let rollback_strategy =
        installer_rollback_strategy(transaction.state, transaction.originally_existed)?;
    if rollback_strategy == InstallerServiceRollbackStrategy::AlreadyRolledBack {
        return installer_transaction_status(&manager, &transaction);
    }
    if let Some(service) = open_optional_service(&manager, ServiceAccess::ALL_ACCESS)? {
        stop_service(&service)?;
        if rollback_strategy == InstallerServiceRollbackStrategy::RestorePreviousService {
            let original = transaction.original.as_ref().ok_or_else(|| {
                DbError::new(
                    "XX001",
                    "installer transaction is missing original service state",
                )
            })?;
            restore_service_configuration(&service, original)?;
        } else if let Err(error) = service.delete()
            && !is_winapi_error(&error, ERROR_SERVICE_MARKED_FOR_DELETE)
        {
            return Err(service_error(
                "failed to remove prepared OrdaDB Windows service",
                error,
            ));
        }
    } else if rollback_strategy == InstallerServiceRollbackStrategy::RestorePreviousService {
        let original = transaction.original.as_ref().ok_or_else(|| {
            DbError::new(
                "XX001",
                "installer transaction is missing original service state",
            )
        })?;
        let info = service_info_from_snapshot(original)?;
        let service = manager
            .create_service(&info, ServiceAccess::ALL_ACCESS)
            .map_err(|error| service_error("failed to recreate previous OrdaDB service", error))?;
        restore_service_secondary_configuration(&service, original)?;
    }
    transaction.state = InstallerServiceTransactionState::RolledBack;
    write_installer_transaction(transaction_path, &transaction)?;
    installer_transaction_status(&manager, &transaction)
}

fn installer_service_manager() -> Result<ServiceManager> {
    ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )
    .map_err(|error| service_error("failed to open Windows Service Control Manager", error))
}

fn validate_installer_commit(
    state: InstallerServiceTransactionState,
    service_state: ServiceState,
) -> Result<()> {
    if state != InstallerServiceTransactionState::Prepared {
        return Err(DbError::new(
            "25000",
            "installer service transaction is not prepared",
        ));
    }
    if service_state != ServiceState::Running {
        return Err(
            DbError::new("55000", "prepared OrdaDB Windows service is not running")
                .with_detail(format!("current service state: {service_state:?}")),
        );
    }
    Ok(())
}

fn installer_rollback_strategy(
    state: InstallerServiceTransactionState,
    originally_existed: bool,
) -> Result<InstallerServiceRollbackStrategy> {
    match state {
        InstallerServiceTransactionState::Captured | InstallerServiceTransactionState::Prepared => {
            Ok(if originally_existed {
                InstallerServiceRollbackStrategy::RestorePreviousService
            } else {
                InstallerServiceRollbackStrategy::RemovePreparedService
            })
        }
        InstallerServiceTransactionState::RolledBack => {
            Ok(InstallerServiceRollbackStrategy::AlreadyRolledBack)
        }
        InstallerServiceTransactionState::Committed => Err(DbError::new(
            "25000",
            "committed installer service transaction cannot be rolled back",
        )),
    }
}

fn validate_installer_paths(transaction: &Path, executable: &Path, data_dir: &Path) -> Result<()> {
    if transaction.as_os_str().is_empty()
        || executable.as_os_str().is_empty()
        || data_dir.as_os_str().is_empty()
    {
        return Err(DbError::new(
            "22023",
            "installer service paths must not be empty",
        ));
    }
    for (label, value) in [
        ("transaction", transaction.as_os_str()),
        ("executable", executable.as_os_str()),
        ("data directory", data_dir.as_os_str()),
    ] {
        if value.encode_wide().count() > MAX_INSTALLER_SERVICE_TEXT_UNITS {
            return Err(DbError::new(
                "54000",
                format!("installer service {label} path is too long"),
            ));
        }
    }
    Ok(())
}

fn capture_service_configuration(
    service: &windows_service::service::Service,
) -> Result<ServiceConfigurationSnapshotV1> {
    let config = service
        .query_config()
        .map_err(|error| service_error("failed to query existing service configuration", error))?;
    if config.load_order_group.is_some() || config.tag_id != 0 {
        return Err(DbError::new(
            "0A000",
            "existing OrdaDB service uses an unsupported load-order group",
        )
        .with_hint("remove the custom load-order group before upgrading OrdaDB"));
    }
    validate_restorable_service_account(config.account_name.as_deref())?;
    let command = parse_windows_command_line(config.executable_path.as_os_str())?;
    let (executable, launch_arguments) = command
        .split_first()
        .ok_or_else(|| DbError::new("XX001", "existing service command line is empty"))?;
    let failure_actions = service.get_failure_actions().map_err(|error| {
        service_error("failed to query existing service failure actions", error)
    })?;
    let failure_actions_on_non_crash_failures = service
        .get_failure_actions_on_non_crash_failures()
        .map_err(|error| {
            service_error(
                "failed to query existing non-crash failure action flag",
                error,
            )
        })?;
    let delayed_auto_start = query_delayed_auto_start(service)?;
    let description = query_service_description(service)?;
    let snapshot = ServiceConfigurationSnapshotV1 {
        service_type: config.service_type.bits(),
        start_type: config.start_type.to_raw(),
        error_control: config.error_control.to_raw(),
        executable: os_units(executable),
        launch_arguments: launch_arguments
            .iter()
            .map(|argument| os_units(argument))
            .collect(),
        dependencies: config
            .dependencies
            .iter()
            .map(|dependency| match dependency {
                ServiceDependency::Service(name) => {
                    ServiceDependencySnapshotV1::Service(os_units(name))
                }
                ServiceDependency::Group(name) => {
                    ServiceDependencySnapshotV1::Group(os_units(name))
                }
            })
            .collect(),
        account_name: config.account_name.as_deref().map(os_units),
        display_name: os_units(&config.display_name),
        failure_actions: failure_actions_snapshot(&failure_actions),
        failure_actions_on_non_crash_failures,
        delayed_auto_start,
        description: description.as_deref().map(os_units),
    };
    validate_service_configuration_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn validate_restorable_service_account(account_name: Option<&OsStr>) -> Result<()> {
    let account_name = account_name.ok_or_else(|| {
        DbError::new(
            "0A000",
            "existing OrdaDB service account cannot be restored transactionally",
        )
        .with_detail("LocalSystem has no explicit account identity in the SCM configuration")
        .with_hint(
            "configure the OrdaDB service to run as NT AUTHORITY\\LocalService before upgrading",
        )
    })?;
    if !account_name
        .to_string_lossy()
        .eq_ignore_ascii_case(SERVICE_ACCOUNT)
    {
        return Err(DbError::new(
            "0A000",
            "existing OrdaDB service account cannot be restored transactionally",
        )
        .with_detail(format!("unsupported service account: {account_name:?}"))
        .with_hint(
            "configure the OrdaDB service to run as NT AUTHORITY\\LocalService before upgrading",
        ));
    }
    Ok(())
}

fn restore_service_configuration(
    service: &windows_service::service::Service,
    snapshot: &ServiceConfigurationSnapshotV1,
) -> Result<()> {
    let info = service_info_from_snapshot(snapshot)?;
    service.change_config(&info).map_err(|error| {
        service_error(
            "failed to restore previous OrdaDB service configuration",
            error,
        )
    })?;
    restore_service_secondary_configuration(service, snapshot)
}

fn restore_service_secondary_configuration(
    service: &windows_service::service::Service,
    snapshot: &ServiceConfigurationSnapshotV1,
) -> Result<()> {
    let actions = failure_actions_from_snapshot(&snapshot.failure_actions)?;
    service.update_failure_actions(actions).map_err(|error| {
        service_error("failed to restore previous service failure actions", error)
    })?;
    service
        .set_failure_actions_on_non_crash_failures(snapshot.failure_actions_on_non_crash_failures)
        .map_err(|error| {
            service_error(
                "failed to restore previous non-crash failure action flag",
                error,
            )
        })?;
    service
        .set_delayed_auto_start(snapshot.delayed_auto_start)
        .map_err(|error| {
            service_error("failed to restore previous delayed auto-start flag", error)
        })?;
    service
        .set_description(
            snapshot
                .description
                .as_deref()
                .map_or_else(OsString::new, |description| {
                    OsString::from_wide(description)
                }),
        )
        .map_err(|error| service_error("failed to restore previous service description", error))
}

fn service_info_from_snapshot(snapshot: &ServiceConfigurationSnapshotV1) -> Result<ServiceInfo> {
    validate_service_configuration_snapshot(snapshot)?;
    let service_type = ServiceType::from_bits(snapshot.service_type)
        .ok_or_else(|| DbError::new("XX001", "installer transaction has invalid service type"))?;
    let start_type = ServiceStartType::from_raw(snapshot.start_type).map_err(|error| {
        DbError::new(
            "XX001",
            "installer transaction has invalid service start type",
        )
        .with_detail(error.to_string())
    })?;
    let error_control = ServiceErrorControl::from_raw(snapshot.error_control).map_err(|error| {
        DbError::new(
            "XX001",
            "installer transaction has invalid service error control",
        )
        .with_detail(error.to_string())
    })?;
    Ok(ServiceInfo {
        name: SERVICE_NAME.into(),
        display_name: OsString::from_wide(&snapshot.display_name),
        service_type,
        start_type,
        error_control,
        executable_path: PathBuf::from(OsString::from_wide(&snapshot.executable)),
        launch_arguments: snapshot
            .launch_arguments
            .iter()
            .map(|argument| OsString::from_wide(argument))
            .collect(),
        dependencies: snapshot
            .dependencies
            .iter()
            .map(|dependency| match dependency {
                ServiceDependencySnapshotV1::Service(name) => {
                    ServiceDependency::Service(OsString::from_wide(name))
                }
                ServiceDependencySnapshotV1::Group(name) => {
                    ServiceDependency::Group(OsString::from_wide(name))
                }
            })
            .collect(),
        account_name: snapshot.account_name.as_deref().map(OsString::from_wide),
        account_password: None,
    })
}

fn failure_actions_snapshot(actions: &ServiceFailureActions) -> ServiceFailureActionsSnapshotV1 {
    ServiceFailureActionsSnapshotV1 {
        reset_after_seconds: match actions.reset_period {
            ServiceFailureResetPeriod::Never => None,
            ServiceFailureResetPeriod::After(duration) => Some(duration.as_secs()),
        },
        reboot_message: actions.reboot_msg.as_deref().map(os_units),
        command: actions.command.as_deref().map(os_units),
        actions: actions.actions.as_ref().map(|actions| {
            actions
                .iter()
                .map(|action| ServiceActionSnapshotV1 {
                    action_type: action.action_type.to_raw(),
                    delay_milliseconds: u64::try_from(action.delay.as_millis()).unwrap_or(u64::MAX),
                })
                .collect()
        }),
    }
}

fn failure_actions_from_snapshot(
    snapshot: &ServiceFailureActionsSnapshotV1,
) -> Result<ServiceFailureActions> {
    let actions = snapshot
        .actions
        .as_ref()
        .map(|actions| {
            actions
                .iter()
                .map(|action| {
                    let action_type =
                        ServiceActionType::from_raw(action.action_type).map_err(|error| {
                            DbError::new(
                                "XX001",
                                "installer transaction has invalid failure action type",
                            )
                            .with_detail(error.to_string())
                        })?;
                    Ok(ServiceAction {
                        action_type,
                        delay: Duration::from_millis(action.delay_milliseconds),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(ServiceFailureActions {
        reset_period: snapshot
            .reset_after_seconds
            .map_or(ServiceFailureResetPeriod::Never, |seconds| {
                ServiceFailureResetPeriod::After(Duration::from_secs(seconds))
            }),
        reboot_msg: snapshot.reboot_message.as_deref().map(OsString::from_wide),
        command: snapshot.command.as_deref().map(OsString::from_wide),
        actions,
    })
}

fn query_delayed_auto_start(service: &windows_service::service::Service) -> Result<bool> {
    let bytes = query_service_config2(service, SERVICE_CONFIG_DELAYED_AUTO_START_INFO)?;
    if bytes.len() < mem::size_of::<SERVICE_DELAYED_AUTO_START_INFO>() {
        return Err(DbError::new(
            "XX001",
            "delayed auto-start service configuration is truncated",
        ));
    }
    // SAFETY: QueryServiceConfig2W initialized at least one complete structure.
    let value =
        unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<SERVICE_DELAYED_AUTO_START_INFO>()) };
    Ok(value.fDelayedAutostart != 0)
}

fn query_service_description(
    service: &windows_service::service::Service,
) -> Result<Option<OsString>> {
    let bytes = query_service_config2(service, SERVICE_CONFIG_DESCRIPTION)?;
    if bytes.len() < mem::size_of::<SERVICE_DESCRIPTIONW>() {
        return Err(DbError::new(
            "XX001",
            "service description configuration is truncated",
        ));
    }
    // SAFETY: QueryServiceConfig2W initialized at least one complete structure; the pointer,
    // when non-null, refers into `bytes` and remains valid during this bounded scan.
    let value = unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<SERVICE_DESCRIPTIONW>()) };
    if value.lpDescription.is_null() {
        return Ok(None);
    }
    let mut length = 0usize;
    // SAFETY: SCM returned a NUL-terminated UTF-16 string within its 8 KiB query buffer.
    unsafe {
        while *value.lpDescription.add(length) != 0 {
            length = length
                .checked_add(1)
                .ok_or_else(|| DbError::new("54000", "service description length overflowed"))?;
            if length > MAX_INSTALLER_SERVICE_TEXT_UNITS {
                return Err(DbError::new("54000", "service description is too long"));
            }
        }
        Ok(Some(OsString::from_wide(std::slice::from_raw_parts(
            value.lpDescription,
            length,
        ))))
    }
}

fn query_service_config2(
    service: &windows_service::service::Service,
    level: u32,
) -> Result<Vec<u8>> {
    let mut bytes = vec![0u8; MAX_SERVICE_CONFIG_BYTES];
    let mut needed = 0u32;
    // SAFETY: `bytes` is writable for its reported length and the service handle remains valid.
    let success = unsafe {
        QueryServiceConfig2W(
            service.raw_handle(),
            level,
            bytes.as_mut_ptr(),
            u32::try_from(bytes.len()).expect("service query buffer fits u32"),
            &mut needed,
        )
    };
    if success == 0 {
        return Err(io_error(
            "failed to query extended service configuration",
            std::io::Error::last_os_error(),
        ));
    }
    let needed = usize::try_from(needed)
        .map_err(|_| DbError::new("54000", "service configuration length is too large"))?;
    if needed > bytes.len() {
        return Err(DbError::new(
            "54000",
            "service configuration exceeds its query limit",
        ));
    }
    bytes.truncate(needed.max(mem::size_of::<usize>()));
    Ok(bytes)
}

fn parse_windows_command_line(command_line: &OsStr) -> Result<Vec<OsString>> {
    let mut wide = os_units(command_line);
    if wide.is_empty() {
        return Err(DbError::new(
            "XX001",
            "existing service command line is empty",
        ));
    }
    wide.push(0);
    let mut count = 0i32;
    // SAFETY: `wide` is a valid NUL-terminated UTF-16 command line and `count` is writable.
    let arguments = unsafe { CommandLineToArgvW(wide.as_ptr(), &mut count) };
    if arguments.is_null() || count <= 0 {
        return Err(io_error(
            "failed to parse existing service command line",
            std::io::Error::last_os_error(),
        ));
    }
    let count = usize::try_from(count)
        .map_err(|_| DbError::new("54000", "service argument count is too large"))?;
    let parsed = if count > MAX_INSTALLER_SERVICE_ARGUMENTS {
        Err(DbError::new(
            "54000",
            "existing service has too many launch arguments",
        ))
    } else {
        let mut parsed = Vec::with_capacity(count);
        for index in 0..count {
            // SAFETY: CommandLineToArgvW returned `count` valid NUL-terminated pointers.
            let argument = unsafe { *arguments.add(index) };
            let mut length = 0usize;
            // SAFETY: each argument is NUL terminated and remains valid until LocalFree.
            let parsed_argument = unsafe {
                while *argument.add(length) != 0 {
                    let Some(next) = length.checked_add(1) else {
                        break;
                    };
                    length = next;
                    if length > MAX_INSTALLER_SERVICE_TEXT_UNITS {
                        break;
                    }
                }
                (length <= MAX_INSTALLER_SERVICE_TEXT_UNITS)
                    .then(|| OsString::from_wide(std::slice::from_raw_parts(argument, length)))
            };
            let Some(parsed_argument) = parsed_argument else {
                // SAFETY: CommandLineToArgvW allocates its array with LocalAlloc.
                unsafe { LocalFree(arguments.cast()) };
                return Err(DbError::new(
                    "54000",
                    "existing service launch argument is too long",
                ));
            };
            parsed.push(parsed_argument);
        }
        Ok(parsed)
    };
    // SAFETY: CommandLineToArgvW allocates its array with LocalAlloc.
    unsafe { LocalFree(arguments.cast()) };
    parsed
}

fn os_units(value: &OsStr) -> Vec<u16> {
    value.encode_wide().collect()
}

fn validate_service_configuration_snapshot(
    snapshot: &ServiceConfigurationSnapshotV1,
) -> Result<()> {
    let text_is_bounded = |value: &[u16]| {
        !value.is_empty() && value.len() <= MAX_INSTALLER_SERVICE_TEXT_UNITS && !value.contains(&0)
    };
    if !text_is_bounded(&snapshot.executable)
        || !text_is_bounded(&snapshot.display_name)
        || snapshot.launch_arguments.len() > MAX_INSTALLER_SERVICE_ARGUMENTS
        || snapshot.launch_arguments.iter().any(|argument| {
            argument.len() > MAX_INSTALLER_SERVICE_TEXT_UNITS || argument.contains(&0)
        })
        || snapshot.dependencies.len() > MAX_INSTALLER_SERVICE_ARGUMENTS
        || snapshot
            .dependencies
            .iter()
            .any(|dependency| match dependency {
                ServiceDependencySnapshotV1::Service(value)
                | ServiceDependencySnapshotV1::Group(value) => !text_is_bounded(value),
            })
        || snapshot
            .account_name
            .as_deref()
            .is_some_and(|value| !text_is_bounded(value))
        || snapshot.description.as_deref().is_some_and(|value| {
            value.len() > MAX_INSTALLER_SERVICE_TEXT_UNITS || value.contains(&0)
        })
        || snapshot
            .failure_actions
            .actions
            .as_ref()
            .is_some_and(|actions| {
                actions.len() > MAX_INSTALLER_SERVICE_ARGUMENTS
                    || actions
                        .iter()
                        .any(|action| action.delay_milliseconds > u64::from(u32::MAX))
            })
        || snapshot
            .failure_actions
            .reset_after_seconds
            .is_some_and(|seconds| seconds >= u64::from(u32::MAX))
        || snapshot
            .failure_actions
            .reboot_message
            .as_deref()
            .is_some_and(|value| {
                value.len() > MAX_INSTALLER_SERVICE_TEXT_UNITS || value.contains(&0)
            })
        || snapshot
            .failure_actions
            .command
            .as_deref()
            .is_some_and(|value| {
                value.len() > MAX_INSTALLER_SERVICE_TEXT_UNITS || value.contains(&0)
            })
    {
        return Err(DbError::new(
            "XX001",
            "installer service transaction violates its bounded service schema",
        ));
    }
    Ok(())
}

fn validate_installer_transaction(transaction: &InstallerServiceTransactionV1) -> Result<()> {
    if transaction.schema_version != INSTALLER_TRANSACTION_SCHEMA_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "installer service transaction schema version {} is unsupported",
                transaction.schema_version
            ),
        ));
    }
    if transaction.originally_existed != transaction.original.is_some()
        || transaction.prepared_executable.is_empty()
        || transaction.prepared_executable.len() > MAX_INSTALLER_SERVICE_TEXT_UNITS
        || transaction.prepared_executable.contains(&0)
        || transaction.prepared_data_dir.is_empty()
        || transaction.prepared_data_dir.len() > MAX_INSTALLER_SERVICE_TEXT_UNITS
        || transaction.prepared_data_dir.contains(&0)
    {
        return Err(DbError::new(
            "XX001",
            "installer service transaction violates its bounded schema",
        ));
    }
    if let Some(original) = transaction.original.as_ref() {
        validate_service_configuration_snapshot(original)?;
    }
    Ok(())
}

fn write_installer_transaction(
    path: &Path,
    transaction: &InstallerServiceTransactionV1,
) -> Result<()> {
    validate_installer_transaction(transaction)?;
    let bytes = serde_json::to_vec_pretty(transaction).map_err(|error| {
        DbError::internal("failed to encode installer service transaction")
            .with_detail(error.to_string())
    })?;
    if bytes.len() as u64 > MAX_INSTALLER_TRANSACTION_BYTES {
        return Err(DbError::new(
            "54000",
            "installer service transaction exceeds its size limit",
        ));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| DbError::new("22023", "installer service transaction path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| {
        io_error(
            "failed to create installer service transaction directory",
            error,
        )
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary = parent.join(format!(
        ".installer-service-{}.{}.tmp",
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
                    "failed to create installer service transaction temporary file",
                    error,
                )
            })?;
        file.write_all(&bytes).map_err(|error| {
            io_error(
                "failed to write installer service transaction temporary file",
                error,
            )
        })?;
        file.sync_all().map_err(|error| {
            io_error("failed to synchronize installer service transaction", error)
        })?;
        drop(file);
        atomic_replace(&temporary, path)
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn read_installer_transaction(path: &Path) -> Result<InstallerServiceTransactionV1> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_error("failed to inspect installer service transaction", error))?;
    if metadata.len() > MAX_INSTALLER_TRANSACTION_BYTES {
        return Err(DbError::new(
            "XX001",
            "installer service transaction exceeds its size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .map_err(|_| DbError::new("54000", "installer service transaction is too large"))?,
    );
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| io_error("failed to read installer service transaction", error))?;
    let transaction: InstallerServiceTransactionV1 =
        serde_json::from_slice(&bytes).map_err(|error| {
            DbError::new("XX001", "installer service transaction is invalid JSON")
                .with_detail(error.to_string())
        })?;
    validate_installer_transaction(&transaction)?;
    Ok(transaction)
}

fn installer_transaction_status(
    manager: &ServiceManager,
    transaction: &InstallerServiceTransactionV1,
) -> Result<InstallerServiceTransactionStatus> {
    let service_state = match open_optional_service(manager, ServiceAccess::QUERY_STATUS)? {
        Some(service) => service_state_name(
            service
                .query_status()
                .map_err(|error| service_error("failed to query OrdaDB service state", error))?
                .current_state,
        )
        .to_owned(),
        None => "not_installed".to_owned(),
    };
    Ok(InstallerServiceTransactionStatus {
        schema_version: INSTALLER_TRANSACTION_SCHEMA_VERSION,
        state: transaction.state.as_str().to_owned(),
        originally_existed: transaction.originally_existed,
        service_state,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallerServiceTransactionStatus {
    pub schema_version: u32,
    pub state: String,
    pub originally_existed: bool,
    pub service_state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum InstallerServiceTransactionState {
    Captured,
    Prepared,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallerServiceRollbackStrategy {
    RemovePreparedService,
    RestorePreviousService,
    AlreadyRolledBack,
}

impl InstallerServiceTransactionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Captured => "captured",
            Self::Prepared => "prepared",
            Self::Committed => "committed",
            Self::RolledBack => "rolledBack",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstallerServiceTransactionV1 {
    schema_version: u32,
    state: InstallerServiceTransactionState,
    originally_existed: bool,
    original: Option<ServiceConfigurationSnapshotV1>,
    prepared_executable: Vec<u16>,
    prepared_data_dir: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceConfigurationSnapshotV1 {
    service_type: u32,
    start_type: u32,
    error_control: u32,
    executable: Vec<u16>,
    launch_arguments: Vec<Vec<u16>>,
    dependencies: Vec<ServiceDependencySnapshotV1>,
    account_name: Option<Vec<u16>>,
    display_name: Vec<u16>,
    failure_actions: ServiceFailureActionsSnapshotV1,
    failure_actions_on_non_crash_failures: bool,
    delayed_auto_start: bool,
    description: Option<Vec<u16>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
enum ServiceDependencySnapshotV1 {
    Service(Vec<u16>),
    Group(Vec<u16>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceFailureActionsSnapshotV1 {
    reset_after_seconds: Option<u64>,
    reboot_message: Option<Vec<u16>>,
    command: Option<Vec<u16>>,
    actions: Option<Vec<ServiceActionSnapshotV1>>,
}
