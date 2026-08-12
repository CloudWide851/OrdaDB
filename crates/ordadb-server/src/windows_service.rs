use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::mem;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use windows_service::define_windows_service;
use windows_service::service::{
    ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept,
    ServiceDependency, ServiceErrorControl, ServiceExitCode, ServiceFailureActions,
    ServiceFailureResetPeriod, ServiceInfo, ServiceStartType, ServiceState, ServiceStatus,
    ServiceType,
};
use windows_service::service_control_handler::{
    self, ServiceControlHandlerResult, ServiceStatusHandle,
};
use windows_service::service_dispatcher;
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};
use windows_sys::Win32::Foundation::LocalFree;
use windows_sys::Win32::System::Services::{
    QueryServiceConfig2W, SERVICE_CONFIG_DELAYED_AUTO_START_INFO, SERVICE_CONFIG_DESCRIPTION,
    SERVICE_DELAYED_AUTO_START_INFO, SERVICE_DESCRIPTIONW,
};
use windows_sys::Win32::UI::Shell::CommandLineToArgvW;

use crate::{
    ServerConfig, default_data_dir, join_server_worker, spawn_server_worker, start_server,
};

pub const SERVICE_NAME: &str = "OrdaDB";
pub const SERVICE_DISPLAY_NAME: &str = "OrdaDB Database Service";
pub const SERVICE_ACCOUNT: &str = "NT AUTHORITY\\LocalService";
pub const SERVICE_START_MODE: &str = "auto-delayed";
pub const SERVICE_FAILURE_ACTIONS: &str = "restart/5000,restart/15000,restart/60000";
const SERVICE_DESCRIPTION: &str =
    "OrdaDB single-machine relational database and PostgreSQL-compatible endpoint";
const SERVICE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const ERROR_SERVICE_DOES_NOT_EXIST: i32 = 1060;
const ERROR_SERVICE_ALREADY_RUNNING: i32 = 1056;
const ERROR_SERVICE_NOT_ACTIVE: i32 = 1062;
const ERROR_SERVICE_MARKED_FOR_DELETE: i32 = 1072;
const SERVICE_SPECIFIC_STARTUP_FAILURE: u32 = 0x0DB0;
const STARTUP_FAILURE_SCHEMA_VERSION: u32 = 1;
const STARTUP_FAILURE_FILE: &str = "last-startup-failure-v1.json";
const MAX_STARTUP_FAILURE_BYTES: u64 = 16 * 1024;
const MAX_STARTUP_FAILURE_TEXT_BYTES: usize = 1024;
const INSTALLER_TRANSACTION_SCHEMA_VERSION: u32 = 1;
const MAX_INSTALLER_TRANSACTION_BYTES: u64 = 64 * 1024;
const MAX_INSTALLER_SERVICE_ARGUMENTS: usize = 64;
const MAX_INSTALLER_SERVICE_TEXT_UNITS: usize = 16 * 1024;
const MAX_SERVICE_CONFIG_BYTES: usize = 8 * 1024;

define_windows_service!(ffi_service_main, service_main);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceCommand {
    Install,
    Update,
    Start,
    Stop,
    Uninstall,
    Status,
}

impl std::str::FromStr for ServiceCommand {
    type Err = DbError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "install" => Ok(Self::Install),
            "update" => Ok(Self::Update),
            "start" => Ok(Self::Start),
            "stop" => Ok(Self::Stop),
            "uninstall" => Ok(Self::Uninstall),
            "status" => Ok(Self::Status),
            _ => Err(DbError::new(
                "22023",
                "service command must be install, update, start, stop, uninstall, or status",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsServiceStatus {
    pub installed: bool,
    pub state: String,
    pub process_id: Option<u32>,
    pub account: &'static str,
    pub start_mode: &'static str,
    pub failure_actions: &'static str,
    pub executable_path: PathBuf,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub exit_code: Option<u32>,
    #[serde(default)]
    pub failure_phase: Option<String>,
    #[serde(default)]
    pub diagnostic_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ServiceStartupPhase {
    RegisterControlHandler,
    ReportStartPending,
    CreateWorker,
    CreateRuntime,
    StartServer,
    ReportRunning,
    WaitForStop,
    ReportStopPending,
    Shutdown,
    ReportStopped,
    JoinWorker,
}

impl ServiceStartupPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RegisterControlHandler => "registerControlHandler",
            Self::ReportStartPending => "reportStartPending",
            Self::CreateWorker => "createWorker",
            Self::CreateRuntime => "createRuntime",
            Self::StartServer => "startServer",
            Self::ReportRunning => "reportRunning",
            Self::WaitForStop => "waitForStop",
            Self::ReportStopPending => "reportStopPending",
            Self::Shutdown => "shutdown",
            Self::ReportStopped => "reportStopped",
            Self::JoinWorker => "joinWorker",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStartupFailureV1 {
    pub schema_version: u32,
    pub occurred_at_unix_ms: u64,
    pub sql_state: String,
    pub phase: ServiceStartupPhase,
    pub reason: String,
    pub hint: Option<String>,
    pub exit_code: u32,
}

struct ServiceRunFailure {
    phase: ServiceStartupPhase,
    error: Box<DbError>,
}

impl ServiceRunFailure {
    fn new(phase: ServiceStartupPhase, error: DbError) -> Self {
        Self {
            phase,
            error: Box::new(error),
        }
    }
}

pub fn dispatch_windows_service() -> Result<()> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .map_err(|error| service_error("failed to enter Windows service dispatcher", error))
}

pub fn manage_windows_service(
    command: ServiceCommand,
    executable_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
) -> Result<WindowsServiceStatus> {
    let executable_path = executable_path.as_ref();
    let data_dir = data_dir.as_ref();
    if matches!(command, ServiceCommand::Install | ServiceCommand::Update)
        && !executable_path.is_file()
    {
        return Err(DbError::new(
            "22023",
            "Windows service executable must be an existing file",
        ));
    }
    if data_dir.as_os_str().is_empty() {
        return Err(DbError::new(
            "22023",
            "Windows service data directory must not be empty",
        ));
    }
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )
    .map_err(|error| service_error("failed to open Windows Service Control Manager", error))?;

    match command {
        ServiceCommand::Install | ServiceCommand::Update => {
            let info = desired_service_info(executable_path, data_dir);
            let service = match manager.open_service(SERVICE_NAME, ServiceAccess::ALL_ACCESS) {
                Ok(service) => {
                    service.change_config(&info).map_err(|error| {
                        service_error("failed to update OrdaDB Windows service", error)
                    })?;
                    service
                }
                Err(error) if is_winapi_error(&error, ERROR_SERVICE_DOES_NOT_EXIST) => manager
                    .create_service(&info, ServiceAccess::ALL_ACCESS)
                    .map_err(|error| {
                        service_error("failed to install OrdaDB Windows service", error)
                    })?,
                Err(error) => {
                    return Err(service_error(
                        "failed to open OrdaDB Windows service",
                        error,
                    ));
                }
            };
            configure_service(&service)?;
        }
        ServiceCommand::Start => {
            let service = manager
                .open_service(
                    SERVICE_NAME,
                    ServiceAccess::START | ServiceAccess::QUERY_STATUS,
                )
                .map_err(|error| service_error("failed to open OrdaDB Windows service", error))?;
            let status = service
                .query_status()
                .map_err(|error| service_error("failed to query OrdaDB Windows service", error))?;
            if status.current_state != ServiceState::Running {
                if let Err(error) = service.start::<&OsStr>(&[])
                    && !is_winapi_error(&error, ERROR_SERVICE_ALREADY_RUNNING)
                {
                    return Err(service_error(
                        "failed to start OrdaDB Windows service",
                        error,
                    ));
                }
                wait_for_state(&service, ServiceState::Running, Some(data_dir))?;
            }
        }
        ServiceCommand::Stop => {
            if let Some(service) =
                open_optional_service(&manager, ServiceAccess::STOP | ServiceAccess::QUERY_STATUS)?
            {
                stop_service(&service)?;
            }
        }
        ServiceCommand::Uninstall => {
            if let Some(service) = open_optional_service(
                &manager,
                ServiceAccess::STOP | ServiceAccess::QUERY_STATUS | ServiceAccess::DELETE,
            )? {
                stop_service(&service)?;
                if let Err(error) = service.delete()
                    && !is_winapi_error(&error, ERROR_SERVICE_MARKED_FOR_DELETE)
                {
                    return Err(service_error(
                        "failed to uninstall OrdaDB Windows service",
                        error,
                    ));
                }
            }
        }
        ServiceCommand::Status => {}
    }
    service_status(&manager, executable_path, data_dir)
}

fn desired_service_info(executable_path: &Path, data_dir: &Path) -> ServiceInfo {
    ServiceInfo {
        name: SERVICE_NAME.into(),
        display_name: SERVICE_DISPLAY_NAME.into(),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: executable_path.to_path_buf(),
        launch_arguments: vec![
            "--service".into(),
            "--data-dir".into(),
            data_dir.as_os_str().to_owned(),
        ],
        dependencies: Vec::<ServiceDependency>::new(),
        account_name: Some(SERVICE_ACCOUNT.into()),
        account_password: None,
    }
}

fn configure_service(service: &windows_service::service::Service) -> Result<()> {
    service
        .set_description(SERVICE_DESCRIPTION)
        .map_err(|error| service_error("failed to set OrdaDB service description", error))?;
    service
        .set_delayed_auto_start(true)
        .map_err(|error| service_error("failed to set delayed automatic start", error))?;
    service
        .update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(24 * 60 * 60)),
            reboot_msg: None,
            command: None,
            actions: Some(vec![
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(5),
                },
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(15),
                },
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(60),
                },
            ]),
        })
        .map_err(|error| service_error("failed to set service failure actions", error))?;
    service
        .set_failure_actions_on_non_crash_failures(true)
        .map_err(|error| {
            service_error(
                "failed to enable service actions for non-crash failures",
                error,
            )
        })
}

fn open_optional_service(
    manager: &ServiceManager,
    access: ServiceAccess,
) -> Result<Option<windows_service::service::Service>> {
    match manager.open_service(SERVICE_NAME, access) {
        Ok(service) => Ok(Some(service)),
        Err(error)
            if is_winapi_error(&error, ERROR_SERVICE_DOES_NOT_EXIST)
                || is_winapi_error(&error, ERROR_SERVICE_MARKED_FOR_DELETE) =>
        {
            Ok(None)
        }
        Err(error) => Err(service_error(
            "failed to open OrdaDB Windows service",
            error,
        )),
    }
}

fn stop_service(service: &windows_service::service::Service) -> Result<()> {
    let status = service
        .query_status()
        .map_err(|error| service_error("failed to query OrdaDB Windows service", error))?;
    if status.current_state == ServiceState::Stopped {
        return Ok(());
    }
    if status.current_state != ServiceState::StopPending
        && let Err(error) = service.stop()
        && !is_winapi_error(&error, ERROR_SERVICE_NOT_ACTIVE)
    {
        return Err(service_error(
            "failed to stop OrdaDB Windows service",
            error,
        ));
    }
    wait_for_state(service, ServiceState::Stopped, None).map(|_| ())
}

fn wait_for_state(
    service: &windows_service::service::Service,
    target: ServiceState,
    data_dir: Option<&Path>,
) -> Result<ServiceStatus> {
    let deadline = Instant::now()
        .checked_add(SERVICE_STATE_TIMEOUT)
        .ok_or_else(|| DbError::new("54000", "service state timeout overflowed"))?;
    loop {
        let status = service
            .query_status()
            .map_err(|error| service_error("failed to query OrdaDB Windows service", error))?;
        if status.current_state == target {
            return Ok(status);
        }
        if status.current_state == ServiceState::Stopped {
            let exit_code = service_exit_code_value(status.exit_code).unwrap_or_default();
            let mut error = DbError::new(
                "58030",
                format!("OrdaDB Windows service stopped before reaching {target:?}"),
            )
            .with_detail(format!("service exit code: {exit_code}"));
            if let Some(data_dir) = data_dir {
                let path = service_startup_failure_path(data_dir);
                error = if path.is_file() {
                    match read_service_startup_failure(&path) {
                        Ok(failure) => {
                            let detail = format!(
                                "service exit code: {exit_code}; phase: {}; reason: {}",
                                failure.phase.as_str(),
                                failure.reason
                            );
                            error.detail = Some(detail.into_boxed_str());
                            let diagnostic_hint =
                                format!("inspect the startup diagnostic at {}", path.display());
                            error.with_hint(match failure.hint {
                                Some(hint) => format!("{hint}; {diagnostic_hint}"),
                                None => diagnostic_hint,
                            })
                        }
                        Err(diagnostic_error) => append_error_context(
                            error.with_hint(format!(
                                "inspect the startup diagnostic at {}",
                                path.display()
                            )),
                            format!(
                                "startup diagnostic could not be decoded: {}",
                                diagnostic_error.message
                            ),
                        ),
                    }
                } else {
                    error.with_hint(format!(
                        "startup diagnostic was not written; expected {}",
                        path.display()
                    ))
                };
            }
            return Err(error);
        }
        if Instant::now() >= deadline {
            return Err(DbError::new(
                "57014",
                format!("timed out waiting for OrdaDB service to become {target:?}"),
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn service_status(
    manager: &ServiceManager,
    executable_path: &Path,
    data_dir: &Path,
) -> Result<WindowsServiceStatus> {
    let Some(service) = open_optional_service(manager, ServiceAccess::QUERY_STATUS)? else {
        return Ok(WindowsServiceStatus {
            installed: false,
            state: "not_installed".into(),
            process_id: None,
            account: SERVICE_ACCOUNT,
            start_mode: SERVICE_START_MODE,
            failure_actions: SERVICE_FAILURE_ACTIONS,
            executable_path: executable_path.to_path_buf(),
            data_dir: data_dir.to_path_buf(),
            exit_code: None,
            failure_phase: None,
            diagnostic_path: None,
        });
    };
    let status = service
        .query_status()
        .map_err(|error| service_error("failed to query OrdaDB Windows service", error))?;
    let exit_code = service_exit_code_value(status.exit_code);
    let diagnostic_path = exit_code.map(|_| service_startup_failure_path(data_dir));
    let failure_phase = diagnostic_path
        .as_deref()
        .filter(|path| path.is_file())
        .map(read_service_startup_failure)
        .transpose()?
        .map(|failure| failure.phase.as_str().to_owned());
    Ok(WindowsServiceStatus {
        installed: true,
        state: service_state_name(status.current_state).into(),
        process_id: status.process_id,
        account: SERVICE_ACCOUNT,
        start_mode: SERVICE_START_MODE,
        failure_actions: SERVICE_FAILURE_ACTIONS,
        executable_path: executable_path.to_path_buf(),
        data_dir: data_dir.to_path_buf(),
        exit_code,
        failure_phase,
        diagnostic_path,
    })
}

const fn service_state_name(state: ServiceState) -> &'static str {
    match state {
        ServiceState::Stopped => "stopped",
        ServiceState::StartPending => "start_pending",
        ServiceState::StopPending => "stop_pending",
        ServiceState::Running => "running",
        ServiceState::ContinuePending => "continue_pending",
        ServiceState::PausePending => "pause_pending",
        ServiceState::Paused => "paused",
    }
}

fn is_winapi_error(error: &windows_service::Error, code: i32) -> bool {
    matches!(error, windows_service::Error::Winapi(error) if error.raw_os_error() == Some(code))
}

fn service_main(arguments: Vec<OsString>) {
    if let Err(error) = run_service(arguments) {
        eprintln!("{}: {}", error.sql_state, error.message);
        if let Some(detail) = error.detail {
            eprintln!("{detail}");
        }
        if let Some(hint) = error.hint {
            eprintln!("HINT: {hint}");
        }
    }
}

fn run_service(arguments: Vec<OsString>) -> Result<()> {
    let data_dir = configured_process_data_dir()
        .or_else(|| {
            arguments
                .get(1)
                .filter(|value| !value.is_empty())
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(default_data_dir);
    let (stop_sender, stop_receiver) = mpsc::channel::<()>();
    let status_handle =
        match service_control_handler::register(SERVICE_NAME, move |control| match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                if stop_sender.send(()).is_ok() {
                    ServiceControlHandlerResult::NoError
                } else {
                    ServiceControlHandlerResult::Other(1062)
                }
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }) {
            Ok(handle) => handle,
            Err(error) => {
                return report_unregistered_service_failure(
                    &data_dir,
                    ServiceRunFailure::new(
                        ServiceStartupPhase::RegisterControlHandler,
                        service_error("failed to register service control handler", error),
                    ),
                );
            }
        };
    match run_registered_service(&status_handle, stop_receiver, data_dir.clone()) {
        Ok(()) => Ok(()),
        Err(failure) => report_registered_service_failure(&status_handle, &data_dir, failure),
    }
}

fn run_registered_service(
    status_handle: &ServiceStatusHandle,
    stop_receiver: mpsc::Receiver<()>,
    data_dir: PathBuf,
) -> std::result::Result<(), ServiceRunFailure> {
    set_status(
        status_handle,
        ServiceState::StartPending,
        ServiceControlAccept::empty(),
        1,
        Duration::from_secs(30),
        ServiceExitCode::Win32(0),
    )
    .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::ReportStartPending, error))?;
    let status_handle = *status_handle;
    let worker = spawn_server_worker("ordadb-service-runtime", move || {
        run_service_worker(status_handle, stop_receiver, data_dir)
    })
    .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::CreateWorker, error))?;
    join_server_worker(worker, "Windows service worker")
        .map_err(|error| ServiceRunFailure::new(ServiceStartupPhase::JoinWorker, error))?
}

fn disable_service_recovery(service: &windows_service::service::Service) -> Result<()> {
    service
        .update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::ZERO),
            reboot_msg: Some(OsString::new()),
            command: Some(OsString::new()),
            actions: Some(Vec::new()),
        })
        .map_err(|error| service_error("failed to disable service failure actions", error))?;
    service
        .set_failure_actions_on_non_crash_failures(false)
        .map_err(|error| {
            service_error(
                "failed to disable service actions for non-crash failures",
                error,
            )
        })
}

pub fn prepare_installer_service(
    transaction_path: impl AsRef<Path>,
    executable_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
) -> Result<InstallerServiceTransactionStatus> {
    let transaction_path = transaction_path.as_ref();
    let executable_path = executable_path.as_ref();
    let data_dir = data_dir.as_ref();
    validate_installer_paths(transaction_path, executable_path, data_dir)?;
    let manager = installer_service_manager()?;
    let mut transaction = if transaction_path.is_file() {
        let transaction = read_installer_transaction(transaction_path)?;
        match transaction.state {
            InstallerServiceTransactionState::Prepared
                if transaction.prepared_executable == os_units(executable_path.as_os_str())
                    && transaction.prepared_data_dir == os_units(data_dir.as_os_str()) =>
            {
                return installer_transaction_status(&manager, &transaction);
            }
            InstallerServiceTransactionState::Captured
                if transaction.prepared_executable == os_units(executable_path.as_os_str())
                    && transaction.prepared_data_dir == os_units(data_dir.as_os_str()) =>
            {
                transaction
            }
            InstallerServiceTransactionState::Captured => {
                return Err(DbError::new(
                    "22023",
                    "captured installer service transaction targets different paths",
                ));
            }
            InstallerServiceTransactionState::Committed => {
                return Err(DbError::new(
                    "25000",
                    "installer service transaction is already committed",
                ));
            }
            InstallerServiceTransactionState::RolledBack => {
                return Err(DbError::new(
                    "25000",
                    "installer service transaction is already rolled back",
                ));
            }
            InstallerServiceTransactionState::Prepared => {
                return Err(DbError::new(
                    "22023",
                    "installer service transaction targets different paths",
                ));
            }
        }
    } else {
        let service = open_optional_service(&manager, ServiceAccess::ALL_ACCESS)?;
        if let Some(service) = service.as_ref() {
            let status = service.query_status().map_err(|error| {
                service_error("failed to query existing OrdaDB Windows service", error)
            })?;
            if status.current_state != ServiceState::Stopped {
                return Err(DbError::new(
                    "55006",
                    "existing OrdaDB Windows service must be stopped before prepare",
                ));
            }
        }
        let transaction = InstallerServiceTransactionV1 {
            schema_version: INSTALLER_TRANSACTION_SCHEMA_VERSION,
            state: InstallerServiceTransactionState::Captured,
            originally_existed: service.is_some(),
            original: service
                .as_ref()
                .map(capture_service_configuration)
                .transpose()?,
            prepared_executable: os_units(executable_path.as_os_str()),
            prepared_data_dir: os_units(data_dir.as_os_str()),
        };
        write_installer_transaction(transaction_path, &transaction)?;
        transaction
    };

    let desired = desired_service_info(executable_path, data_dir);
    let service = match manager.open_service(SERVICE_NAME, ServiceAccess::ALL_ACCESS) {
        Ok(service) => {
            service.change_config(&desired).map_err(|error| {
                service_error("failed to prepare updated OrdaDB Windows service", error)
            })?;
            service
        }
        Err(error) if is_winapi_error(&error, ERROR_SERVICE_DOES_NOT_EXIST) => manager
            .create_service(&desired, ServiceAccess::ALL_ACCESS)
            .map_err(|error| {
                service_error("failed to prepare new OrdaDB Windows service", error)
            })?,
        Err(error) => {
            return Err(service_error(
                "failed to open OrdaDB Windows service for prepare",
                error,
            ));
        }
    };
    service
        .set_description(SERVICE_DESCRIPTION)
        .map_err(|error| service_error("failed to set OrdaDB service description", error))?;
    service
        .set_delayed_auto_start(true)
        .map_err(|error| service_error("failed to set delayed automatic start", error))?;
    disable_service_recovery(&service)?;
    transaction.state = InstallerServiceTransactionState::Prepared;
    write_installer_transaction(transaction_path, &transaction)?;
    installer_transaction_status(&manager, &transaction)
}

pub fn commit_installer_service(
    transaction_path: impl AsRef<Path>,
) -> Result<InstallerServiceTransactionStatus> {
    let transaction_path = transaction_path.as_ref();
    let mut transaction = read_installer_transaction(transaction_path)?;
    if transaction.state == InstallerServiceTransactionState::Committed {
        return installer_transaction_status(&installer_service_manager()?, &transaction);
    }
    let manager = installer_service_manager()?;
    let service = manager
        .open_service(
            SERVICE_NAME,
            ServiceAccess::QUERY_STATUS | ServiceAccess::CHANGE_CONFIG,
        )
        .map_err(|error| service_error("failed to open prepared OrdaDB service", error))?;
    let status = service
        .query_status()
        .map_err(|error| service_error("failed to query prepared OrdaDB service", error))?;
    validate_installer_commit(transaction.state, status.current_state)?;
    configure_service(&service)?;
    transaction.state = InstallerServiceTransactionState::Committed;
    write_installer_transaction(transaction_path, &transaction)?;
    installer_transaction_status(&manager, &transaction)
}

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
