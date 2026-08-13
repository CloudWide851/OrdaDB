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
