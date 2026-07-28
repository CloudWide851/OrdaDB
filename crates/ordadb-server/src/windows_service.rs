use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

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

use crate::{ServerConfig, default_data_dir, start_server};

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
                wait_for_state(&service, ServiceState::Running)?;
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
    wait_for_state(service, ServiceState::Stopped).map(|_| ())
}

fn wait_for_state(
    service: &windows_service::service::Service,
    target: ServiceState,
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
        });
    };
    let status = service
        .query_status()
        .map_err(|error| service_error("failed to query OrdaDB Windows service", error))?;
    Ok(WindowsServiceStatus {
        installed: true,
        state: service_state_name(status.current_state).into(),
        process_id: status.process_id,
        account: SERVICE_ACCOUNT,
        start_mode: SERVICE_START_MODE,
        failure_actions: SERVICE_FAILURE_ACTIONS,
        executable_path: executable_path.to_path_buf(),
        data_dir: data_dir.to_path_buf(),
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
        let _ = error;
    }
}

fn run_service(arguments: Vec<OsString>) -> Result<()> {
    let (stop_sender, stop_receiver) = mpsc::channel::<()>();
    let status_handle =
        service_control_handler::register(SERVICE_NAME, move |control| match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                let _ = stop_sender.send(());
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        })
        .map_err(|error| service_error("failed to register service control handler", error))?;
    set_status(
        &status_handle,
        ServiceState::StartPending,
        ServiceControlAccept::empty(),
        1,
        Duration::from_secs(30),
    )?;

    let data_dir = configured_process_data_dir()
        .or_else(|| {
            arguments
                .get(1)
                .filter(|value| !value.is_empty())
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(default_data_dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            DbError::new("XX000", "failed to create service runtime").with_detail(error.to_string())
        })?;
    runtime.block_on(async move {
        let server = start_server(ServerConfig::new(data_dir)).await?;
        set_status(
            &status_handle,
            ServiceState::Running,
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            0,
            Duration::ZERO,
        )?;
        tokio::task::spawn_blocking(move || stop_receiver.recv())
            .await
            .map_err(|error| {
                DbError::new("XX000", "service stop task failed").with_detail(error.to_string())
            })?
            .map_err(|error| {
                DbError::new("XX000", "service stop channel closed").with_detail(error.to_string())
            })?;
        set_status(
            &status_handle,
            ServiceState::StopPending,
            ServiceControlAccept::empty(),
            1,
            Duration::from_secs(30),
        )?;
        server.shutdown().await?;
        set_status(
            &status_handle,
            ServiceState::Stopped,
            ServiceControlAccept::empty(),
            0,
            Duration::ZERO,
        )
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
) -> Result<()> {
    handle
        .set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state,
            controls_accepted,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint,
            wait_hint,
            process_id: None,
        })
        .map_err(|error| service_error("failed to update Windows service status", error))
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
}
