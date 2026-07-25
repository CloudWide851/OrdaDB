use std::ffi::OsString;
use std::sync::mpsc;
use std::time::Duration;

use windows_service::define_windows_service;
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{
    self, ServiceControlHandlerResult, ServiceStatusHandle,
};
use windows_service::service_dispatcher;

use ordadb_types::{DbError, Result};

use crate::{ServerConfig, default_data_dir, start_server};

pub const SERVICE_NAME: &str = "OrdaDB";
pub const SERVICE_DISPLAY_NAME: &str = "OrdaDB Database Service";
pub const SERVICE_ACCOUNT: &str = "NT AUTHORITY\\LocalService";
pub const SERVICE_START_MODE: &str = "auto-delayed";
pub const SERVICE_FAILURE_ACTIONS: &str = "restart/5000,restart/15000,restart/60000";

define_windows_service!(ffi_service_main, service_main);

pub fn dispatch_windows_service() -> Result<()> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .map_err(|error| service_error("failed to enter Windows service dispatcher", error))
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

    let data_dir = arguments
        .get(1)
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
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
