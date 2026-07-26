use std::path::PathBuf;
use std::sync::Arc;

use ordadb_connectors::{
    OperationStarted, PluginCatalogItem, PluginCatalogSnapshot, PluginManager,
    PluginManagerOptions, RegistryStatus,
};
use ordadb_types::DbError;
use serde::Serialize;
use tauri::{Emitter, Manager, State};

const PLUGIN_PROGRESS_EVENT: &str = "plugin://progress";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppStatus {
    pub name: &'static str,
    pub version: &'static str,
    pub mode: &'static str,
    pub state: &'static str,
}

pub fn build_app_status() -> AppStatus {
    AppStatus {
        name: "OrdaDB Console",
        version: env!("CARGO_PKG_VERSION"),
        mode: "desktop",
        state: "ready",
    }
}

#[tauri::command]
fn get_app_status() -> AppStatus {
    build_app_status()
}

#[tauri::command]
fn plugin_registry_status(manager: State<'_, Arc<PluginManager>>) -> RegistryStatus {
    manager.registry_status()
}

#[tauri::command]
async fn plugin_catalog(
    manager: State<'_, Arc<PluginManager>>,
) -> Result<PluginCatalogSnapshot, DbError> {
    manager.catalog_snapshot().await
}

#[tauri::command(rename_all = "camelCase")]
async fn plugin_install(
    manager: State<'_, Arc<PluginManager>>,
    plugin_id: String,
) -> Result<OperationStarted, DbError> {
    manager.inner().install(&plugin_id).await
}

#[tauri::command(rename_all = "camelCase")]
fn plugin_cancel(
    manager: State<'_, Arc<PluginManager>>,
    operation_id: String,
) -> Result<(), DbError> {
    manager.cancel(&operation_id)
}

#[tauri::command(rename_all = "camelCase")]
async fn plugin_retry(
    manager: State<'_, Arc<PluginManager>>,
    plugin_id: String,
) -> Result<OperationStarted, DbError> {
    manager.inner().retry(&plugin_id).await
}

#[tauri::command(rename_all = "camelCase")]
async fn plugin_update(
    manager: State<'_, Arc<PluginManager>>,
    plugin_id: String,
) -> Result<OperationStarted, DbError> {
    manager.inner().update(&plugin_id).await
}

#[tauri::command(rename_all = "camelCase")]
fn plugin_rollback(
    manager: State<'_, Arc<PluginManager>>,
    plugin_id: String,
) -> Result<PluginCatalogItem, DbError> {
    manager.rollback(&plugin_id)
}

fn build_plugin_manager_options(plugin_root: PathBuf) -> PluginManagerOptions {
    let mut options = PluginManagerOptions::new(plugin_root);
    options.registry_url = option_env!("ORDADB_PLUGIN_REGISTRY_URL").map(str::to_owned);
    options.registry_public_key =
        option_env!("ORDADB_PLUGIN_REGISTRY_PUBLIC_KEY").map(str::to_owned);
    options
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let plugin_root = app
                .path()
                .app_local_data_dir()
                .map_err(|error| format!("failed to resolve OrdaDB local data path: {error}"))?
                .join("connectors");
            let manager = PluginManager::open_https(build_plugin_manager_options(plugin_root))
                .map_err(|error| format!("failed to initialize connector manager: {error}"))?;
            let mut progress = manager.subscribe_progress();
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                loop {
                    match progress.recv().await {
                        Ok(update) => {
                            let _ = handle.emit(PLUGIN_PROGRESS_EVENT, update);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            app.manage(manager);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_app_status,
            plugin_registry_status,
            plugin_catalog,
            plugin_install,
            plugin_cancel,
            plugin_retry,
            plugin_update,
            plugin_rollback
        ])
        .run(tauri::generate_context!())
        .expect("failed to run OrdaDB desktop application");
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{AppStatus, build_app_status, build_plugin_manager_options};

    #[test]
    fn builds_a_stable_desktop_status() {
        assert_eq!(
            build_app_status(),
            AppStatus {
                name: "OrdaDB Console",
                version: env!("CARGO_PKG_VERSION"),
                mode: "desktop",
                state: "ready",
            }
        );
    }

    #[test]
    fn plugin_registry_is_fail_closed_when_packaging_does_not_inject_trust() {
        let options = build_plugin_manager_options(PathBuf::from(r"C:\OrdaDB\connectors"));
        assert_eq!(
            options.registry_url.is_some(),
            option_env!("ORDADB_PLUGIN_REGISTRY_URL").is_some()
        );
        assert_eq!(
            options.registry_public_key.is_some(),
            option_env!("ORDADB_PLUGIN_REGISTRY_PUBLIC_KEY").is_some()
        );
        if option_env!("ORDADB_PLUGIN_REGISTRY_URL").is_none()
            || option_env!("ORDADB_PLUGIN_REGISTRY_PUBLIC_KEY").is_none()
        {
            assert!(options.registry_url.is_none() || options.registry_public_key.is_none());
        }
    }
}
