use serde::Serialize;

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

pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![get_app_status])
        .run(tauri::generate_context!())
        .expect("failed to run OrdaDB desktop application");
}

#[cfg(test)]
mod tests {
    use super::{AppStatus, build_app_status};

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
}
