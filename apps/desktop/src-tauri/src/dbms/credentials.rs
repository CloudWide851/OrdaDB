use ordadb_types::DbError;
use ordadb_windows::{PromptedCredential, prompt_for_credential};
use serde::{Deserialize, Serialize};

use super::{NATIVE_CONNECTOR_ID, invalid, task_error};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialSaved {
    pub(super) credential_id: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PromptCredentialRequest {
    pub(super) credential_id: String,
    pub(super) connector_id: String,
    pub(super) suggested_username: String,
}

impl std::fmt::Debug for PromptCredentialRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PromptCredentialRequest")
            .field("credential_id", &self.credential_id)
            .field("connector_id", &self.connector_id)
            .field("suggested_username", &"<redacted>")
            .finish()
    }
}

pub(super) async fn prompt_database_credential(
    connector_id: String,
    suggested_username: String,
    first_administrator: bool,
) -> Result<Option<PromptedCredential>, DbError> {
    let (caption, message) = if first_administrator {
        (
            "创建 OrdaDB 首位管理员".to_owned(),
            "请输入首位管理员用户名和密码。用户名和密码只在受保护的本机窗口中处理，并由当前 Windows 用户加密保存。"
                .to_owned(),
        )
    } else {
        let display_name = match connector_id.as_str() {
            NATIVE_CONNECTOR_ID => "OrdaDB",
            "postgresql" => "PostgreSQL",
            "mysql" => "MySQL",
            "sqlite" => "SQLite",
            "sql-server" => "SQL Server",
            "mongodb" => "MongoDB",
            "redis" => "Redis",
            "mariadb" => "MariaDB",
            "clickhouse" => "ClickHouse",
            "oracle" => "Oracle",
            _ => return Err(invalid("unknown connector ID")),
        };
        (
            format!("连接 {display_name}"),
            format!("请输入 {display_name} 用户名和密码。凭据不会进入 OrdaDB 网页界面或状态文件。"),
        )
    };
    let target = format!("OrdaDB Console/{connector_id}");
    tauri::async_runtime::spawn_blocking(move || {
        prompt_for_credential(&target, &suggested_username, &caption, &message)
    })
    .await
    .map_err(|error| task_error("Windows credential prompt task failed", error))?
}
