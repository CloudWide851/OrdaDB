use std::path::Path;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use ordadb_types::{DbError, Result};
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::{
    CONNECTOR_API_VERSION, ConnectorRequestV1, ConnectorResponseV1, CredentialPayload,
    PluginManager, io_error, network_error, read_connector_frame, validate_protocol_ready,
    write_connector_frame,
};

const PIPE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

pub struct ConnectorHost {
    child: Child,
    pipe: NamedPipeServer,
    plugin_id: String,
    plugin_version: String,
}

impl std::fmt::Debug for ConnectorHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorHost")
            .field("plugin_id", &self.plugin_id)
            .field("plugin_version", &self.plugin_version)
            .field("process_id", &self.child.id())
            .finish_non_exhaustive()
    }
}

impl ConnectorHost {
    pub async fn launch(manager: &Arc<PluginManager>, plugin_id: &str) -> Result<Self> {
        let installation = manager.active_installation(plugin_id)?;
        Self::launch_entry(
            &installation.entry,
            &installation.manifest.id,
            &installation.manifest.version,
        )
        .await
    }

    async fn launch_entry(entry: &Path, plugin_id: &str, plugin_version: &str) -> Result<Self> {
        let pipe_name = connector_pipe_name();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let mut pipe = options
            .create(&pipe_name)
            .map_err(|error| io_error("failed to create connector named pipe", error))?;
        ordadb_windows::restrict_named_pipe_acl(&pipe)?;

        let mut command = Command::new(entry);
        command
            .arg("--ordadb-pipe")
            .arg(&pipe_name)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_hidden_process(&mut command);
        let mut child = command
            .spawn()
            .map_err(|error| io_error("failed to start connector helper process", error))?;

        let connected = tokio::select! {
            connected = pipe.connect() => connected.map_err(|error| {
                io_error("connector named-pipe connection failed", error)
            }),
            status = child.wait() => {
                return Err(helper_exit_error(status));
            }
            () = tokio::time::sleep(PIPE_CONNECT_TIMEOUT) => {
                return Err(network_error(
                    "connector helper did not connect before the deadline",
                    "named-pipe connection timeout",
                ));
            }
        };
        if let Err(error) = connected {
            let _ = child.kill().await;
            return Err(error);
        }

        if let Err(error) = negotiate(&mut pipe, plugin_id, plugin_version).await {
            let _ = child.kill().await;
            return Err(error);
        }
        Ok(Self {
            child,
            pipe,
            plugin_id: plugin_id.into(),
            plugin_version: plugin_version.into(),
        })
    }

    pub async fn connect(
        &mut self,
        connection_id: impl Into<String>,
        endpoint: impl Into<String>,
        database: Option<String>,
        credential: CredentialPayload,
    ) -> Result<()> {
        let connection_id = connection_id.into();
        self.send(&ConnectorRequestV1::Connect {
            connection_id: connection_id.clone(),
            endpoint: endpoint.into(),
            database,
            credential,
        })
        .await?;
        match self.receive().await? {
            ConnectorResponseV1::Connected {
                connection_id: actual,
            } if actual == connection_id => Ok(()),
            ConnectorResponseV1::Error { error, .. } => Err(error),
            _ => Err(DbError::new(
                "08P01",
                "connector returned an unexpected connect response",
            )),
        }
    }

    pub async fn send(&mut self, request: &ConnectorRequestV1) -> Result<()> {
        if self
            .child
            .try_wait()
            .map_err(|error| io_error("failed to inspect connector helper process", error))?
            .is_some()
        {
            return Err(network_error(
                "connector helper process exited",
                "process is no longer running",
            ));
        }
        write_connector_frame(&mut self.pipe, request).await
    }

    pub async fn receive(&mut self) -> Result<ConnectorResponseV1> {
        tokio::select! {
            response = read_connector_frame(&mut self.pipe) => response,
            status = self.child.wait() => Err(helper_exit_error(status)),
        }
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.send(&ConnectorRequestV1::Shutdown).await;
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(io_error(
                "failed to wait for connector helper shutdown",
                error,
            )),
            Err(_) => {
                self.child
                    .kill()
                    .await
                    .map_err(|error| io_error("failed to stop connector helper process", error))?;
                Ok(())
            }
        }
    }
}

async fn negotiate(
    pipe: &mut NamedPipeServer,
    plugin_id: &str,
    plugin_version: &str,
) -> Result<()> {
    write_connector_frame(
        pipe,
        &ConnectorRequestV1::Hello {
            api_version: CONNECTOR_API_VERSION,
            plugin_id: plugin_id.into(),
            plugin_version: plugin_version.into(),
        },
    )
    .await?;
    let response = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        read_connector_frame::<_, ConnectorResponseV1>(pipe),
    )
    .await
    .map_err(|_| {
        network_error(
            "connector handshake timed out",
            "no protocol response before the deadline",
        )
    })??;
    match response {
        ConnectorResponseV1::Ready(ready) => {
            validate_protocol_ready(&ready, plugin_id, plugin_version)
        }
        ConnectorResponseV1::Error { error, .. } => Err(error),
        _ => Err(DbError::new(
            "08P01",
            "connector did not begin with a Ready response",
        )),
    }
}

fn connector_pipe_name() -> String {
    format!(r"\\.\pipe\ordadb-connector-{}", Uuid::new_v4())
}

fn configure_hidden_process(command: &mut Command) {
    use std::os::windows::process::CommandExt as _;
    use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

    command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
}

fn helper_exit_error(status: std::io::Result<ExitStatus>) -> DbError {
    match status {
        Ok(status) => network_error(
            "connector helper process exited before completing the protocol",
            format!("exit status {status}"),
        ),
        Err(error) => io_error("failed to wait for connector helper process", error),
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    use super::*;
    use crate::ProtocolReady;

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let mut server = options.create(&pipe_name).expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV1 =
                    read_connector_frame(&mut client).await.expect("hello");
                let ConnectorRequestV1::Hello {
                    api_version,
                    plugin_id,
                    plugin_version,
                } = hello
                else {
                    panic!("expected hello");
                };
                assert_eq!(api_version, CONNECTOR_API_VERSION);
                write_connector_frame(
                    &mut client,
                    &ConnectorResponseV1::Ready(ProtocolReady {
                        api_version,
                        plugin_id,
                        plugin_version,
                    }),
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        negotiate(&mut server, "ordadb-postgresql", "1.0.0")
            .await
            .expect("negotiate");
        client.await.expect("client task");
    }

    #[tokio::test]
    async fn helper_exit_is_reported_without_sending_credentials() {
        let command = std::env::var_os("ComSpec").expect("ComSpec");
        let error = ConnectorHost::launch_entry(Path::new(&command), "ordadb-test", "1.0.0")
            .await
            .expect_err("cmd does not speak connector protocol");
        assert!(matches!(error.sql_state.as_str(), "08006" | "58030"));
    }
}
