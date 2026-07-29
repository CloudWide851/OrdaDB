use std::fmt::{Debug, Formatter};

use ordadb_types::{DbError, QueryEvent, Result, Value};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zeroize::Zeroizing;

use crate::{MIN_CONNECTOR_API_VERSION, protocol_error};

pub const MAX_CONNECTOR_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct CredentialPayload {
    pub username: String,
    pub password: Zeroizing<String>,
}

impl CredentialPayload {
    #[must_use]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: Zeroizing::new(password.into()),
        }
    }
}

impl Debug for CredentialPayload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialPayload")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Serialize for CredentialPayload {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            username: &'a str,
            password: &'a str,
        }
        Wire {
            username: &self.username,
            password: self.password.as_str(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CredentialPayload {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            username: String,
            password: String,
        }
        let wire = Wire::deserialize(deserializer)?;
        Ok(Self::new(wire.username, wire.password))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolReady {
    pub api_version: u32,
    pub plugin_id: String,
    pub plugin_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogEntry {
    pub kind: String,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "camelCase")]
pub enum ConnectorRequestV1 {
    Hello {
        api_version: u32,
        plugin_id: String,
        plugin_version: String,
    },
    Connect {
        connection_id: String,
        endpoint: String,
        database: Option<String>,
        credential: CredentialPayload,
    },
    Catalog {
        request_id: String,
        connection_id: String,
    },
    Execute {
        request_id: String,
        connection_id: String,
        sql: String,
        params: Vec<Value>,
    },
    Cancel {
        request_id: String,
    },
    Begin {
        request_id: String,
        connection_id: String,
    },
    Commit {
        request_id: String,
        connection_id: String,
    },
    Rollback {
        request_id: String,
        connection_id: String,
    },
    Monitor {
        request_id: String,
        connection_id: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "camelCase")]
pub enum ConnectorResponseV1 {
    Ready(ProtocolReady),
    Connected {
        connection_id: String,
    },
    Catalog {
        request_id: String,
        entries: Vec<CatalogEntry>,
    },
    QueryEvent {
        request_id: String,
        event: QueryEvent,
    },
    Monitor {
        request_id: String,
        sessions: u32,
        active_queries: u32,
    },
    Error {
        request_id: Option<String>,
        error: DbError,
    },
    Shutdown,
}

pub fn validate_protocol_ready(
    ready: &ProtocolReady,
    plugin_id: &str,
    plugin_version: &str,
) -> Result<()> {
    if ready.api_version != MIN_CONNECTOR_API_VERSION {
        return Err(DbError::unsupported(format!(
            "connector protocol version {}",
            ready.api_version
        ))
        .with_hint("Install a connector built for this OrdaDB host."));
    }
    if ready.plugin_id != plugin_id || ready.plugin_version != plugin_version {
        return Err(protocol_error(
            "connector handshake identity does not match the installed manifest",
        ));
    }
    Ok(())
}

pub async fn read_connector_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let length = reader.read_u32_le().await.map_err(|error| {
        protocol_error(format!("failed to read connector frame length: {error}"))
    })?;
    let length =
        usize::try_from(length).map_err(|_| protocol_error("connector frame length overflowed"))?;
    if length == 0 || length > MAX_CONNECTOR_FRAME_BYTES {
        return Err(protocol_error(format!(
            "connector frame length must be between 1 and {MAX_CONNECTOR_FRAME_BYTES} bytes"
        )));
    }
    let mut bytes = vec![0_u8; length];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|error| protocol_error(format!("failed to read connector frame: {error}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| protocol_error(format!("connector frame is invalid JSON: {error}")))
}

pub async fn write_connector_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes =
        Zeroizing::new(serde_json::to_vec(value).map_err(|error| {
            protocol_error(format!("failed to encode connector frame: {error}"))
        })?);
    if bytes.is_empty() || bytes.len() > MAX_CONNECTOR_FRAME_BYTES {
        return Err(protocol_error(format!(
            "connector frame length must be between 1 and {MAX_CONNECTOR_FRAME_BYTES} bytes"
        )));
    }
    let length = u32::try_from(bytes.len())
        .map_err(|_| protocol_error("connector frame length overflowed"))?;
    writer.write_u32_le(length).await.map_err(|error| {
        protocol_error(format!("failed to write connector frame length: {error}"))
    })?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| protocol_error(format!("failed to write connector frame: {error}")))?;
    writer
        .flush()
        .await
        .map_err(|error| protocol_error(format!("failed to flush connector frame: {error}")))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, duplex};

    use super::*;

    #[test]
    fn credentials_redact_debug_but_round_trip_on_the_pipe() {
        let credential = CredentialPayload::new("dba", "never-log-this");
        let debug = format!("{credential:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("never-log-this"));

        let bytes = serde_json::to_vec(&credential).expect("serialize");
        let decoded: CredentialPayload = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded.username, "dba");
        assert_eq!(decoded.password.as_str(), "never-log-this");
    }

    #[tokio::test]
    async fn connector_frames_are_little_endian_bounded_and_typed() {
        let (mut writer, mut reader) = duplex(4096);
        let expected = ConnectorResponseV1::Ready(ProtocolReady {
            api_version: MIN_CONNECTOR_API_VERSION,
            plugin_id: "ordadb-postgresql".into(),
            plugin_version: "1.0.0".into(),
        });
        write_connector_frame(&mut writer, &expected)
            .await
            .expect("write frame");
        let decoded: ConnectorResponseV1 =
            read_connector_frame(&mut reader).await.expect("read frame");
        let ConnectorResponseV1::Ready(ready) = decoded else {
            panic!("expected ready");
        };
        validate_protocol_ready(&ready, "ordadb-postgresql", "1.0.0").expect("valid handshake");
    }

    #[tokio::test]
    async fn connector_frames_reject_zero_oversize_and_truncation() {
        let (mut writer, mut reader) = duplex(32);
        writer.write_u32_le(0).await.expect("zero length");
        let error = read_connector_frame::<_, ConnectorResponseV1>(&mut reader)
            .await
            .expect_err("zero frame");
        assert_eq!(error.sql_state, "08P01");

        let (mut writer, mut reader) = duplex(32);
        writer
            .write_u32_le(u32::try_from(MAX_CONNECTOR_FRAME_BYTES + 1).expect("frame length"))
            .await
            .expect("oversize length");
        let error = read_connector_frame::<_, ConnectorResponseV1>(&mut reader)
            .await
            .expect_err("oversize frame");
        assert_eq!(error.sql_state, "08P01");

        let (mut writer, mut reader) = duplex(32);
        writer.write_u32_le(8).await.expect("length");
        writer.write_all(b"{}").await.expect("partial body");
        drop(writer);
        let error = read_connector_frame::<_, ConnectorResponseV1>(&mut reader)
            .await
            .expect_err("truncated frame");
        assert_eq!(error.sql_state, "08P01");
    }

    #[test]
    fn handshake_rejects_protocol_and_identity_mismatches() {
        let error = validate_protocol_ready(
            &ProtocolReady {
                api_version: 2,
                plugin_id: "ordadb-postgresql".into(),
                plugin_version: "1.0.0".into(),
            },
            "ordadb-postgresql",
            "1.0.0",
        )
        .expect_err("protocol mismatch");
        assert_eq!(error.sql_state, "0A000");

        let error = validate_protocol_ready(
            &ProtocolReady {
                api_version: MIN_CONNECTOR_API_VERSION,
                plugin_id: "other".into(),
                plugin_version: "1.0.0".into(),
            },
            "ordadb-postgresql",
            "1.0.0",
        )
        .expect_err("identity mismatch");
        assert_eq!(error.sql_state, "08P01");
    }
}
