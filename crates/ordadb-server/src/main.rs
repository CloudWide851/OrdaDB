use std::path::PathBuf;

use ordadb_server::{
    ServerConfig, ServiceCommand, TlsPaths, default_data_dir, dispatch_windows_service,
    manage_windows_service, start_server,
};
use ordadb_types::{DbError, Result};

fn main() {
    if let Err(error) = run() {
        eprintln!("{}: {}", error.sql_state, error.message);
        if let Some(detail) = error.detail {
            eprintln!("{detail}");
        }
        if let Some(hint) = error.hint {
            eprintln!("HINT: {hint}");
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|argument| argument == "service")
    {
        return run_service_command(&arguments[1..]);
    }
    if arguments.iter().any(|argument| argument == "--service") {
        return dispatch_windows_service();
    }
    let config = parse_config(&arguments)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            DbError::new("XX000", "failed to create server runtime").with_detail(error.to_string())
        })?;
    runtime.block_on(async move {
        let server = start_server(config).await?;
        println!(
            "{{\"state\":\"ready\",\"pgAddress\":\"{}\",\"adminAddress\":\"{}\",\"bootstrapPipe\":{}}}",
            server.pg_address,
            server.admin_address,
            server
                .bootstrap_pipe
                .as_ref()
                .map_or_else(|| "null".to_owned(), |pipe| format!("{pipe:?}"))
        );
        tokio::signal::ctrl_c().await.map_err(|error| {
            DbError::new("58030", "failed to wait for Ctrl+C").with_detail(error.to_string())
        })?;
        server.shutdown().await
    })
}

fn run_service_command(arguments: &[String]) -> Result<()> {
    let command = arguments
        .first()
        .ok_or_else(|| DbError::new("22023", "service command is required"))?
        .parse::<ServiceCommand>()?;
    let mut data_dir = default_data_dir();
    let mut executable_path = std::env::current_exe().map_err(|error| {
        DbError::new("58030", "failed to resolve the server executable")
            .with_detail(error.to_string())
    })?;
    let mut index = 1;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| DbError::new("22023", format!("{option} requires a value")))?;
        match option.as_str() {
            "--data-dir" => data_dir = PathBuf::from(value),
            "--executable" => executable_path = PathBuf::from(value),
            _ => {
                return Err(DbError::new(
                    "22023",
                    format!("unknown service option {option}"),
                ));
            }
        }
        index += 2;
    }
    let status = manage_windows_service(command, executable_path, data_dir)?;
    println!(
        "{}",
        serde_json::to_string(&status).map_err(|error| {
            DbError::new("XX000", "failed to encode Windows service status")
                .with_detail(error.to_string())
        })?
    );
    Ok(())
}

fn parse_config(arguments: &[String]) -> Result<ServerConfig> {
    let mut data_dir = default_data_dir();
    let mut pg_bind = None;
    let mut admin_bind = None;
    let mut certificate: Option<PathBuf> = None;
    let mut private_key: Option<PathBuf> = None;
    let mut index = 0;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| DbError::new("22023", format!("{option} requires a value")))?;
        match option.as_str() {
            "--data-dir" => data_dir = PathBuf::from(value),
            "--pg-bind" => {
                pg_bind = Some(value.parse().map_err(|_| {
                    DbError::new("22023", "--pg-bind must be an IP socket address")
                })?);
            }
            "--admin-bind" => {
                admin_bind = Some(value.parse().map_err(|_| {
                    DbError::new("22023", "--admin-bind must be an IP socket address")
                })?);
            }
            "--tls-cert" => certificate = Some(PathBuf::from(value)),
            "--tls-key" => private_key = Some(PathBuf::from(value)),
            unknown => {
                return Err(DbError::new(
                    "22023",
                    format!("unknown server option {unknown}"),
                ));
            }
        }
        index += 2;
    }
    let mut config = ServerConfig::new(data_dir);
    if let Some(pg_bind) = pg_bind {
        config.pg_bind = pg_bind;
    }
    if let Some(admin_bind) = admin_bind {
        config.admin_bind = admin_bind;
    }
    config.tls = match (certificate, private_key) {
        (Some(certificate), Some(private_key)) => Some(TlsPaths {
            certificate,
            private_key,
        }),
        (None, None) => None,
        _ => {
            return Err(DbError::new(
                "22023",
                "--tls-cert and --tls-key must be provided together",
            ));
        }
    };
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_line_tls_paths_must_be_paired() {
        let error = parse_config(&[
            "--data-dir".into(),
            r"C:\temp\ordadb".into(),
            "--tls-cert".into(),
            "server.pem".into(),
        ])
        .expect_err("pair");
        assert_eq!(error.sql_state, "22023");
    }

    #[test]
    fn service_command_requires_an_action() {
        assert_eq!(
            run_service_command(&[])
                .expect_err("missing action")
                .sql_state,
            "22023"
        );
    }
}
