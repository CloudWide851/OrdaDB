use std::collections::BTreeMap;

use ordadb_engine::CatalogSettingMetadata;
use ordadb_sql::{ParsedExprKind, ParsedStatement, parse};
use ordadb_types::{DbError, Result, Value};

const MAX_STARTUP_PARAMETER_BYTES: usize = 1_024;
const MAX_APPLICATION_NAME_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgSessionSettings {
    server_version: String,
    application_name: String,
    client_encoding: String,
    date_style: String,
    time_zone: String,
    integer_datetimes: String,
    standard_conforming_strings: String,
    default_transaction_isolation: String,
    transaction_isolation: String,
    default_transaction_read_only: String,
    session_authorization: String,
    extra_float_digits: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PgSettingStatement {
    Show {
        name: String,
    },
    Set {
        name: String,
        value: String,
    },
    SetConfig {
        name: String,
        value: String,
        is_local: bool,
        result_name: String,
    },
    Reset {
        name: String,
    },
    ResetAll,
}

impl PgSessionSettings {
    pub(crate) fn from_startup(
        server_version: String,
        user: &str,
        parameters: &BTreeMap<String, String>,
    ) -> Result<Self> {
        validate_startup_parameters(parameters)?;
        let mut settings = Self {
            server_version,
            application_name: String::new(),
            client_encoding: "UTF8".to_owned(),
            date_style: "ISO, YMD".to_owned(),
            time_zone: "UTC".to_owned(),
            integer_datetimes: "on".to_owned(),
            standard_conforming_strings: "on".to_owned(),
            default_transaction_isolation: "read committed".to_owned(),
            transaction_isolation: "read committed".to_owned(),
            default_transaction_read_only: "off".to_owned(),
            session_authorization: user.to_owned(),
            extra_float_digits: "1".to_owned(),
        };
        if let Some(application_name) = parameters.get("application_name") {
            settings.set("application_name", application_name)?;
        }
        if let Some(client_encoding) = parameters.get("client_encoding") {
            settings.set("client_encoding", client_encoding)?;
        }
        if let Some(options) = parameters.get("options") {
            settings.apply_startup_options(options)?;
        }
        Ok(settings)
    }

    pub(crate) fn parameter_statuses(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("server_version", &self.server_version),
            ("server_encoding", "UTF8"),
            ("client_encoding", &self.client_encoding),
            ("DateStyle", &self.date_style),
            ("TimeZone", &self.time_zone),
            ("integer_datetimes", &self.integer_datetimes),
            (
                "standard_conforming_strings",
                &self.standard_conforming_strings,
            ),
            (
                "default_transaction_isolation",
                &self.default_transaction_isolation,
            ),
            (
                "default_transaction_read_only",
                &self.default_transaction_read_only,
            ),
            ("session_authorization", &self.session_authorization),
            ("application_name", &self.application_name),
        ]
    }

    pub(crate) fn system_catalog_metadata(&self) -> Vec<CatalogSettingMetadata> {
        SYSTEM_CATALOG_SETTINGS
            .iter()
            .map(|descriptor| {
                let setting = self
                    .get(descriptor.name)
                    .unwrap_or(descriptor.boot_value)
                    .to_owned();
                let source = if setting == descriptor.reset_value {
                    "default"
                } else if descriptor.name == "application_name" {
                    "client"
                } else {
                    "session"
                };
                CatalogSettingMetadata {
                    name: descriptor.name.to_owned(),
                    setting,
                    unit: descriptor.unit.map(str::to_owned),
                    category: descriptor.category.to_owned(),
                    short_description: descriptor.short_description.to_owned(),
                    context: descriptor.context.to_owned(),
                    value_type: descriptor.value_type.to_owned(),
                    source: source.to_owned(),
                    minimum: descriptor.minimum.map(str::to_owned),
                    maximum: descriptor.maximum.map(str::to_owned),
                    enum_values: descriptor.enum_values.map(str::to_owned),
                    boot_value: descriptor.boot_value.to_owned(),
                    reset_value: descriptor.reset_value.to_owned(),
                }
            })
            .collect()
    }

    pub(crate) fn runtime_values(&self) -> BTreeMap<String, String> {
        SYSTEM_CATALOG_SETTINGS
            .iter()
            .filter_map(|descriptor| {
                self.get(descriptor.name)
                    .map(|setting| (normalize_name(descriptor.name), setting.to_owned()))
            })
            .collect()
    }

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        match normalize_name(name).as_str() {
            "server_version" => Some(&self.server_version),
            "server_encoding" => Some("UTF8"),
            "client_encoding" => Some(&self.client_encoding),
            "datestyle" => Some(&self.date_style),
            "timezone" => Some(&self.time_zone),
            "integer_datetimes" => Some(&self.integer_datetimes),
            "standard_conforming_strings" => Some(&self.standard_conforming_strings),
            "default_transaction_isolation" => Some(&self.default_transaction_isolation),
            "transaction_isolation" => Some(&self.transaction_isolation),
            "default_transaction_read_only" => Some(&self.default_transaction_read_only),
            "session_authorization" => Some(&self.session_authorization),
            "application_name" => Some(&self.application_name),
            "extra_float_digits" => Some(&self.extra_float_digits),
            _ => None,
        }
    }

    pub(crate) fn set(&mut self, name: &str, value: &str) -> Result<()> {
        validate_value(name, value)?;
        match normalize_name(name).as_str() {
            "application_name" => {
                if value.len() > MAX_APPLICATION_NAME_BYTES {
                    return Err(DbError::new(
                        "22023",
                        "application_name exceeds the 64-byte session limit",
                    ));
                }
                self.application_name = value.to_owned();
            }
            "client_encoding" => {
                if !matches!(normalize_encoding(value).as_str(), "utf8" | "unicode") {
                    return Err(DbError::new(
                        "22023",
                        format!("unsupported client encoding {value}"),
                    )
                    .with_hint("OrdaDB PostgreSQL sessions require UTF8 client encoding"));
                }
                self.client_encoding = "UTF8".to_owned();
            }
            "datestyle" => {
                if !value.eq_ignore_ascii_case("ISO, YMD") {
                    return Err(unsupported_setting(name, value));
                }
                self.date_style = "ISO, YMD".to_owned();
            }
            "timezone" => {
                if !matches!(value.to_ascii_uppercase().as_str(), "UTC" | "GMT") {
                    return Err(unsupported_setting(name, value));
                }
                self.time_zone = "UTC".to_owned();
            }
            "default_transaction_isolation" => {
                self.default_transaction_isolation = isolation_value(value)?;
            }
            "transaction_isolation" => {
                self.transaction_isolation = isolation_value(value)?;
            }
            "default_transaction_read_only" => {
                self.default_transaction_read_only = boolean_setting(value)?;
            }
            "standard_conforming_strings" => {
                if boolean_setting(value)? != "on" {
                    return Err(unsupported_setting(name, value));
                }
                self.standard_conforming_strings = "on".to_owned();
            }
            "extra_float_digits" => {
                let parsed = value
                    .parse::<i16>()
                    .map_err(|_| DbError::new("22023", "extra_float_digits must be an integer"))?;
                if !(-15..=3).contains(&parsed) {
                    return Err(DbError::new(
                        "22023",
                        "extra_float_digits must be between -15 and 3",
                    ));
                }
                self.extra_float_digits = parsed.to_string();
            }
            "server_version"
            | "server_encoding"
            | "integer_datetimes"
            | "session_authorization" => {
                return Err(DbError::new(
                    "55P02",
                    format!("parameter {name} cannot be changed"),
                ));
            }
            _ => {
                return Err(DbError::new(
                    "42704",
                    format!("unrecognized configuration parameter {name}"),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn reset(&mut self, name: &str) -> Result<()> {
        match normalize_name(name).as_str() {
            "application_name" => self.application_name.clear(),
            "client_encoding" => self.client_encoding = "UTF8".to_owned(),
            "datestyle" => self.date_style = "ISO, YMD".to_owned(),
            "timezone" => self.time_zone = "UTC".to_owned(),
            "default_transaction_isolation" => {
                self.default_transaction_isolation = "read committed".to_owned();
            }
            "transaction_isolation" => {
                self.transaction_isolation = self.default_transaction_isolation.clone();
            }
            "default_transaction_read_only" => {
                self.default_transaction_read_only = "off".to_owned();
            }
            "standard_conforming_strings" => {
                self.standard_conforming_strings = "on".to_owned();
            }
            "extra_float_digits" => self.extra_float_digits = "1".to_owned(),
            "server_version"
            | "server_encoding"
            | "integer_datetimes"
            | "session_authorization" => {
                return Err(DbError::new(
                    "55P02",
                    format!("parameter {name} cannot be changed"),
                ));
            }
            _ => {
                return Err(DbError::new(
                    "42704",
                    format!("unrecognized configuration parameter {name}"),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn reset_all(&mut self) {
        self.application_name.clear();
        self.client_encoding = "UTF8".to_owned();
        self.date_style = "ISO, YMD".to_owned();
        self.time_zone = "UTC".to_owned();
        self.standard_conforming_strings = "on".to_owned();
        self.default_transaction_isolation = "read committed".to_owned();
        self.transaction_isolation = "read committed".to_owned();
        self.default_transaction_read_only = "off".to_owned();
        self.extra_float_digits = "1".to_owned();
    }

    fn apply_startup_options(&mut self, options: &str) -> Result<()> {
        let tokens = options.split_ascii_whitespace().collect::<Vec<_>>();
        let mut index = 0;
        while index < tokens.len() {
            let token = tokens[index];
            let assignment = if token == "-c" || token == "--set" {
                index = index.saturating_add(1);
                tokens.get(index).copied().ok_or_else(|| {
                    DbError::new("22023", "startup options end after a setting flag")
                })?
            } else if let Some(assignment) = token.strip_prefix("-c") {
                assignment
            } else {
                return Err(
                    DbError::new("22023", format!("unsupported startup option {token}"))
                        .with_hint("use -c name=value for supported PostgreSQL session settings"),
                );
            };
            let (name, value) = assignment.split_once('=').ok_or_else(|| {
                DbError::new("22023", "startup setting must use name=value syntax")
            })?;
            self.set(name, value)?;
            index = index.saturating_add(1);
        }
        Ok(())
    }
}

struct SystemCatalogSettingDescriptor {
    name: &'static str,
    unit: Option<&'static str>,
    category: &'static str,
    short_description: &'static str,
    context: &'static str,
    value_type: &'static str,
    minimum: Option<&'static str>,
    maximum: Option<&'static str>,
    enum_values: Option<&'static str>,
    boot_value: &'static str,
    reset_value: &'static str,
}

const SYSTEM_CATALOG_SETTINGS: &[SystemCatalogSettingDescriptor] = &[
    system_catalog_setting(
        "server_version",
        "Preset Options",
        "Shows the PostgreSQL compatibility version.",
        "internal",
        "string",
        "18.0",
        "18.0",
    ),
    system_catalog_setting(
        "server_encoding",
        "Client Connection Defaults / Locale and Formatting",
        "Shows the server character set encoding.",
        "internal",
        "string",
        "UTF8",
        "UTF8",
    ),
    system_catalog_setting(
        "client_encoding",
        "Client Connection Defaults / Locale and Formatting",
        "Sets the client character set encoding.",
        "user",
        "string",
        "UTF8",
        "UTF8",
    ),
    system_catalog_setting(
        "DateStyle",
        "Client Connection Defaults / Locale and Formatting",
        "Sets the display format for date and time values.",
        "user",
        "string",
        "ISO, YMD",
        "ISO, YMD",
    ),
    system_catalog_setting(
        "TimeZone",
        "Client Connection Defaults / Locale and Formatting",
        "Sets the time zone for displaying and interpreting time stamps.",
        "user",
        "string",
        "UTC",
        "UTC",
    ),
    system_catalog_setting(
        "integer_datetimes",
        "Preset Options",
        "Reports whether datetimes are stored as 64-bit integers.",
        "internal",
        "bool",
        "on",
        "on",
    ),
    system_catalog_setting(
        "standard_conforming_strings",
        "Version and Platform Compatibility / Previous PostgreSQL Versions",
        "Causes ordinary strings to treat backslashes literally.",
        "user",
        "bool",
        "on",
        "on",
    ),
    system_catalog_enum_setting(
        "default_transaction_isolation",
        "Client Connection Defaults / Statement Behavior",
        "Sets the transaction isolation level of each new transaction.",
        "user",
        "read committed",
    ),
    system_catalog_enum_setting(
        "transaction_isolation",
        "Client Connection Defaults / Statement Behavior",
        "Shows the current transaction isolation level.",
        "user",
        "read committed",
    ),
    system_catalog_setting(
        "default_transaction_read_only",
        "Client Connection Defaults / Statement Behavior",
        "Sets the default read-only status of new transactions.",
        "user",
        "bool",
        "off",
        "off",
    ),
    system_catalog_setting(
        "session_authorization",
        "Client Connection Defaults / Statement Behavior",
        "Sets the session user identifier.",
        "backend",
        "string",
        "ordadb",
        "ordadb",
    ),
    system_catalog_setting(
        "application_name",
        "Reporting and Logging / What to Log",
        "Sets the application name reported in statistics and logs.",
        "user",
        "string",
        "",
        "",
    ),
    SystemCatalogSettingDescriptor {
        name: "extra_float_digits",
        unit: None,
        category: "Client Connection Defaults / Locale and Formatting",
        short_description: "Sets the number of digits displayed for floating-point values.",
        context: "user",
        value_type: "integer",
        minimum: Some("-15"),
        maximum: Some("3"),
        enum_values: None,
        boot_value: "1",
        reset_value: "1",
    },
];

const fn system_catalog_setting(
    name: &'static str,
    category: &'static str,
    short_description: &'static str,
    context: &'static str,
    value_type: &'static str,
    boot_value: &'static str,
    reset_value: &'static str,
) -> SystemCatalogSettingDescriptor {
    SystemCatalogSettingDescriptor {
        name,
        unit: None,
        category,
        short_description,
        context,
        value_type,
        minimum: None,
        maximum: None,
        enum_values: None,
        boot_value,
        reset_value,
    }
}

const fn system_catalog_enum_setting(
    name: &'static str,
    category: &'static str,
    short_description: &'static str,
    context: &'static str,
    reset_value: &'static str,
) -> SystemCatalogSettingDescriptor {
    SystemCatalogSettingDescriptor {
        name,
        unit: None,
        category,
        short_description,
        context,
        value_type: "enum",
        minimum: None,
        maximum: None,
        enum_values: Some("{\"read committed\",\"repeatable read\",serializable}"),
        boot_value: "read committed",
        reset_value,
    }
}

pub(crate) fn parse_setting_statement(sql: &str) -> Result<Option<PgSettingStatement>> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if let Some(statement) = parse_set_config_statement(sql)? {
        return Ok(Some(statement));
    }
    let upper = sql.to_ascii_uppercase();
    if let Some(name) = upper.strip_prefix("SHOW ") {
        let name = match name.trim() {
            "TIME ZONE" => "timezone",
            value => value,
        };
        return Ok(Some(PgSettingStatement::Show {
            name: name.to_ascii_lowercase(),
        }));
    }
    if upper == "RESET ALL" {
        return Ok(Some(PgSettingStatement::ResetAll));
    }
    if let Some(name) = upper.strip_prefix("RESET ") {
        return Ok(Some(PgSettingStatement::Reset {
            name: name.trim().to_ascii_lowercase(),
        }));
    }
    let Some(mut assignment) = strip_prefix_ignore_ascii_case(sql, "SET ") else {
        return Ok(None);
    };
    if assignment.to_ascii_uppercase().starts_with("TRANSACTION ")
        || assignment
            .to_ascii_uppercase()
            .starts_with("SESSION CHARACTERISTICS ")
    {
        return Ok(None);
    }
    if let Some(value) = strip_prefix_ignore_ascii_case(assignment, "SESSION ") {
        assignment = value;
    } else if let Some(value) = strip_prefix_ignore_ascii_case(assignment, "LOCAL ") {
        assignment = value;
    }
    let (name, value) = split_assignment(assignment)?;
    Ok(Some(PgSettingStatement::Set {
        name: name.trim().to_ascii_lowercase(),
        value: unquote_setting_value(value.trim())?,
    }))
}

fn parse_set_config_statement(sql: &str) -> Result<Option<PgSettingStatement>> {
    if strip_prefix_ignore_ascii_case(sql, "SELECT ").is_none() {
        return Ok(None);
    }
    let statement = parse(sql)?;
    let ParsedStatement::RoutineSelect {
        name,
        arguments,
        alias,
    } = statement
    else {
        return Ok(None);
    };
    let parts = name
        .parts
        .iter()
        .map(|part| part.name.as_str())
        .collect::<Vec<_>>();
    let is_set_config = matches!(parts.as_slice(), [name] if name.eq_ignore_ascii_case("set_config"))
        || matches!(parts.as_slice(), [schema, name]
            if schema.eq_ignore_ascii_case("pg_catalog")
                && name.eq_ignore_ascii_case("set_config"));
    if !is_set_config {
        return Ok(None);
    }
    let [name, value, is_local] = arguments.as_slice() else {
        return Err(DbError::new(
            "42883",
            "set_config requires text, text, and boolean arguments",
        ));
    };
    let ParsedExprKind::Literal(Value::Text(name)) = &name.kind else {
        return Err(DbError::new(
            "42883",
            "set_config setting name must be a text literal",
        ));
    };
    let ParsedExprKind::Literal(Value::Text(value)) = &value.kind else {
        return Err(DbError::new(
            "42883",
            "set_config setting value must be a text literal",
        ));
    };
    let ParsedExprKind::Literal(Value::Boolean(is_local)) = &is_local.kind else {
        return Err(DbError::new(
            "42883",
            "set_config is_local must be a boolean literal",
        ));
    };
    Ok(Some(PgSettingStatement::SetConfig {
        name: normalize_name(name.as_str()),
        value: value.clone(),
        is_local: *is_local,
        result_name: alias.map_or_else(
            || "set_config".to_owned(),
            |alias| alias.name.as_str().to_owned(),
        ),
    }))
}

fn strip_prefix_ignore_ascii_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

fn split_assignment(value: &str) -> Result<(&str, &str)> {
    if let Some((name, value)) = value.split_once('=') {
        return Ok((name, value));
    }
    let upper = value.to_ascii_uppercase();
    let position = upper.find(" TO ").ok_or_else(|| {
        DbError::new(
            "42601",
            "SET requires a configuration name followed by TO or =",
        )
    })?;
    Ok((&value[..position], &value[position + 4..]))
}

fn unquote_setting_value(value: &str) -> Result<String> {
    if value.is_empty() {
        return Err(DbError::new("42601", "SET requires a value"));
    }
    let bytes = value.as_bytes();
    if matches!(bytes.first(), Some(b'\'') | Some(b'\"')) {
        if bytes.last() != bytes.first() || bytes.len() < 2 {
            return Err(DbError::new(
                "42601",
                "SET value contains an unterminated quoted string",
            ));
        }
        return Ok(value[1..value.len() - 1].to_owned());
    }
    Ok(value.to_owned())
}

fn validate_startup_parameters(parameters: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in parameters {
        validate_value(name, value)?;
        if name.len() > MAX_STARTUP_PARAMETER_BYTES {
            return Err(DbError::new(
                "22023",
                "startup parameter name exceeds the 1024-byte limit",
            ));
        }
        match name.as_str() {
            "user" | "database" | "application_name" | "client_encoding" | "options" => {}
            "replication" if matches!(value.as_str(), "false" | "0" | "off") => {}
            "replication" => {
                return Err(DbError::new(
                    "0A000",
                    "PostgreSQL replication connections are not supported",
                ));
            }
            _ => {
                return Err(DbError::new(
                    "42704",
                    format!("unsupported startup parameter {name}"),
                ));
            }
        }
    }
    Ok(())
}

fn validate_value(name: &str, value: &str) -> Result<()> {
    if name.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
        return Err(DbError::new(
            "22023",
            "session parameters cannot contain NUL bytes",
        ));
    }
    if value.len() > MAX_STARTUP_PARAMETER_BYTES {
        return Err(DbError::new(
            "22023",
            format!("session parameter {name} exceeds the 1024-byte value limit"),
        ));
    }
    Ok(())
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn normalize_encoding(value: &str) -> String {
    value.trim().replace('-', "").to_ascii_lowercase()
}

fn isolation_value(value: &str) -> Result<String> {
    let normalized = value.trim().replace('-', " ").to_ascii_lowercase();
    match normalized.as_str() {
        "read committed" | "repeatable read" | "serializable" => Ok(normalized),
        _ => Err(DbError::new(
            "22023",
            format!("unsupported transaction isolation level {value}"),
        )),
    }
}

fn boolean_setting(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok("on".to_owned()),
        "off" | "false" | "no" | "0" => Ok("off".to_owned()),
        _ => Err(DbError::new(
            "22023",
            format!("invalid Boolean session value {value}"),
        )),
    }
}

fn unsupported_setting(name: &str, value: &str) -> DbError {
    DbError::new(
        "0A000",
        format!("session setting {name} does not support value {value}"),
    )
}

#[cfg(test)]
mod tests {
    use super::PgSessionSettings;
    use std::collections::BTreeMap;

    #[test]
    fn startup_options_are_validated_and_projected() {
        let parameters = BTreeMap::from([
            ("user".to_owned(), "dba".to_owned()),
            ("database".to_owned(), "ordadb".to_owned()),
            ("application_name".to_owned(), "pgjdbc".to_owned()),
            (
                "options".to_owned(),
                "-c extra_float_digits=3 -c timezone=UTC".to_owned(),
            ),
        ]);
        let settings =
            PgSessionSettings::from_startup("18.0 (OrdaDB test)".to_owned(), "dba", &parameters)
                .expect("settings");
        assert_eq!(settings.get("application_name"), Some("pgjdbc"));
        assert_eq!(settings.get("extra_float_digits"), Some("3"));
        assert_eq!(settings.get("TimeZone"), Some("UTC"));
        assert!(
            settings
                .parameter_statuses()
                .contains(&("server_version", "18.0 (OrdaDB test)"))
        );
    }

    #[test]
    fn startup_rejects_unsafe_or_unsupported_values() {
        let unsupported = BTreeMap::from([("replication".to_owned(), "database".to_owned())]);
        let error = PgSessionSettings::from_startup("18.0".to_owned(), "dba", &unsupported)
            .expect_err("replication rejected");
        assert_eq!(error.sql_state, "0A000");

        let encoding = BTreeMap::from([("client_encoding".to_owned(), "LATIN1".to_owned())]);
        let error = PgSessionSettings::from_startup("18.0".to_owned(), "dba", &encoding)
            .expect_err("encoding rejected");
        assert_eq!(error.sql_state, "22023");
    }

    #[test]
    fn settings_fail_closed_for_unknown_and_read_only_names() {
        let mut settings =
            PgSessionSettings::from_startup("18.0".to_owned(), "dba", &BTreeMap::new())
                .expect("settings");
        assert_eq!(
            settings
                .set("server_version", "19")
                .expect_err("read only")
                .sql_state,
            "55P02"
        );
        assert_eq!(
            settings
                .set("ordadb.secret", "value")
                .expect_err("unknown")
                .sql_state,
            "42704"
        );
    }

    #[test]
    fn setting_statements_parse_without_consuming_transaction_commands() {
        assert_eq!(
            super::parse_setting_statement("SHOW TIME ZONE").expect("show"),
            Some(super::PgSettingStatement::Show {
                name: "timezone".to_owned()
            })
        );
        assert_eq!(
            super::parse_setting_statement("SET application_name TO 'DataGrip'").expect("set"),
            Some(super::PgSettingStatement::Set {
                name: "application_name".to_owned(),
                value: "DataGrip".to_owned()
            })
        );
        assert!(
            super::parse_setting_statement("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .expect("transaction")
                .is_none()
        );
        assert_eq!(
            super::parse_setting_statement(
                "SELECT pg_catalog.set_config('application_name', 'DataGrip', false) AS applied"
            )
            .expect("set_config"),
            Some(super::PgSettingStatement::SetConfig {
                name: "application_name".to_owned(),
                value: "DataGrip".to_owned(),
                is_local: false,
                result_name: "applied".to_owned(),
            })
        );
    }

    #[test]
    fn reset_restores_safe_defaults() {
        let mut settings =
            PgSessionSettings::from_startup("18.0".to_owned(), "dba", &BTreeMap::new())
                .expect("settings");
        settings
            .set("application_name", "DataGrip")
            .expect("set application");
        settings.reset("application_name").expect("reset");
        assert_eq!(settings.get("application_name"), Some(""));
        settings
            .reset("server_version")
            .expect_err("read only reset");
    }
}
