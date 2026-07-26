use ordadb_admin::{Action, AuthStore, DbObject, Principal};
use ordadb_types::{DbError, Result};
use sqlparser::ast::{
    Action as SqlAction, AlterRoleOperation, Expr, GrantObjects, GranteeName, ObjectName,
    ObjectType, Password, Privileges, RoleOption, Statement as SqlStatement, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use zeroize::Zeroizing;

pub(crate) enum SecurityStatement {
    CreateRole {
        name: String,
        if_not_exists: bool,
    },
    CreateUser {
        name: String,
        password: Zeroizing<Vec<u8>>,
        if_not_exists: bool,
    },
    AlterUserPassword {
        name: String,
        password: Zeroizing<Vec<u8>>,
    },
    SetUserEnabled {
        name: String,
        enabled: bool,
    },
    DropRole {
        name: String,
        if_exists: bool,
    },
    DropUser {
        name: String,
        if_exists: bool,
    },
    GrantRole {
        role: String,
        member: String,
    },
    RevokeRole {
        role: String,
        member: String,
    },
    GrantPrivilege {
        role: String,
        action: Action,
        object: DbObject,
    },
    RevokePrivilege {
        role: String,
        action: Action,
        object: DbObject,
    },
}

impl SecurityStatement {
    pub(crate) const fn tag(&self) -> &'static str {
        match self {
            Self::CreateRole { .. } => "CREATE ROLE",
            Self::CreateUser { .. } => "CREATE USER",
            Self::AlterUserPassword { .. } | Self::SetUserEnabled { .. } => "ALTER USER",
            Self::DropRole { .. } => "DROP ROLE",
            Self::DropUser { .. } => "DROP USER",
            Self::GrantRole { .. } | Self::GrantPrivilege { .. } => "GRANT",
            Self::RevokeRole { .. } | Self::RevokePrivilege { .. } => "REVOKE",
        }
    }
}

pub(crate) fn is_security_sql(sql: &str) -> bool {
    let normalized = sql.trim_start().to_ascii_uppercase();
    [
        "CREATE ROLE ",
        "CREATE USER ",
        "ALTER ROLE ",
        "ALTER USER ",
        "DROP ROLE ",
        "DROP USER ",
        "GRANT ",
        "REVOKE ",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
}

pub(crate) fn redacted_security_sql(sql: &str) -> String {
    if is_security_sql(sql) {
        "SECURITY DDL <redacted>".to_owned()
    } else {
        sql.to_owned()
    }
}

pub(crate) fn parse_security_statement(sql: &str) -> Result<Option<SecurityStatement>> {
    if !is_security_sql(sql) {
        return Ok(None);
    }
    if let Some(statement) = parse_role_membership(sql)? {
        return Ok(Some(statement));
    }

    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let uppercase = trimmed.to_ascii_uppercase();
    let (parser_sql, user_alias) = if uppercase.starts_with("CREATE USER ") {
        (
            format!("CREATE ROLE {}", &trimmed["CREATE USER ".len()..]),
            true,
        )
    } else if uppercase.starts_with("ALTER USER ") {
        (
            format!("ALTER ROLE {}", &trimmed["ALTER USER ".len()..]),
            true,
        )
    } else {
        (trimmed.to_owned(), false)
    };
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, &parser_sql)
        .map_err(|_| security_syntax_error())?;
    if statements.len() != 1 {
        return Err(security_syntax_error());
    }
    let statement = statements.pop().ok_or_else(security_syntax_error)?;
    convert_statement(statement, user_alias).map(Some)
}

pub(crate) fn execute_security_statement(
    auth: &AuthStore,
    principal: &mut Principal,
    statement: SecurityStatement,
) -> Result<&'static str> {
    let tag = statement.tag();
    match statement {
        SecurityStatement::CreateRole {
            name,
            if_not_exists,
        } => {
            auth.create_role(&name, if_not_exists)?;
        }
        SecurityStatement::CreateUser {
            name,
            password,
            if_not_exists,
        } => {
            auth.create_user(&name, &password, if_not_exists)?;
        }
        SecurityStatement::AlterUserPassword { name, password } => {
            auth.alter_user_password(&name, &password)?;
        }
        SecurityStatement::SetUserEnabled { name, enabled } => {
            auth.set_user_enabled(&name, enabled)?;
        }
        SecurityStatement::DropRole { name, if_exists } => {
            auth.drop_role(&name, if_exists)?;
        }
        SecurityStatement::DropUser { name, if_exists } => {
            auth.drop_user(&name, if_exists)?;
        }
        SecurityStatement::GrantRole { role, member } => {
            auth.grant_role(&role, &member)?;
        }
        SecurityStatement::RevokeRole { role, member } => {
            auth.revoke_role(&role, &member)?;
        }
        SecurityStatement::GrantPrivilege {
            role,
            action,
            object,
        } => {
            auth.grant_privilege(&role, action, object)?;
        }
        SecurityStatement::RevokePrivilege {
            role,
            action,
            object,
        } => {
            auth.revoke_privilege(&role, action, &object)?;
        }
    }
    if let Ok(updated) = auth.principal(&principal.user) {
        *principal = updated;
    } else {
        principal.roles.clear();
    }
    Ok(tag)
}

fn convert_statement(statement: SqlStatement, user_alias: bool) -> Result<SecurityStatement> {
    match statement {
        SqlStatement::CreateRole(role) => {
            if role.names.len() != 1
                || role.inherit.is_some()
                || role.bypassrls.is_some()
                || role.superuser.is_some()
                || role.create_db.is_some()
                || role.create_role.is_some()
                || role.replication.is_some()
                || role.connection_limit.is_some()
                || role.valid_until.is_some()
                || !role.in_role.is_empty()
                || !role.in_group.is_empty()
                || !role.role.is_empty()
                || !role.user.is_empty()
                || !role.admin.is_empty()
                || role.authorization_owner.is_some()
            {
                return unsupported("this CREATE ROLE/USER option is not supported");
            }
            let name = object_name(role.names.first().ok_or_else(security_syntax_error)?)?;
            let is_user = user_alias || role.login == Some(true);
            if is_user {
                let password = password_bytes(
                    role.password
                        .ok_or_else(|| DbError::new("42601", "CREATE USER requires PASSWORD"))?,
                )?;
                Ok(SecurityStatement::CreateUser {
                    name,
                    password,
                    if_not_exists: role.if_not_exists,
                })
            } else {
                if role.login.is_some() || role.password.is_some() {
                    return unsupported("CREATE ROLE supports only non-login roles");
                }
                Ok(SecurityStatement::CreateRole {
                    name,
                    if_not_exists: role.if_not_exists,
                })
            }
        }
        SqlStatement::AlterRole { name, operation } => {
            let name = identifier(&name.value)?;
            let AlterRoleOperation::WithOptions { options } = operation else {
                return unsupported("only ALTER USER password/login options are supported");
            };
            if !user_alias || options.len() != 1 {
                return unsupported("ALTER ROLE options are not supported");
            }
            match options
                .into_iter()
                .next()
                .ok_or_else(security_syntax_error)?
            {
                RoleOption::Password(password) => Ok(SecurityStatement::AlterUserPassword {
                    name,
                    password: password_bytes(password)?,
                }),
                RoleOption::Login(enabled) => {
                    Ok(SecurityStatement::SetUserEnabled { name, enabled })
                }
                _ => unsupported("only PASSWORD, LOGIN, and NOLOGIN are supported for ALTER USER"),
            }
        }
        SqlStatement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict: _,
            purge,
            temporary,
            table,
        } => {
            if names.len() != 1 || cascade || purge || temporary || table.is_some() {
                return unsupported("this DROP ROLE/USER form is not supported");
            }
            let name = object_name(names.first().ok_or_else(security_syntax_error)?)?;
            match object_type {
                ObjectType::Role => Ok(SecurityStatement::DropRole { name, if_exists }),
                ObjectType::User => Ok(SecurityStatement::DropUser { name, if_exists }),
                _ => unsupported("security DDL can drop only ROLE or USER"),
            }
        }
        SqlStatement::Grant(grant) => {
            if grant.with_grant_option
                || grant.as_grantor.is_some()
                || grant.granted_by.is_some()
                || grant.current_grants.is_some()
            {
                return unsupported("GRANT options are not supported");
            }
            let (action, object) = privilege(grant.privileges, grant.objects)?;
            let role = grantee_name(&grant.grantees)?;
            Ok(SecurityStatement::GrantPrivilege {
                role,
                action,
                object,
            })
        }
        SqlStatement::Revoke(revoke) => {
            if revoke.granted_by.is_some() || revoke.cascade.is_some() {
                return unsupported("REVOKE options are not supported");
            }
            let (action, object) = privilege(revoke.privileges, revoke.objects)?;
            let role = grantee_name(&revoke.grantees)?;
            Ok(SecurityStatement::RevokePrivilege {
                role,
                action,
                object,
            })
        }
        _ => unsupported("security DDL statement is not supported"),
    }
}

fn parse_role_membership(sql: &str) -> Result<Option<SecurityStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let uppercase = trimmed.to_ascii_uppercase();
    if !(uppercase.starts_with("GRANT ") || uppercase.starts_with("REVOKE "))
        || uppercase.contains(" ON ")
    {
        return Ok(None);
    }
    let parts = trimmed.split_ascii_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        [grant, role, to, member]
            if grant.eq_ignore_ascii_case("GRANT") && to.eq_ignore_ascii_case("TO") =>
        {
            Ok(Some(SecurityStatement::GrantRole {
                role: identifier(role)?,
                member: identifier(member)?,
            }))
        }
        [revoke, role, from, member]
            if revoke.eq_ignore_ascii_case("REVOKE") && from.eq_ignore_ascii_case("FROM") =>
        {
            Ok(Some(SecurityStatement::RevokeRole {
                role: identifier(role)?,
                member: identifier(member)?,
            }))
        }
        _ => Err(security_syntax_error()),
    }
}

fn privilege(privileges: Privileges, objects: Option<GrantObjects>) -> Result<(Action, DbObject)> {
    let object = grant_object(objects.ok_or_else(|| {
        DbError::new(
            "42601",
            "object privileges require ON followed by one object",
        )
    })?)?;
    let action = match privileges {
        Privileges::All { .. } => Action::Manage,
        Privileges::Actions(actions) if actions.len() == 1 => map_action(
            actions
                .into_iter()
                .next()
                .ok_or_else(security_syntax_error)?,
            &object,
        )?,
        Privileges::Actions(_) => {
            return unsupported("one privilege must be granted or revoked at a time");
        }
    };
    Ok((action, object))
}

fn map_action(action: SqlAction, object: &DbObject) -> Result<Action> {
    match action {
        SqlAction::Connect => Ok(Action::Connect),
        SqlAction::Select { columns: None } | SqlAction::Read => Ok(Action::Read),
        SqlAction::Insert { columns: None }
        | SqlAction::Update { columns: None }
        | SqlAction::Delete => Ok(Action::Write),
        SqlAction::Execute { obj_type: None } | SqlAction::Exec { obj_type: None } => {
            Ok(Action::Execute)
        }
        SqlAction::Usage => Ok(match object {
            DbObject::Function(_) => Action::Execute,
            _ => Action::Read,
        }),
        SqlAction::Create { obj_type: None }
        | SqlAction::Drop
        | SqlAction::References { columns: None }
        | SqlAction::Trigger
        | SqlAction::Truncate => Ok(Action::Ddl),
        _ => unsupported("this privilege action is not supported"),
    }
}

fn grant_object(object: GrantObjects) -> Result<DbObject> {
    match object {
        GrantObjects::Databases(names) => Ok(DbObject::Database(single_name(&names, "database")?)),
        GrantObjects::Schemas(names) => Ok(DbObject::Schema(single_name(&names, "schema")?)),
        GrantObjects::Tables(names) | GrantObjects::Views(names) => {
            Ok(DbObject::Table(single_name(&names, "table")?))
        }
        GrantObjects::Sequences(names) => Ok(DbObject::Sequence(single_name(&names, "sequence")?)),
        GrantObjects::Function { name, .. } | GrantObjects::Procedure { name, .. } => {
            Ok(DbObject::Function(object_name(&name)?))
        }
        _ => unsupported("this GRANT/REVOKE object type is not supported"),
    }
}

fn single_name(names: &[ObjectName], kind: &str) -> Result<String> {
    if names.len() != 1 {
        return unsupported(format!("one {kind} must be granted at a time"));
    }
    object_name(names.first().ok_or_else(security_syntax_error)?)
}

fn grantee_name(grantees: &[sqlparser::ast::Grantee]) -> Result<String> {
    if grantees.len() != 1 {
        return unsupported("one role grantee must be specified");
    }
    let name = grantees
        .first()
        .and_then(|grantee| grantee.name.as_ref())
        .ok_or_else(security_syntax_error)?;
    match name {
        GranteeName::ObjectName(name) => object_name(name),
        GranteeName::UserHost { .. } => unsupported("host-qualified grantees are not supported"),
    }
}

fn password_bytes(password: Password) -> Result<Zeroizing<Vec<u8>>> {
    let Password::Password(Expr::Value(value)) = password else {
        return unsupported("PASSWORD NULL and password expressions are not supported");
    };
    match value.value {
        SqlValue::SingleQuotedString(value) | SqlValue::DoubleQuotedString(value) => {
            Ok(Zeroizing::new(value.into_bytes()))
        }
        _ => unsupported("password must be a quoted string"),
    }
}

fn object_name(name: &ObjectName) -> Result<String> {
    identifier(&name.to_string())
}

fn identifier(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty()
        || value.contains(['"', '\'', ';'])
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(DbError::new(
            "42602",
            "security identifiers must be unquoted ASCII names",
        ));
    }
    Ok(value.to_ascii_lowercase())
}

fn security_syntax_error() -> DbError {
    DbError::new("42601", "invalid security DDL syntax")
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("0A000", message))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn parses_executes_and_redacts_supported_security_ddl() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("auth");
        let mut principal = auth
            .bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");

        for sql in [
            "CREATE ROLE analyst",
            "CREATE USER alice PASSWORD 'initial password value'",
            "GRANT analyst TO alice",
            "GRANT SELECT ON TABLE app.items TO analyst",
            "ALTER USER alice PASSWORD 'replacement password value'",
        ] {
            let statement = parse_security_statement(sql)
                .expect("parse")
                .expect("security statement");
            execute_security_statement(&auth, &mut principal, statement).expect("execute");
        }
        assert!(
            auth.authenticate_password("alice", b"replacement password value")
                .is_ok()
        );
        assert_eq!(
            redacted_security_sql("ALTER USER alice PASSWORD 'secret value'"),
            "SECURITY DDL <redacted>"
        );
        let contents = std::fs::read_to_string(auth.path()).expect("auth file");
        assert!(!contents.contains("initial password value"));
        assert!(!contents.contains("replacement password value"));

        for sql in [
            "REVOKE SELECT ON TABLE app.items FROM analyst",
            "REVOKE analyst FROM alice",
            "DROP USER alice",
            "DROP ROLE analyst",
        ] {
            let statement = parse_security_statement(sql)
                .expect("parse")
                .expect("security statement");
            execute_security_statement(&auth, &mut principal, statement).expect("execute");
        }
    }
}
