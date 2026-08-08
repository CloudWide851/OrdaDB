use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use ordadb_types::{DbError, Result};

use crate::{AuthStore, Principal};

const MAX_ROLE_DEPTH: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Connect,
    Read,
    Write,
    Ddl,
    Execute,
    Monitor,
    Backup,
    Manage,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum DbObject {
    Server,
    Database(String),
    Schema(String),
    Table(String),
    Sequence(String),
    Function(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    #[serde(default)]
    pub inherits: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub role: String,
    pub action: Action,
    pub object: DbObject,
}

#[derive(Debug, Clone)]
pub struct Authorizer {
    roles: BTreeMap<String, Role>,
    grants: Vec<Grant>,
}

impl Authorizer {
    pub fn from_store(store: &AuthStore) -> Result<Self> {
        let (roles, grants) = store.authorization_snapshot()?;
        Ok(Self { roles, grants })
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        object: &DbObject,
    ) -> Result<()> {
        let effective = self.effective_roles(&principal.roles)?;
        let allowed = self.grants.iter().any(|grant| {
            effective.contains(&grant.role)
                && (grant.action == Action::Manage || grant.action == action)
                && object_matches(&grant.object, object)
        });
        if allowed {
            return Ok(());
        }
        Err(DbError::new("42501", "permission denied")
            .with_detail(format!("{} lacks {action:?} on {object:?}", principal.user)))
    }

    pub fn authorize_all(
        &self,
        principal: &Principal,
        actions: &[Action],
        object: &DbObject,
    ) -> Result<()> {
        for action in actions {
            self.authorize(principal, *action, object)?;
        }
        Ok(())
    }

    pub fn authorize_sql(&self, principal: &Principal, database: &str, sql: &str) -> Result<()> {
        let (action, object) = classify_sql(database, sql);
        self.authorize(principal, action, &object)
    }

    /// Return the bounded RBAC object scopes that make catalog objects
    /// discoverable to this principal. Callers must still apply ownership and
    /// public-visibility rules against their transactional catalog snapshot.
    pub fn discovery_objects(&self, principal: &Principal) -> Result<BTreeSet<DbObject>> {
        let effective = self.effective_roles(&principal.roles)?;
        Ok(self
            .grants
            .iter()
            .filter(|grant| effective.contains(&grant.role))
            .map(|grant| grant.object.clone())
            .collect())
    }

    fn effective_roles(&self, roots: &BTreeSet<String>) -> Result<BTreeSet<String>> {
        let mut effective = BTreeSet::new();
        let mut stack: Vec<(String, usize)> = roots.iter().cloned().map(|role| (role, 0)).collect();
        while let Some((name, depth)) = stack.pop() {
            if depth > MAX_ROLE_DEPTH {
                return Err(DbError::new(
                    "54001",
                    "role inheritance depth exceeds the configured limit",
                ));
            }
            if !effective.insert(name.clone()) {
                continue;
            }
            let role = self.roles.get(&name).ok_or_else(|| {
                DbError::new("XX001", format!("principal references unknown role {name}"))
            })?;
            stack.extend(
                role.inherits
                    .iter()
                    .cloned()
                    .map(|parent| (parent, depth + 1)),
            );
        }
        Ok(effective)
    }
}

fn object_matches(granted: &DbObject, requested: &DbObject) -> bool {
    match granted {
        DbObject::Server => true,
        DbObject::Database(_) => !matches!(requested, DbObject::Server),
        DbObject::Schema(schema) => match requested {
            DbObject::Schema(requested) => requested == schema,
            DbObject::Table(requested)
            | DbObject::Sequence(requested)
            | DbObject::Function(requested) => requested
                .strip_prefix(schema)
                .is_some_and(|suffix| suffix.starts_with('.')),
            DbObject::Server | DbObject::Database(_) => false,
        },
        _ => granted == requested,
    }
}

fn classify_sql(database: &str, sql: &str) -> (Action, DbObject) {
    let normalized = sql.trim_start();
    let tokens = normalized.split_ascii_whitespace().collect::<Vec<_>>();
    let keyword = tokens
        .first()
        .copied()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let action = match keyword.as_str() {
        "SELECT" | "EXPLAIN" | "SHOW" => Action::Read,
        "INSERT" | "UPDATE" | "DELETE" | "COPY" => Action::Write,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" => Action::Ddl,
        "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "SET" | "CONNECT" => Action::Connect,
        _ => Action::Execute,
    };
    let object = match keyword.as_str() {
        "SELECT" => token_after(&tokens, "FROM").map(DbObject::Table),
        "INSERT" => token_after(&tokens, "INTO").map(DbObject::Table),
        "UPDATE" => tokens
            .get(1)
            .and_then(|token| object_token(token))
            .map(DbObject::Table),
        "DELETE" => token_after(&tokens, "FROM").map(DbObject::Table),
        "CALL" => tokens
            .get(1)
            .and_then(|token| object_token(token))
            .map(DbObject::Function),
        _ => None,
    }
    .unwrap_or_else(|| DbObject::Database(database.to_ascii_lowercase()));
    (action, object)
}

fn token_after(tokens: &[&str], keyword: &str) -> Option<String> {
    tokens.windows(2).find_map(|window| {
        window[0]
            .eq_ignore_ascii_case(keyword)
            .then(|| object_token(window[1]))
            .flatten()
    })
}

fn object_token(value: &str) -> Option<String> {
    let value = value
        .trim_matches(|character: char| matches!(character, '"' | '\'' | '(' | ')' | ',' | ';'))
        .split('(')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    (!value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')))
    .then_some(value)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn bootstrap_administrator_is_allowed_and_unknown_role_is_rejected() {
        let directory = tempdir().expect("tempdir");
        let store = AuthStore::open(directory.path()).expect("open");
        let principal = store
            .bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");
        let authorizer = Authorizer::from_store(&store).expect("authorizer");
        authorizer
            .authorize(
                &principal,
                Action::Backup,
                &DbObject::Database("ordadb".into()),
            )
            .expect("admin");
        let unknown = Principal {
            user: "guest".into(),
            roles: BTreeSet::from(["guest".into()]),
        };
        assert_eq!(
            authorizer
                .authorize(&unknown, Action::Read, &DbObject::Server)
                .expect_err("deny")
                .sql_state,
            "XX001"
        );
    }

    #[test]
    fn export_authorization_requires_read_and_backup() {
        let directory = tempdir().expect("tempdir");
        let store = AuthStore::open(directory.path()).expect("open");
        store
            .bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");
        store.create_role("exporter", false).expect("create role");
        store
            .create_user("analyst", b"correct horse battery staple", false)
            .expect("create user");
        store.grant_role("exporter", "analyst").expect("grant role");
        let table = DbObject::Table("public.items".into());
        store
            .grant_privilege("exporter", Action::Backup, table.clone())
            .expect("grant backup");
        let principal = store.principal("analyst").expect("principal");
        let authorizer = Authorizer::from_store(&store).expect("authorizer");
        assert_eq!(
            authorizer
                .authorize_all(&principal, &[Action::Read, Action::Backup], &table)
                .expect_err("read grant is required")
                .sql_state,
            "42501"
        );

        store
            .grant_privilege("exporter", Action::Read, table.clone())
            .expect("grant read");
        Authorizer::from_store(&store)
            .expect("authorizer")
            .authorize_all(&principal, &[Action::Read, Action::Backup], &table)
            .expect("both grants authorize export");
    }

    #[test]
    fn role_depth_is_bounded_iteratively() {
        let mut roles = BTreeMap::new();
        for index in 0..=MAX_ROLE_DEPTH + 1 {
            roles.insert(
                format!("role{index}"),
                Role {
                    name: format!("role{index}"),
                    inherits: if index == 0 {
                        BTreeSet::new()
                    } else {
                        BTreeSet::from([format!("role{}", index - 1)])
                    },
                },
            );
        }
        let authorizer = Authorizer {
            roles,
            grants: Vec::new(),
        };
        let error = authorizer
            .effective_roles(&BTreeSet::from([format!("role{}", MAX_ROLE_DEPTH + 1)]))
            .expect_err("depth");
        assert_eq!(error.sql_state, "54001");
    }

    #[test]
    fn sql_classification_respects_database_schema_and_table_grants() {
        let principal = Principal {
            user: "alice".into(),
            roles: BTreeSet::from(["reader".into()]),
        };
        let role = Role {
            name: "reader".into(),
            inherits: BTreeSet::new(),
        };
        let authorizer = Authorizer {
            roles: BTreeMap::from([("reader".into(), role)]),
            grants: vec![Grant {
                role: "reader".into(),
                action: Action::Read,
                object: DbObject::Schema("app".into()),
            }],
        };
        authorizer
            .authorize_sql(&principal, "ordadb", "SELECT * FROM app.items")
            .expect("schema covers table");
        assert_eq!(
            authorizer
                .authorize_sql(&principal, "ordadb", "SELECT * FROM private.items")
                .expect_err("different schema")
                .sql_state,
            "42501"
        );
        assert_eq!(
            classify_sql("ordadb", "UPDATE app.items SET value = 1"),
            (Action::Write, DbObject::Table("app.items".into()))
        );
    }
}
