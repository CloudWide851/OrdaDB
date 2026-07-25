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

    pub fn authorize_sql(&self, principal: &Principal, database: &str, sql: &str) -> Result<()> {
        let (action, object) = classify_sql(database, sql);
        self.authorize(principal, action, &object)
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
        _ => granted == requested,
    }
}

fn classify_sql(database: &str, sql: &str) -> (Action, DbObject) {
    let normalized = sql.trim_start();
    let keyword = normalized
        .split_ascii_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let action = match keyword.as_str() {
        "SELECT" | "EXPLAIN" | "SHOW" => Action::Read,
        "INSERT" | "UPDATE" | "DELETE" | "COPY" => Action::Write,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" => Action::Ddl,
        "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "SET" => Action::Connect,
        _ => Action::Execute,
    };
    (action, DbObject::Database(database.to_ascii_lowercase()))
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
}
