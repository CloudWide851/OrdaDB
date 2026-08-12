use std::collections::BTreeMap;
use std::path::PathBuf;

use ordadb_server::{
    commit_installer_service, prepare_installer_service, rollback_installer_service,
};
use ordadb_types::Result;
use serde_json::Value;

use super::{ensure_empty, invalid, take, take_required};

pub(super) fn run(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let prepare = take(&mut options, "--prepare").map(PathBuf::from);
    let commit = take(&mut options, "--commit").map(PathBuf::from);
    let rollback = take(&mut options, "--rollback").map(PathBuf::from);
    let action_count = usize::from(prepare.is_some())
        + usize::from(commit.is_some())
        + usize::from(rollback.is_some());
    if action_count != 1 {
        return Err(invalid(
            "installer-service requires exactly one of --prepare, --commit, or --rollback transaction paths",
        ));
    }
    let output = if let Some(transaction) = prepare {
        let executable = PathBuf::from(take_required(&mut options, "--executable")?);
        let data_dir = PathBuf::from(take_required(&mut options, "--data-dir")?);
        ensure_empty(&options)?;
        prepare_installer_service(&transaction, executable, data_dir)?
    } else if let Some(transaction) = commit {
        ensure_empty(&options)?;
        commit_installer_service(&transaction)?
    } else if let Some(transaction) = rollback {
        ensure_empty(&options)?;
        rollback_installer_service(&transaction)?
    } else {
        unreachable!("exactly one action was validated")
    };
    serde_json::to_value(output).map_err(|error| {
        ordadb_types::DbError::internal("failed to encode installer service result")
            .with_detail(error.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installer_service_requires_one_transaction_action() {
        let mut options =
            BTreeMap::from([("--unrelated".to_owned(), Some("state.json".to_owned()))]);
        let error = run(std::mem::take(&mut options)).expect_err("missing action");
        assert_eq!(error.sql_state, "22023");
    }
}
