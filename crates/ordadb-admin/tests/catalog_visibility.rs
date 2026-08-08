use ordadb_admin::{Action, AuthStore, Authorizer, DbObject};
use tempfile::tempdir;

#[test]
fn inherited_object_privilege_is_discoverable() {
    let directory = tempdir().expect("tempdir");
    let store = AuthStore::open(directory.path()).expect("open auth store");
    let password = [b'p'; 32];
    store
        .bootstrap_admin("dba", &password)
        .expect("bootstrap administrator");
    store
        .create_role("catalog_reader", false)
        .expect("create inherited role");
    store
        .create_user("analyst", &password, false)
        .expect("create user");
    store
        .grant_role("catalog_reader", "analyst")
        .expect("grant inherited role");

    let table = DbObject::Table("public.items".into());
    store
        .grant_privilege("catalog_reader", Action::Read, table.clone())
        .expect("grant object privilege");

    let principal = store.principal("analyst").expect("principal");
    let visible = Authorizer::from_store(&store)
        .expect("authorizer")
        .discovery_objects(&principal)
        .expect("discovery scopes");
    assert!(visible.contains(&table));
}
