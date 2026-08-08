use ordadb_engine::{Engine, EngineConfig};
use ordadb_types::{QueryEvent, Row, ScalarType, Schema, Value};
use tempfile::tempdir;

#[test]
fn datagrip_catalog_queries_use_postgresql_catalog_types() {
    let directory = tempdir().expect("temporary data directory");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine.connect().expect("connect session");

    let (namespace_schema, namespace_rows) = query(
        &mut session,
        "SELECT n.oid AS schema_oid, n.nspname AS schema_name, r.rolname AS owner_name \
         FROM pg_catalog.pg_namespace AS n \
         LEFT JOIN pg_catalog.pg_roles AS r ON r.oid = n.nspowner \
         WHERE n.nspname IN ('pg_catalog', 'information_schema', 'public') \
         ORDER BY n.nspname LIMIT 256",
    );
    assert_schema(
        &namespace_schema,
        &[
            ("schema_oid", ScalarType::Oid),
            ("schema_name", ScalarType::Name),
            ("owner_name", ScalarType::Name),
        ],
    );
    assert_eq!(namespace_rows.len(), 3);
    assert_eq!(
        namespace_rows
            .iter()
            .map(|row| row.values[1].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("information_schema".into()),
            Value::Text("pg_catalog".into()),
            Value::Text("public".into()),
        ]
    );

    let (relation_schema, relation_rows) = query(
        &mut session,
        "SELECT c.oid AS relation_oid, n.oid AS schema_oid, n.nspname AS schema_name, \
                c.relname AS relation_name, c.relkind \
         FROM pg_catalog.pg_class AS c \
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r', 'v', 'm', 'S') \
         ORDER BY n.nspname, c.relname, c.oid LIMIT 256",
    );
    assert_schema(
        &relation_schema,
        &[
            ("relation_oid", ScalarType::Oid),
            ("schema_oid", ScalarType::Oid),
            ("schema_name", ScalarType::Name),
            ("relation_name", ScalarType::Name),
            ("relkind", ScalarType::InternalChar),
        ],
    );
    assert!(!relation_rows.is_empty());
    assert!(relation_rows.iter().all(|row| {
        matches!(row.values[0], Value::Int64(_))
            && matches!(row.values[1], Value::Int64(_))
            && matches!(row.values[2], Value::Text(_))
            && matches!(row.values[3], Value::Text(_))
            && matches!(&row.values[4], Value::Text(value) if value.len() == 1)
    }));
}

fn query(session: &mut ordadb_engine::Session, sql: &str) -> (Schema, Vec<Row>) {
    let events = session.execute(sql, &[]).expect("execute query");
    let mut schema = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            QueryEvent::Schema(value) => schema = Some(value),
            QueryEvent::Batch(batch) => rows.extend(batch.rows),
            QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
        }
    }
    (schema.expect("query schema"), rows)
}

fn assert_schema(schema: &Schema, expected: &[(&str, ScalarType)]) {
    assert_eq!(schema.fields.len(), expected.len());
    for (field, (name, data_type)) in schema.fields.iter().zip(expected) {
        assert_eq!(field.name, *name);
        assert_eq!(&field.data_type, data_type);
    }
}
