use ordadb_sql::{ParsedStatement, StatementEffect, classify_statement_effect, parse};

fn effect(sql: &str) -> StatementEffect {
    let statement = parse(sql).unwrap_or_else(|error| panic!("failed to parse {sql:?}: {error}"));
    classify_statement_effect(&statement)
}

#[test]
fn read_only_queries_and_nested_query_shapes_are_classified_from_the_ast() {
    for sql in [
        "SELECT 1",
        "SELECT * FROM items WHERE id IN (SELECT id FROM other_items)",
        "WITH source AS (SELECT id FROM items) SELECT id FROM source",
        "SELECT id FROM items UNION SELECT id FROM other_items",
        "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM items",
        "EXPLAIN SELECT * FROM items",
    ] {
        assert_eq!(effect(sql), StatementEffect::ReadOnly, "{sql}");
    }
}

#[test]
fn mutations_session_state_routines_and_maintenance_require_approval() {
    for sql in [
        "INSERT INTO items (id) VALUES (1)",
        "UPDATE items SET id = 2",
        "DELETE FROM items",
        "CREATE TABLE items (id INTEGER)",
        "BEGIN",
        "VACUUM",
        "LISTEN updates",
        "CALL refresh_items()",
        "SELECT nextval('items_id_seq')",
        "SELECT pg_notify('updates', 'ready')",
        "EXPLAIN DELETE FROM items",
    ] {
        assert_eq!(effect(sql), StatementEffect::RequiresApproval, "{sql}");
    }

    assert_eq!(
        parse("WITH deleted AS (DELETE FROM items RETURNING id) SELECT id FROM deleted")
            .expect_err("data-modifying CTE must fail closed before classification")
            .sql_state,
        "0A000"
    );
}

#[test]
fn excessive_classifier_depth_fails_closed_without_native_recursion() {
    let mut statement = ParsedStatement::ScalarSelect {
        projection: Vec::new(),
    };
    for _ in 0..66 {
        statement = ParsedStatement::Explain {
            statement: Box::new(statement),
        };
    }
    assert_eq!(
        classify_statement_effect(&statement),
        StatementEffect::RequiresApproval
    );
}
