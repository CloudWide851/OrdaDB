use std::collections::BTreeSet;
use std::sync::Arc;

use ordadb_engine::{
    Engine, EngineConfig, HybridSearchRequest, ScalarSearchFilter, SearchRequest, SearchRowId,
    Session, TextSearchRequest, VectorSearchRequest,
};
use ordadb_types::{Identifier, QueryEvent, Result, Value};
use tempfile::tempdir;

fn execute(session: &mut Session, sql: &str, parameters: &[Value]) -> Result<Vec<QueryEvent>> {
    session.execute_stream(sql, parameters)?.collect()
}

#[test]
fn search_indexes_rebuild_across_mutation_rollback_reopen_and_drop() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE documents (\
            id BIGINT PRIMARY KEY, \
            title TEXT NOT NULL, \
            embedding VECTOR(3) NOT NULL\
        )",
        &[],
    )
    .expect("create table");
    for (id, title, vector) in [
        (1_i64, "rust database engine", vec![1.0, 0.0, 0.0]),
        (2_i64, "query optimizer", vec![0.0, 1.0, 0.0]),
        (3_i64, "database recovery", vec![0.8, 0.2, 0.0]),
    ] {
        execute(
            &mut session,
            "INSERT INTO documents (id, title, embedding) VALUES ($1, $2, $3)",
            &[
                Value::Int64(id),
                Value::Text(title.to_owned()),
                Value::Vector(vector),
            ],
        )
        .expect("insert");
    }
    execute(
        &mut session,
        "CREATE INDEX documents_fts ON documents USING fulltext (title) \
         WITH (analyzer = 'standard')",
        &[],
    )
    .expect("full-text index");
    execute(
        &mut session,
        "CREATE INDEX documents_hnsw ON documents USING hnsw (embedding) \
         WITH (metric = 'cosine', m = 8, ef_construction = 32, ef_search = 24)",
        &[],
    )
    .expect("HNSW index");
    for statement in [
        "REINDEX INDEX public.documents_fts",
        "REINDEX INDEX public.documents_hnsw",
        "REINDEX TABLE public.documents",
        "REINDEX SCHEMA public",
        "REINDEX DATABASE ordadb",
    ] {
        execute(&mut session, statement, &[]).expect(statement);
    }

    let catalog = engine.catalog_snapshot().expect("catalog");
    let full_text_id = catalog
        .index(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents_fts"),
        )
        .expect("full-text definition")
        .id;
    let hnsw_id = catalog
        .index(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents_hnsw"),
        )
        .expect("HNSW definition")
        .id;
    drop(catalog);

    let text_request = TextSearchRequest {
        index_id: full_text_id,
        query: "database".to_owned(),
        limit: 10,
        allowed_rows: None,
    };
    let text = session
        .search(SearchRequest::Text(text_request.clone()))
        .expect("text search");
    assert_eq!(
        text.iter().map(|hit| hit.row_id).collect::<BTreeSet<_>>(),
        BTreeSet::from([SearchRowId::new(0), SearchRowId::new(2)])
    );
    let scalar_filtered = session
        .search_with_filter(
            SearchRequest::Text(text_request.clone()),
            Some(&ScalarSearchFilter {
                expression: "id = $1".to_owned(),
                parameters: vec![Value::Int64(3)],
            }),
        )
        .expect("scalar prefilter");
    assert_eq!(
        scalar_filtered
            .iter()
            .map(|hit| hit.row_id)
            .collect::<Vec<_>>(),
        [SearchRowId::new(2)]
    );

    let vector_request = VectorSearchRequest {
        index_id: hnsw_id,
        vector: vec![1.0, 0.0, 0.0],
        limit: 3,
        ef_search: Some(32),
        allowed_rows: None,
    };
    let vector = session
        .search(SearchRequest::Vector(vector_request.clone()))
        .expect("vector search");
    assert_eq!(vector.first().expect("nearest").row_id, SearchRowId::new(0));

    let allowed = Arc::new(BTreeSet::from([SearchRowId::new(2)]));
    let hybrid = session
        .search(SearchRequest::Hybrid(HybridSearchRequest {
            text: TextSearchRequest {
                allowed_rows: Some(Arc::clone(&allowed)),
                ..text_request.clone()
            },
            vector: VectorSearchRequest {
                allowed_rows: Some(allowed),
                ..vector_request.clone()
            },
            text_weight: 0.6,
            vector_weight: 0.4,
            limit: 3,
        }))
        .expect("hybrid prefilter");
    assert_eq!(
        hybrid.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
        [SearchRowId::new(2)]
    );
    assert_eq!(hybrid[0].row.values[0], Value::Int64(3));

    execute(
        &mut session,
        "INSERT INTO documents (id, title, embedding) VALUES ($1, $2, $3)",
        &[
            Value::Int64(5),
            Value::Text("committed searchable mutation".to_owned()),
            Value::Vector(vec![0.7, 0.3, 0.0]),
        ],
    )
    .expect("committed mutation");
    assert_eq!(
        session
            .search(SearchRequest::Text(TextSearchRequest {
                query: "searchable".to_owned(),
                ..text_request.clone()
            }))
            .expect("mutation search")
            .first()
            .expect("mutation hit")
            .row_id,
        SearchRowId::new(3)
    );
    let invalid = session
        .execute_stream(
            "INSERT INTO documents (id, title, embedding) VALUES ($1, $2, $3)",
            &[
                Value::Int64(6),
                Value::Text("invalid vector mutation".to_owned()),
                Value::Vector(vec![f32::INFINITY, 0.0, 0.0]),
            ],
        )
        .expect_err("non-finite HNSW mutation");
    assert_eq!(invalid.sql_state, "22003");
    assert!(
        session
            .search(SearchRequest::Text(TextSearchRequest {
                query: "invalid".to_owned(),
                ..text_request.clone()
            }))
            .expect("failed mutation search")
            .is_empty()
    );

    execute(&mut session, "BEGIN", &[]).expect("begin");
    execute(
        &mut session,
        "INSERT INTO documents (id, title, embedding) VALUES ($1, $2, $3)",
        &[
            Value::Int64(4),
            Value::Text("transient database".to_owned()),
            Value::Vector(vec![0.9, 0.1, 0.0]),
        ],
    )
    .expect("transaction insert");
    let transient = session
        .search(SearchRequest::Text(TextSearchRequest {
            query: "transient".to_owned(),
            ..text_request.clone()
        }))
        .expect("own write search");
    assert_eq!(transient.len(), 1);
    execute(&mut session, "ROLLBACK", &[]).expect("rollback");
    assert!(
        session
            .search(SearchRequest::Text(TextSearchRequest {
                query: "transient".to_owned(),
                ..text_request.clone()
            }))
            .expect("rolled-back search")
            .is_empty()
    );

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        session
            .search(SearchRequest::Text(TextSearchRequest {
                query: "searchable".to_owned(),
                ..text_request.clone()
            }))
            .expect("reopened text search")
            .first()
            .expect("reopened mutation")
            .row_id,
        SearchRowId::new(3)
    );
    assert_eq!(
        session
            .search(SearchRequest::Vector(vector_request))
            .expect("reopened vector search")
            .first()
            .expect("nearest")
            .row_id,
        SearchRowId::new(0)
    );
    execute(&mut session, "DROP INDEX documents_fts", &[]).expect("drop full-text index");
    let missing = session
        .search(SearchRequest::Text(text_request))
        .expect_err("dropped search index");
    assert_eq!(missing.sql_state, "42704");
}
