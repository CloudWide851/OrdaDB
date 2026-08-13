
#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, header};
    use serde_json::Value;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use ordadb_engine::EngineConfig;

    use super::*;

    fn state() -> (tempfile::TempDir, AdminState) {
        let directory = tempdir().expect("tempdir");
        let engine = Arc::new(
            Engine::open(EngineConfig::new(directory.path().join("data"))).expect("engine"),
        );
        let auth = Arc::new(AuthStore::open(directory.path().join("data")).expect("auth store"));
        auth.bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");
        let registry = Arc::new(SessionRegistry::default());
        (directory, AdminState::new(engine, auth, registry))
    }

    #[tokio::test]
    async fn health_is_public_but_catalog_requires_a_bearer_token() {
        let (_directory, state) = state();
        let app = api_router(state);
        let live = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health/live")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("live");
        assert_eq!(live.status(), StatusCode::OK);
        let catalog = app
            .oneshot(
                Request::builder()
                    .uri("/v1/catalog")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("catalog");
        assert_eq!(catalog.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn issued_token_can_read_metrics() {
        let (_directory, state) = state();
        let app = api_router(state);
        let token_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/auth/token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"username":"dba","password":"correct horse battery staple"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("token");
        assert_eq!(token_response.status(), StatusCode::CREATED);
        let bytes = to_bytes(token_response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        let token = body["data"]["accessToken"].as_str().expect("token");
        let metrics = app
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("metrics");
        assert_eq!(metrics.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn lock_route_projects_active_engine_locks() {
        let (_directory, state) = state();
        let token = state
            .tokens
            .issue(&state.auth, "dba", b"correct horse battery staple")
            .expect("token")
            .access_token;
        let mut session = state.engine.connect().expect("session");
        session
            .execute("CREATE TABLE lock_probe (id INT PRIMARY KEY)", &[])
            .expect("create table");
        let mut transaction = session.begin().expect("transaction");
        transaction
            .execute("INSERT INTO lock_probe VALUES (1)", &[])
            .expect("insert");
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/locks")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("locks");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["data"]["singleWriter"], false);
        assert!(
            body["data"]["activeLocks"]
                .as_array()
                .expect("active locks")
                .iter()
                .any(|lock| lock.as_str().is_some_and(|lock| {
                    lock.starts_with("granted transaction=") && lock.contains("resource=")
                }))
        );
        transaction.rollback().expect("rollback");
    }

    #[test]
    fn catalog_projection_exposes_only_safe_search_index_metadata() {
        let (_directory, state) = state();
        let mut session = state.engine.connect().expect("session");
        for sql in [
            "CREATE TABLE documents (title TEXT, embedding VECTOR(3))",
            "CREATE INDEX documents_fts ON documents USING fulltext (title) \
             WITH (analyzer = 'whitespace')",
            "CREATE INDEX documents_hnsw ON documents USING hnsw (embedding) \
             WITH (metric = 'cosine', m = 8, ef_construction = 32, ef_search = 24)",
        ] {
            session
                .execute_stream(sql, &[])
                .expect("execute")
                .collect::<ordadb_types::Result<Vec<_>>>()
                .expect("drain");
        }
        let catalog = state.engine.catalog_snapshot().expect("catalog");
        let projection =
            serde_json::to_value(CatalogProjection::from_catalog(&catalog)).expect("projection");
        let indexes = projection["database"]["schemas"][0]["tables"][0]["indexes"]
            .as_array()
            .expect("indexes");
        assert_eq!(indexes[0]["method"], "full_text");
        assert_eq!(indexes[0]["options"]["kind"], "full_text");
        assert_eq!(indexes[0]["options"]["analyzer"], "whitespace");
        assert_eq!(indexes[1]["method"], "hnsw");
        assert_eq!(indexes[1]["options"]["kind"], "hnsw");
        assert!(indexes[1].get("path").is_none());
        assert!(indexes[1].get("graph").is_none());
    }

    #[tokio::test]
    async fn every_management_route_has_an_authenticated_contract() {
        let (_directory, state) = state();
        let token = state
            .tokens
            .issue(&state.auth, "dba", b"correct horse battery staple")
            .expect("token")
            .access_token;
        let app = api_router(state);

        let ready = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health/ready")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready");
        assert_eq!(ready.status(), StatusCode::OK);

        for path in [
            "/v1/catalog",
            "/v1/sessions",
            "/v1/locks",
            "/v1/queries",
            "/v1/metrics",
            "/v1/storage",
            "/v1/wal",
            "/v1/backups",
            "/v1/operations",
            "/v1/service",
            "/v1/config",
            "/v1/logs/stream",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .expect("authorized request"),
                )
                .await
                .expect("authorized response");
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }

        let checkpoint = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoint")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("checkpoint request"),
            )
            .await
            .expect("checkpoint");
        assert_eq!(checkpoint.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/config")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("unsupported request"),
            )
            .await
            .expect("unsupported response");
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn backup_route_starts_a_real_bounded_operation() {
        let (_directory, state) = state();
        let token = state
            .tokens
            .issue(&state.auth, "dba", b"correct horse battery staple")
            .expect("token")
            .access_token;
        let app = api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/backups")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"path":"api-backup.orda"}"#))
                    .expect("backup request"),
            )
            .await
            .expect("backup response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("json");
        let operation_id =
            Uuid::parse_str(body["data"]["operationId"].as_str().expect("operation ID"))
                .expect("UUID");
        for _ in 0..100 {
            let operation = state.operations.get(operation_id).expect("operation");
            if !matches!(
                operation.state,
                crate::OperationState::Queued | crate::OperationState::Running
            ) {
                assert_eq!(operation.state, crate::OperationState::Succeeded);
                assert_eq!(operation.path, PathBuf::from("api-backup.orda"));
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("backup operation did not finish");
    }
}
