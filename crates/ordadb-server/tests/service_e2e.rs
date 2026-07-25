use std::net::SocketAddr;
use std::time::Duration;

use ordadb_protocol::{ClientConfig, PgClient};
use ordadb_server::{ServerConfig, request_bootstrap, start_server};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::tempdir;
use zeroize::Zeroizing;

const ADMIN_USER: &str = "dba";
const ADMIN_PASSWORD: &str = "correct horse battery staple";
const OID_INT8: u32 = 20;

fn client(address: SocketAddr, password: &str) -> ordadb_types::Result<PgClient> {
    PgClient::connect(ClientConfig {
        address,
        user: ADMIN_USER.into(),
        database: "ordadb".into(),
        password: Zeroizing::new(password.into()),
        application_name: "ordadb-service-e2e".into(),
    })
}

async fn management_token(address: SocketAddr) -> String {
    let response: Value = reqwest::Client::new()
        .post(format!("http://{address}/v1/auth/token"))
        .json(&json!({
            "username": ADMIN_USER,
            "password": ADMIN_PASSWORD,
        }))
        .send()
        .await
        .expect("token request")
        .error_for_status()
        .expect("token status")
        .json()
        .await
        .expect("token JSON");
    response["data"]["accessToken"]
        .as_str()
        .expect("access token")
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_management_copy_and_restart_are_end_to_end() {
    let directory = tempdir().expect("tempdir");
    let mut config = ServerConfig::new(directory.path());
    config.pg_bind = "127.0.0.1:0".parse().expect("PG bind");
    config.admin_bind = "127.0.0.1:0".parse().expect("admin bind");

    let first = start_server(config.clone()).await.expect("first server");
    let bootstrap_pipe = first.bootstrap_pipe.clone().expect("bootstrap pipe");
    let bootstrap = request_bootstrap(
        &bootstrap_pipe,
        ADMIN_USER.into(),
        Zeroizing::new(ADMIN_PASSWORD.into()),
    )
    .await
    .expect("bootstrap request");
    assert!(bootstrap.success);
    assert_eq!(bootstrap.user.as_deref(), Some(ADMIN_USER));

    let invalid_address = first.pg_address;
    let invalid =
        tokio::task::spawn_blocking(move || client(invalid_address, "wrong password value"))
            .await
            .expect("invalid auth task")
            .expect_err("invalid SCRAM must fail");
    assert_eq!(invalid.sql_state, "28P01");
    assert_eq!(invalid.message, "authentication failed");

    let pg_address = first.pg_address;
    let mut pg = tokio::task::spawn_blocking(move || client(pg_address, ADMIN_PASSWORD))
        .await
        .expect("connect task")
        .expect("connect");
    let mut pg = tokio::task::spawn_blocking(move || {
        pg.query("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")?;
        pg.query("INSERT INTO items VALUES (1, 'first'), (2, 'semi;colon')")?;

        let selected = pg.query("SELECT id, name FROM items ORDER BY id")?;
        assert_eq!(selected.columns, ["id", "name"]);
        assert_eq!(selected.rows.len(), 2);
        assert_eq!(selected.rows[1][1].as_deref(), Some("semi;colon"));

        let extended = pg.query_prepared(
            "SELECT id, name FROM items WHERE id >= $1 ORDER BY id",
            &[OID_INT8],
            &[Some(b"2".to_vec())],
            1,
        )?;
        assert_eq!(extended.columns, ["id", "name"]);
        assert_eq!(
            extended.rows,
            vec![vec![Some("2".into()), Some("semi;colon".into())]]
        );
        assert_eq!(extended.command_tags, ["SELECT 1"]);

        let extended_error = pg
            .query_prepared(
                "SELECT id FROM items WHERE id >= $1",
                &[OID_INT8],
                &[Some(b"not-an-integer".to_vec())],
                0,
            )
            .expect_err("invalid extended parameter");
        assert_eq!(extended_error.sql_state, "22P02");
        assert_eq!(pg.query("SELECT id FROM items")?.rows.len(), 2);

        let copy_error = pg
            .copy_from_stdin("items", b"5,valid before failure\n6,too,many,columns\n")
            .expect_err("invalid COPY input");
        assert_eq!(copy_error.sql_state, "22P04");
        assert!(
            pg.query("SELECT id FROM items WHERE id = 5")?
                .rows
                .is_empty(),
            "failed COPY must roll back rows imported before the error"
        );

        assert_eq!(
            pg.copy_from_stdin("items", b"3,copy row\n4,second copy row\n")?,
            "COPY 2"
        );
        let copied = pg.copy_to_stdout("items")?;
        assert_eq!(copied.columns, 2);
        assert_eq!(copied.command_tag, "COPY 4");
        let copied = String::from_utf8(copied.data).expect("COPY UTF-8");
        assert!(copied.contains("3,copy row"));
        assert!(copied.contains("4,second copy row"));
        Ok::<PgClient, ordadb_types::DbError>(pg)
    })
    .await
    .expect("query task")
    .expect("PG work");

    let http = reqwest::Client::new();
    let live = http
        .get(format!("http://{}/v1/health/live", first.admin_address))
        .send()
        .await
        .expect("live request");
    assert_eq!(live.status(), StatusCode::OK);
    let token = management_token(first.admin_address).await;

    let mut left_rows = String::new();
    let mut right_rows = String::new();
    for id in 1..=1_000 {
        left_rows.push_str(&format!("{id},1\n"));
        right_rows.push_str(&format!("{id},1\n"));
    }
    pg = tokio::task::spawn_blocking(move || {
        pg.query("CREATE TABLE cancel_left (id BIGINT PRIMARY KEY, match_id BIGINT NOT NULL)")?;
        pg.query("CREATE TABLE cancel_right (id BIGINT PRIMARY KEY, match_id BIGINT NOT NULL)")?;
        pg.copy_from_stdin("cancel_left", left_rows.as_bytes())?;
        pg.copy_from_stdin("cancel_right", right_rows.as_bytes())?;
        Ok::<PgClient, ordadb_types::DbError>(pg)
    })
    .await
    .expect("cancellation fixture task")
    .expect("cancellation fixture");

    let cancel = pg.cancellation_token();
    let query_task = tokio::task::spawn_blocking(move || {
        let result = pg.query(
            "SELECT l.id, r.id FROM cancel_left l \
             INNER JOIN cancel_right r ON l.match_id = r.match_id",
        );
        (result, pg)
    });
    let queries_url = format!("http://{}/v1/queries", first.admin_address);
    let mut observed_running = false;
    for _ in 0..200 {
        let queries: Value = http
            .get(&queries_url)
            .bearer_auth(&token)
            .send()
            .await
            .expect("queries request")
            .error_for_status()
            .expect("queries status")
            .json()
            .await
            .expect("queries JSON");
        observed_running = queries["data"].as_array().is_some_and(|queries| {
            queries.iter().any(|query| {
                query["outcome"] == "running"
                    && query["sql"]
                        .as_str()
                        .is_some_and(|sql| sql.contains("cancel_left"))
            })
        });
        if observed_running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        observed_running,
        "management API must expose the active query"
    );
    tokio::task::spawn_blocking(move || cancel.cancel())
        .await
        .expect("cancel task")
        .expect("cancel request");
    let (cancelled, mut pg) = query_task.await.expect("cancelled query task");
    let cancelled = cancelled.expect_err("query must be cancelled");
    assert_eq!(cancelled.sql_state, "57014");
    let reusable = tokio::task::spawn_blocking(move || {
        let result = pg.query("SELECT id FROM items ORDER BY id")?;
        Ok::<(PgClient, usize), ordadb_types::DbError>((pg, result.rows.len()))
    })
    .await
    .expect("reusable client task")
    .expect("reusable client");
    assert_eq!(reusable.1, 4);
    drop(reusable.0);

    let catalog: Value = http
        .get(format!("http://{}/v1/catalog", first.admin_address))
        .bearer_auth(&token)
        .send()
        .await
        .expect("catalog request")
        .error_for_status()
        .expect("catalog status")
        .json()
        .await
        .expect("catalog JSON");
    assert_eq!(catalog["data"]["database"]["name"], "u:ordadb");
    let checkpoint = http
        .post(format!("http://{}/v1/checkpoint", first.admin_address))
        .bearer_auth(&token)
        .send()
        .await
        .expect("checkpoint request");
    assert_eq!(checkpoint.status(), StatusCode::OK);
    drop(http);

    tokio::time::timeout(Duration::from_secs(5), first.shutdown())
        .await
        .expect("first shutdown timeout")
        .expect("first shutdown");

    let second = start_server(config).await.expect("second server");
    assert!(second.bootstrap_pipe.is_none());
    let second_address = second.pg_address;
    let active_client = tokio::task::spawn_blocking(move || -> ordadb_types::Result<PgClient> {
        let mut pg = client(second_address, ADMIN_PASSWORD)?;
        let selected = pg.query("SELECT id, name FROM items ORDER BY id")?;
        assert_eq!(selected.rows.len(), 4);
        assert_eq!(selected.rows[3][1].as_deref(), Some("second copy row"));
        Ok(pg)
    })
    .await
    .expect("reconnect task")
    .expect("restart query");

    let stopped_address = second.pg_address;
    tokio::time::timeout(Duration::from_secs(5), second.shutdown())
        .await
        .expect("second shutdown timeout")
        .expect("second shutdown");
    drop(active_client);
    let closed = tokio::task::spawn_blocking(move || client(stopped_address, ADMIN_PASSWORD))
        .await
        .expect("closed connection task")
        .expect_err("stopped listener must refuse new connections");
    assert!(closed.sql_state.starts_with("08"));
}
