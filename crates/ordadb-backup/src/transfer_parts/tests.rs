
#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_engine::EngineConfig;
    use ordadb_types::QueryEvent;
    use tempfile::tempdir;

    fn engine(data_dir: &Path) -> Engine {
        let engine = Engine::open(EngineConfig::new(data_dir)).expect("engine");
        let mut session = engine.connect().expect("session");
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT NOT NULL, enabled BOOLEAN)",
                &[],
            )
            .expect("create");
        engine
    }

    fn row_count(engine: &Engine) -> usize {
        let mut session = engine.connect().expect("session");
        session
            .execute("SELECT * FROM items", &[])
            .expect("select")
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows.len()),
                _ => None,
            })
            .sum()
    }

    #[test]
    fn csv_and_json_lines_round_trip_through_atomic_files() {
        let directory = tempdir().expect("tempdir");
        let operations = directory.path().join("operations");
        fs::create_dir(&operations).expect("operations");
        let source = engine(&directory.path().join("source"));
        fs::write(
            operations.join("items.csv"),
            "id,label,enabled\r\n1,alpha,true\r\n2,beta,\\N\r\n",
        )
        .expect("csv");
        let request = TableTransferRequest {
            schema: "public".into(),
            table: "items".into(),
            path: "items.csv".into(),
            format: TransferFormat::Csv,
        };
        let imported = import_table(
            &source,
            &operations,
            &request,
            TransferLimits::default(),
            None,
        )
        .expect("import");
        assert_eq!(imported.rows, 2);

        let export = TableTransferRequest {
            path: "items.jsonl".into(),
            format: TransferFormat::JsonLines,
            ..request
        };
        let exported = export_table(
            &source,
            &operations,
            &export,
            TransferLimits::default(),
            None,
        )
        .expect("export");
        assert_eq!(exported.rows, 2);
        let destination = engine(&directory.path().join("destination"));
        import_table(
            &destination,
            &operations,
            &export,
            TransferLimits::default(),
            None,
        )
        .expect("JSON import");
        assert_eq!(row_count(&destination), 2);
    }

    #[test]
    fn postgres_catalog_scalar_transfer_values_preserve_bounds() {
        assert_eq!(
            json_to_value(&serde_json::json!(u32::MAX), &ScalarType::Oid)
                .expect("maximum JSON OID"),
            Value::Int64(i64::from(u32::MAX))
        );
        assert_eq!(
            json_to_value(
                &serde_json::json!(u64::from(u32::MAX) + 1),
                &ScalarType::Oid
            )
            .expect_err("out-of-range JSON OID")
            .sql_state,
            "22003"
        );
        assert_eq!(
            text_to_value("42", &ScalarType::Oid).expect("text OID"),
            Value::Int64(42)
        );
        assert_eq!(
            text_to_value(&"n".repeat(MAX_POSTGRES_NAME_BYTES + 1), &ScalarType::Name)
                .expect_err("oversized name")
                .sql_state,
            "22001"
        );
        assert_eq!(
            text_to_value("é", &ScalarType::InternalChar)
                .expect_err("multibyte internal char")
                .sql_state,
            "22P02"
        );
    }

    #[test]
    fn malformed_or_cancelled_import_rolls_back_every_row() {
        let directory = tempdir().expect("tempdir");
        let operations = directory.path().join("operations");
        fs::create_dir(&operations).expect("operations");
        let engine = engine(&directory.path().join("data"));
        fs::write(
            operations.join("bad.csv"),
            "id,label,enabled\n1,alpha,true\n1,duplicate,false\n",
        )
        .expect("bad csv");
        let request = TableTransferRequest {
            schema: "public".into(),
            table: "items".into(),
            path: "bad.csv".into(),
            format: TransferFormat::Csv,
        };
        assert_eq!(
            import_table(
                &engine,
                &operations,
                &request,
                TransferLimits::default(),
                None,
            )
            .expect_err("duplicate")
            .sql_state,
            "23505"
        );
        assert_eq!(row_count(&engine), 0);

        let cancelled = AtomicBool::new(true);
        assert_eq!(
            import_table(
                &engine,
                &operations,
                &request,
                TransferLimits::default(),
                Some(&cancelled),
            )
            .expect_err("cancelled")
            .sql_state,
            "57014"
        );
        assert_eq!(row_count(&engine), 0);
    }

    #[test]
    fn operation_paths_cannot_escape_the_root() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("operations");
        fs::create_dir(&root).expect("root");
        assert_eq!(
            resolve_operation_path(&root, directory.path().join("outside.csv"), true)
                .expect_err("outside")
                .sql_state,
            "42501"
        );
    }
}
