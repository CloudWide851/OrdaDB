
fn validate_language_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || value.starts_with(['-', '.', '_'])
        || value.ends_with(['-', '.', '_'])
        || value.bytes().any(|byte| {
            !byte.is_ascii_lowercase()
                && !byte.is_ascii_digit()
                && !matches!(byte, b'-' | b'.' | b'_')
        })
    {
        return Err(protocol_error("connector command language ID is invalid"));
    }
    Ok(())
}

fn validate_opaque_id(value: &str, name: &str) -> Result<()> {
    validate_bounded_text(value, name, MAX_CONNECTOR_IDENTIFIER_BYTES)
}

fn validate_bounded_text(value: &str, name: &str, maximum: usize) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(protocol_error(format!(
            "{name} must contain 1-{maximum} printable bytes"
        )));
    }
    Ok(())
}

fn valid_sql_state(value: &str) -> bool {
    value.len() == 5
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

const fn category_sql_state(category: ConnectorErrorCategoryV3) -> &'static str {
    match category {
        ConnectorErrorCategoryV3::Authentication => "28000",
        ConnectorErrorCategoryV3::Authorization => "42501",
        ConnectorErrorCategoryV3::InvalidInput => "22023",
        ConnectorErrorCategoryV3::NotFound => "42704",
        ConnectorErrorCategoryV3::Conflict => "40001",
        ConnectorErrorCategoryV3::ResourceLimit => "54000",
        ConnectorErrorCategoryV3::Unsupported => "0A000",
        ConnectorErrorCategoryV3::Cancelled => "57014",
        ConnectorErrorCategoryV3::Timeout | ConnectorErrorCategoryV3::Unavailable => "08006",
        ConnectorErrorCategoryV3::Vendor => "58000",
        ConnectorErrorCategoryV3::Internal => "XX000",
    }
}

fn category_from_sql_state(sql_state: &str) -> ConnectorErrorCategoryV3 {
    match sql_state {
        "28000" | "28P01" => ConnectorErrorCategoryV3::Authentication,
        "42501" => ConnectorErrorCategoryV3::Authorization,
        "40001" | "40P01" | "55P03" => ConnectorErrorCategoryV3::Conflict,
        "0A000" => ConnectorErrorCategoryV3::Unsupported,
        "42704" => ConnectorErrorCategoryV3::NotFound,
        "57014" => ConnectorErrorCategoryV3::Cancelled,
        value if value.starts_with("08") => ConnectorErrorCategoryV3::Unavailable,
        value if value.starts_with("22") || value.starts_with("42") => {
            ConnectorErrorCategoryV3::InvalidInput
        }
        value if value.starts_with("53") || value.starts_with("54") => {
            ConnectorErrorCategoryV3::ResourceLimit
        }
        value if value.starts_with("XX") => ConnectorErrorCategoryV3::Internal,
        _ => ConnectorErrorCategoryV3::Vendor,
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit_error(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn language(
        id: &str,
        input_modes: Vec<ConnectorCommandInputModeV3>,
    ) -> ConnectorCommandLanguageV3 {
        ConnectorCommandLanguageV3 {
            id: id.into(),
            display_name: id.into(),
            input_modes,
        }
    }

    fn capabilities(kind: ConnectorKindV3) -> ConnectorCapabilitiesV3 {
        let command_languages = match kind {
            ConnectorKindV3::Sql => vec![language(
                "postgresql-sql",
                vec![ConnectorCommandInputModeV3::Text],
            )],
            ConnectorKindV3::Document => vec![language(
                "mql",
                vec![
                    ConnectorCommandInputModeV3::Text,
                    ConnectorCommandInputModeV3::Document,
                ],
            )],
            ConnectorKindV3::KeyValue => vec![language(
                "resp3",
                vec![ConnectorCommandInputModeV3::Arguments],
            )],
        };
        ConnectorCapabilitiesV3 {
            kind,
            command_languages,
            catalog: true,
            cancellation: true,
            transactions: true,
            savepoints: true,
            batch_query: true,
            maximum_batch_rows: 1024,
            maximum_catalog_page_size: 256,
            tls_modes: vec![ConnectorTlsModeV2::Disable, ConnectorTlsModeV2::Require],
        }
    }

    #[test]
    fn v3_handshake_and_commands_have_stable_camel_case_json() {
        let ready = ConnectorResponseV3::Ready {
            ready: ProtocolReadyV3 {
                api_version: CONNECTOR_PROTOCOL_V3,
                plugin_id: "mongodb".into(),
                plugin_version: "1.0.0".into(),
                capabilities: capabilities(ConnectorKindV3::Document),
            },
        };
        let json = serde_json::to_value(ready).expect("serialize ready");
        assert_eq!(json["kind"], "ready");
        assert_eq!(json["ready"]["apiVersion"], 3);
        assert_eq!(json["ready"]["capabilities"]["kind"], "document");
        assert_eq!(
            json["ready"]["capabilities"]["commandLanguages"][0]["inputModes"][1],
            "document"
        );

        let commands = [
            ConnectorCommandV3::Text {
                language_id: "postgresql-sql".into(),
                text: "SELECT 1".into(),
                params: Vec::new(),
            },
            ConnectorCommandV3::Document {
                language_id: "mql".into(),
                document: json!({ "find": "items" }),
            },
            ConnectorCommandV3::Arguments {
                language_id: "resp3".into(),
                arguments: vec![ConnectorValueV2::Text("GET".into())],
            },
        ];
        assert_eq!(serde_json::to_value(&commands[0]).unwrap()["kind"], "text");
        assert_eq!(
            serde_json::to_value(&commands[1]).unwrap()["kind"],
            "document"
        );
        assert_eq!(
            serde_json::to_value(&commands[2]).unwrap()["kind"],
            "arguments"
        );

        let catalog = ConnectorResponseV3::CatalogPage {
            request_id: "catalog-1".into(),
            page: ConnectorCatalogPageV3 {
                nodes: Vec::new(),
                next_cursor: Some("page-2".into()),
            },
        };
        let catalog = serde_json::to_value(catalog).expect("catalog JSON");
        assert_eq!(catalog["kind"], "catalogPage");
        assert_eq!(catalog["requestId"], "catalog-1");
        assert_eq!(catalog["page"]["nextCursor"], "page-2");

        let batches = [
            ConnectorResultBatchV3::Rows { rows: Vec::new() },
            ConnectorResultBatchV3::Documents {
                documents: Vec::new(),
            },
            ConnectorResultBatchV3::KeyValues {
                entries: Vec::new(),
            },
        ];
        assert_eq!(serde_json::to_value(&batches[0]).unwrap()["kind"], "rows");
        assert_eq!(
            serde_json::to_value(&batches[1]).unwrap()["kind"],
            "documents"
        );
        assert_eq!(
            serde_json::to_value(&batches[2]).unwrap()["kind"],
            "keyValues"
        );

        let transaction = ConnectorResponseV3::Transaction {
            request_id: "transaction-1".into(),
            state: ConnectorTransactionStateV2::Active,
        };
        assert_eq!(
            serde_json::to_value(transaction).unwrap()["state"],
            "active"
        );
        let cancelled = ConnectorResponseV3::Cancelled {
            request_id: "request-1".into(),
        };
        assert_eq!(
            serde_json::to_value(cancelled).unwrap()["kind"],
            "cancelled"
        );
    }

    #[test]
    fn capabilities_and_commands_fail_closed() {
        let mut sql = capabilities(ConnectorKindV3::Sql);
        validate_capabilities_v3(&sql).expect("valid SQL capabilities");
        sql.command_languages[0].input_modes = vec![ConnectorCommandInputModeV3::Document];
        assert_eq!(
            validate_capabilities_v3(&sql)
                .expect_err("mismatched mode")
                .sql_state,
            "08P01"
        );

        let document = capabilities(ConnectorKindV3::Document);
        let wrong = ConnectorCommandV3::Arguments {
            language_id: "mql".into(),
            arguments: vec![ConnectorValueV2::Text("find".into())],
        };
        assert_eq!(
            validate_command_v3(&wrong, &document)
                .expect_err("unadvertised input mode")
                .sql_state,
            "0A000"
        );
        let scalar_document = ConnectorCommandV3::Document {
            language_id: "mql".into(),
            document: json!(7),
        };
        assert_eq!(
            validate_command_v3(&scalar_document, &document)
                .expect_err("object required")
                .sql_state,
            "22023"
        );

        let mut duplicate = capabilities(ConnectorKindV3::Document);
        duplicate
            .command_languages
            .push(duplicate.command_languages[0].clone());
        assert_eq!(
            validate_capabilities_v3(&duplicate)
                .expect_err("duplicate language")
                .sql_state,
            "08P01"
        );

        let advertised = capabilities(ConnectorKindV3::Document);
        let mut session = advertised.clone();
        session.transactions = false;
        session.savepoints = false;
        session.maximum_batch_rows = 128;
        validate_capability_subset_v3(&advertised, &session).expect("safe session downgrade");
        session.kind = ConnectorKindV3::Sql;
        assert_eq!(
            validate_capability_subset_v3(&advertised, &session)
                .expect_err("kind change")
                .sql_state,
            "08P01"
        );

        let oversized_text = ConnectorCommandV3::Text {
            language_id: "postgresql-sql".into(),
            text: "x".repeat(MAX_CONNECTOR_TEXT_BYTES + 1),
            params: Vec::new(),
        };
        assert_eq!(
            validate_command_v3(&oversized_text, &capabilities(ConnectorKindV3::Sql))
                .expect_err("oversized text")
                .sql_state,
            "22023"
        );

        let oversized_arguments = ConnectorCommandV3::Arguments {
            language_id: "resp3".into(),
            arguments: vec![ConnectorValueV2::Null; MAX_CONNECTOR_COMMAND_ARGUMENTS + 1],
        };
        assert_eq!(
            validate_command_v3(
                &oversized_arguments,
                &capabilities(ConnectorKindV3::KeyValue)
            )
            .expect_err("oversized arguments")
            .sql_state,
            "54000"
        );
    }

    #[test]
    fn catalog_pages_and_result_shapes_are_bounded() {
        let page = ConnectorCatalogPageV3 {
            nodes: vec![ConnectorCatalogNodeV3 {
                id: "db/items".into(),
                parent_id: Some("db".into()),
                kind: ConnectorCatalogNodeKindV3::Collection,
                name: "items".into(),
                namespace: Some("db".into()),
                has_children: true,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            }],
            next_cursor: Some("page-2".into()),
        };
        validate_catalog_page_v3(&page, 10).expect("valid page");
        let mut invalid = page.clone();
        invalid.nodes[0].parent_id = Some(invalid.nodes[0].id.clone());
        assert_eq!(
            validate_catalog_page_v3(&invalid, 10)
                .expect_err("self parent")
                .sql_state,
            "08P01"
        );

        let mut validator = ConnectorResultStreamValidatorV3::new(ConnectorKindV3::Sql, 2);
        validator
            .validate(&ConnectorResultEventV3::Schema {
                columns: Vec::new(),
            })
            .expect("schema");
        validator
            .validate(&ConnectorResultEventV3::Batch {
                batch: ConnectorResultBatchV3::Rows {
                    rows: vec![Vec::new()],
                },
            })
            .expect("rows");
        validator
            .validate(&ConnectorResultEventV3::Complete {
                command_tag: "SELECT".into(),
                affected_items: Some(1),
            })
            .expect("complete");
        assert_eq!(
            validator
                .validate(&ConnectorResultEventV3::Progress { items_processed: 1 })
                .expect_err("post terminal")
                .sql_state,
            "08P01"
        );

        let catalog_capabilities = capabilities(ConnectorKindV3::Document);
        assert_eq!(
            validate_catalog_request_v3(
                None,
                1,
                Some(&"x".repeat(MAX_CONNECTOR_CURSOR_BYTES + 1)),
                &catalog_capabilities,
            )
            .expect_err("oversized cursor")
            .sql_state,
            "08P01"
        );
        assert_eq!(
            validate_catalog_page_v3(&page, 0)
                .expect_err("page exceeds zero limit")
                .sql_state,
            "54000"
        );

        let mut bounded = ConnectorResultStreamValidatorV3::new(ConnectorKindV3::Document, 1);
        assert_eq!(
            bounded
                .validate(&ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Documents {
                        documents: vec![json!({}), json!({})],
                    },
                })
                .expect_err("oversized batch")
                .sql_state,
            "54000"
        );
    }

    #[test]
    fn errors_map_without_forcing_sqlstate_onto_non_sql_connectors() {
        let source = DbError::new("28000", "authentication failed");
        let document = ConnectorErrorV3::from_db_error(&source, ConnectorKindV3::Document);
        assert_eq!(document.sql_state, None);
        validate_error_v3(&document, ConnectorKindV3::Document).expect("document error");
        assert_eq!(document.into_db_error().sql_state, "28000");

        let sql = ConnectorErrorV3::from_db_error(&source, ConnectorKindV3::Sql);
        assert_eq!(sql.sql_state.as_deref(), Some("28000"));
        validate_error_v3(&sql, ConnectorKindV3::Sql).expect("SQL error");
        let sql_json = serde_json::to_value(ConnectorResponseV3::Error {
            request_id: Some("request-1".into()),
            error: sql,
        })
        .expect("error JSON");
        assert_eq!(sql_json["kind"], "error");
        assert_eq!(sql_json["error"]["sqlState"], "28000");

        let unsupported = ConnectorErrorV3::from_db_error(
            &DbError::unsupported("native feature"),
            ConnectorKindV3::Document,
        );
        assert_eq!(unsupported.sql_state, None);
        assert_eq!(unsupported.into_db_error().sql_state, "0A000");
    }

    #[test]
    fn unknown_fields_and_deep_json_are_rejected() {
        let unknown = r#"{"kind":"hello","hello":{"minimumApiVersion":3,"maximumApiVersion":3,"pluginId":"mongodb","pluginVersion":"1.0.0","extra":true}}"#;
        serde_json::from_str::<ConnectorRequestV3>(unknown).expect_err("unknown field");

        let mut value = json!({});
        for _ in 0..=MAX_CONNECTOR_JSON_DEPTH {
            value = json!({ "nested": value });
        }
        let command = ConnectorCommandV3::Document {
            language_id: "mql".into(),
            document: value,
        };
        assert_eq!(
            validate_command_v3(&command, &capabilities(ConnectorKindV3::Document))
                .expect_err("deep JSON")
                .sql_state,
            "54000"
        );
    }

    #[tokio::test]
    async fn v3_frames_round_trip_with_existing_frame_limit() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        let request = ConnectorRequestV3::Cancel {
            request_id: "request-1".into(),
        };
        let write = tokio::spawn(async move {
            write_connector_frame_v3(&mut writer, &request)
                .await
                .expect("write frame");
        });
        let decoded: ConnectorRequestV3 = read_connector_frame_v3(&mut reader)
            .await
            .expect("read frame");
        assert!(
            matches!(decoded, ConnectorRequestV3::Cancel { request_id } if request_id == "request-1")
        );
        write.await.expect("writer task");
        assert_eq!(crate::MAX_CONNECTOR_FRAME_BYTES, 8 * 1024 * 1024);
    }
}
