
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn postgres_epoch_date() -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(
        POSTGRES_EPOCH_DATE.0,
        POSTGRES_EPOCH_DATE.1,
        POSTGRES_EPOCH_DATE.2,
    )
    .ok_or_else(|| DbError::new("XX000", "PostgreSQL epoch date is invalid"))
}

fn postgres_epoch_timestamp() -> Result<NaiveDateTime> {
    postgres_epoch_date()?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| DbError::new("XX000", "PostgreSQL epoch timestamp is invalid"))
}

#[cfg(test)]
mod tests {
    use ordadb_types::Field;

    use super::*;

    #[test]
    fn text_and_binary_round_trip_supported_scalar_parameters() {
        assert_eq!(decode_text(OID_INT8, b"42").expect("int"), Value::Int64(42));
        assert_eq!(
            decode_binary(OID_INT4, &42_i32.to_be_bytes()).expect("int"),
            Value::Int32(42)
        );
        let date = NaiveDate::from_ymd_opt(2026, 7, 25).expect("date");
        let binary = encode_binary(&Value::Date(date), &ScalarType::Date).expect("encode");
        assert_eq!(
            decode_binary(OID_DATE, &binary).expect("decode"),
            Value::Date(date)
        );
    }

    #[test]
    fn postgres_catalog_wire_types_preserve_oids_widths_and_bounds() {
        let schema = Schema::new(vec![
            Field::new("schema_oid", ScalarType::Oid, false),
            Field::new("schema_name", ScalarType::Name, false),
            Field::new("owner_name", ScalarType::Name, true),
            Field::new("relation_oid", ScalarType::Oid, false),
            Field::new("relkind", ScalarType::InternalChar, false),
        ]);
        let mut description = Vec::new();
        write_row_description(&mut description, &schema, &[]).expect("row description");
        assert_eq!(
            row_description_type_metadata(&description),
            vec![
                ("schema_oid".to_owned(), OID_OID, 4),
                ("schema_name".to_owned(), OID_NAME, 64),
                ("owner_name".to_owned(), OID_NAME, 64),
                ("relation_oid".to_owned(), OID_OID, 4),
                ("relkind".to_owned(), OID_INTERNAL_CHAR, 1),
            ]
        );

        let maximum_oid = Value::Int64(i64::from(u32::MAX));
        assert_eq!(
            decode_text(OID_OID, u32::MAX.to_string().as_bytes()).expect("maximum text OID"),
            maximum_oid
        );
        let encoded_oid = encode_binary(&maximum_oid, &ScalarType::Oid).expect("binary OID");
        assert_eq!(encoded_oid, u32::MAX.to_be_bytes());
        assert_eq!(
            decode_binary(OID_OID, &encoded_oid).expect("decode binary OID"),
            maximum_oid
        );
        assert_eq!(
            decode_text(OID_OID, b"4294967296")
                .expect_err("out-of-range text OID")
                .sql_state,
            "22003"
        );
        assert_eq!(
            encode_binary(&Value::Int64(-1), &ScalarType::Oid)
                .expect_err("negative result OID")
                .sql_state,
            "22003"
        );

        let maximum_name = "n".repeat(MAX_POSTGRES_NAME_BYTES);
        assert_eq!(
            decode_text(OID_NAME, maximum_name.as_bytes()).expect("maximum name"),
            Value::Text(maximum_name)
        );
        let oversized_name = "n".repeat(MAX_POSTGRES_NAME_BYTES + 1);
        assert_eq!(
            decode_binary(OID_NAME, oversized_name.as_bytes())
                .expect_err("oversized binary name")
                .sql_state,
            "22001"
        );
        assert_eq!(
            write_data_row(
                &mut Vec::new(),
                &Schema::new(vec![Field::new("name", ScalarType::Name, false)]),
                &Row::new(vec![Value::Text(oversized_name)]),
                &[0],
            )
            .expect_err("oversized text name result")
            .sql_state,
            "22001"
        );

        assert_eq!(
            decode_text(OID_INTERNAL_CHAR, b"r").expect("internal char"),
            Value::Text("r".into())
        );
        assert_eq!(
            decode_text(OID_INTERNAL_CHAR, "é".as_bytes())
                .expect_err("multibyte internal char")
                .sql_state,
            "22P02"
        );
        assert_eq!(
            decode_binary(OID_INTERNAL_CHAR, b"rr")
                .expect_err("wide binary internal char")
                .sql_state,
            "08P01"
        );
    }

    fn row_description_type_metadata(bytes: &[u8]) -> Vec<(String, u32, i16)> {
        assert_eq!(bytes.first(), Some(&b'T'));
        let message_len = u32::from_be_bytes(bytes[1..5].try_into().expect("message length"));
        assert_eq!(
            usize::try_from(message_len).expect("bounded length") + 1,
            bytes.len()
        );
        let mut offset = 5;
        let field_count = u16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("count"));
        offset += 2;
        let mut fields = Vec::with_capacity(usize::from(field_count));
        for _ in 0..field_count {
            let name_end = bytes[offset..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|position| offset + position)
                .expect("field terminator");
            let name = std::str::from_utf8(&bytes[offset..name_end])
                .expect("field UTF-8")
                .to_owned();
            offset = name_end + 1 + 4 + 2;
            let oid = u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("type OID"));
            offset += 4;
            let size = i16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("type size"));
            offset += 2 + 4 + 2;
            fields.push((name, oid, size));
        }
        assert_eq!(offset, bytes.len());
        fields
    }

    #[test]
    fn interval_timestamptz_and_array_formats_round_trip() {
        for numeric in [
            Decimal::new(0, 0),
            Decimal::new(1_234_567, 2),
            Decimal::new(-12, 4),
            Decimal::new(120_000, 4),
        ] {
            let binary = encode_binary(
                &Value::Decimal(numeric),
                &ScalarType::Decimal {
                    precision: None,
                    scale: None,
                },
            )
            .expect("numeric binary");
            assert_eq!(
                decode_binary(OID_NUMERIC, &binary).expect("numeric binary"),
                Value::Decimal(numeric)
            );
        }

        let interval = PgInterval::new(14, 3, 14_706_750_000);
        assert_eq!(
            decode_text(OID_INTERVAL, b"1 year 2 mons 3 days 04:05:06.75").expect("interval text"),
            Value::Interval(interval)
        );
        let binary = encode_binary(&Value::Interval(interval), &ScalarType::Interval)
            .expect("interval binary");
        assert_eq!(
            decode_binary(OID_INTERVAL, &binary).expect("interval binary"),
            Value::Interval(interval)
        );

        let timestamp =
            decode_text(OID_TIMESTAMPTZ, b"2026-08-03 12:30:00+08:00").expect("timestamptz");
        assert_eq!(
            timestamp,
            Value::Timestamp(
                NaiveDateTime::parse_from_str("2026-08-03 04:30:00", "%Y-%m-%d %H:%M:%S")
                    .expect("timestamp")
            )
        );
        let binary = encode_binary(
            &timestamp,
            &ScalarType::Timestamp {
                with_timezone: true,
            },
        )
        .expect("timestamptz binary");
        assert_eq!(
            decode_binary(OID_TIMESTAMPTZ, &binary).expect("timestamptz binary"),
            timestamp
        );

        let array = decode_text(OID_TEXT_ARRAY, br#"[-1:0][2:3]={{"a,b",NULL},{x,"NULL"}}"#)
            .expect("array text");
        let Value::Array(array) = array else {
            panic!("array expected");
        };
        assert_eq!(
            array.dimensions(),
            &[ArrayDimension::new(2, -1), ArrayDimension::new(2, 2)]
        );
        assert_eq!(
            array.values(),
            &[
                Value::Text("a,b".into()),
                Value::Null,
                Value::Text("x".into()),
                Value::Text("NULL".into())
            ]
        );
        assert_eq!(
            String::from_utf8(encode_text(&Value::Array(array)).expect("array encode"))
                .expect("utf8"),
            r#"[-1:0][2:3]={{"a,b",NULL},{x,"NULL"}}"#
        );

        let array = PgArray::one_dimensional(
            ScalarType::Int32,
            vec![Value::Int32(1), Value::Null, Value::Int32(3)],
        )
        .expect("array");
        let data_type = ScalarType::Array {
            element: Box::new(ScalarType::Int32),
        };
        assert_eq!(type_oid(&data_type), OID_INT4_ARRAY);
        let binary = encode_binary(&Value::Array(array.clone()), &data_type).expect("array binary");
        assert_eq!(
            decode_binary(OID_INT4_ARRAY, &binary).expect("array binary"),
            Value::Array(array)
        );
    }

    #[test]
    fn enum_oids_and_text_binary_values_are_stable_and_validated() {
        let enum_type = ScalarType::Enum {
            type_id: TypeId::new(7),
            labels: vec!["queued".into(), "running".into(), "done".into()],
        };
        let scalar_oid = OID_USER_DEFINED_ENUM_BASE + 12;
        let array_oid = scalar_oid + 1;
        assert_eq!(enum_type_oid(TypeId::new(7)), scalar_oid);
        assert_eq!(enum_array_oid(TypeId::new(7)), array_oid);
        assert_eq!(type_oid(&enum_type), scalar_oid);

        for format in [0, 1] {
            assert_eq!(
                decode_parameters_as(
                    &[scalar_oid],
                    std::slice::from_ref(&enum_type),
                    &[format],
                    &[Some(b"running".to_vec())],
                )
                .expect("enum parameter"),
                [Value::Text("running".into())]
            );
        }
        assert_eq!(
            encode_binary(&Value::Text("done".into()), &enum_type).expect("enum binary"),
            b"done"
        );

        let array_type = ScalarType::Array {
            element: Box::new(enum_type.clone()),
        };
        assert_eq!(type_oid(&array_type), array_oid);
        let array = PgArray::one_dimensional(
            enum_type.clone(),
            vec![
                Value::Text("queued".into()),
                Value::Null,
                Value::Text("done".into()),
            ],
        )
        .expect("enum array");
        let expected = Value::Array(array.clone());
        assert_eq!(
            decode_parameters_as(
                &[array_oid],
                std::slice::from_ref(&array_type),
                &[0],
                &[Some(b"{queued,NULL,done}".to_vec())],
            )
            .expect("enum array text"),
            std::slice::from_ref(&expected)
        );
        let binary = encode_binary(&expected, &array_type).expect("enum array binary");
        assert_eq!(
            decode_parameters_as(
                &[array_oid],
                std::slice::from_ref(&array_type),
                &[1],
                &[Some(binary)],
            )
            .expect("enum array binary"),
            [expected]
        );

        let invalid = decode_parameters_as(
            &[scalar_oid],
            std::slice::from_ref(&enum_type),
            &[0],
            &[Some(b"blocked".to_vec())],
        )
        .expect_err("invalid enum label");
        assert_eq!(invalid.sql_state, "22P02");
        let invalid_array = decode_parameters_as(
            &[array_oid],
            std::slice::from_ref(&array_type),
            &[0],
            &[Some(b"{queued,blocked}".to_vec())],
        )
        .expect_err("invalid enum array label");
        assert_eq!(invalid_array.sql_state, "22P02");
    }

    #[test]
    fn row_description_and_data_row_honor_result_formats() {
        let schema = Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new("title", ScalarType::Text, false),
        ]);
        let mut bytes = Vec::new();
        write_row_description(&mut bytes, &schema, &[1, 0]).expect("description");
        write_data_row(
            &mut bytes,
            &schema,
            &Row::new(vec![Value::Int64(42), Value::Text("hello".into())]),
            &[1, 0],
        )
        .expect("row");
        assert!(bytes.starts_with(b"T"));
        assert!(bytes.windows(5).any(|window| window == b"hello"));
    }

    #[test]
    fn malformed_binary_numeric_is_rejected() {
        let error = decode_binary(OID_NUMERIC, &[0, 0]).expect_err("malformed numeric");
        assert_eq!(error.sql_state, "08P01");
    }
}
