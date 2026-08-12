
#[test]
fn merge_rejects_unrepresented_upstream_fields() {
    let catalog = catalog_with_documents();
    let derived = parse(
        "MERGE INTO documents AS d \
         USING (SELECT id, title FROM documents) AS u ON d.id = u.id \
         WHEN MATCHED THEN DELETE",
    )
    .expect_err("derived MERGE source");
    assert_eq!(derived.sql_state, FEATURE_NOT_SUPPORTED);

    let by_source = bind(
        parse(
            "MERGE INTO documents AS d USING documents AS u ON d.id = u.id \
         WHEN NOT MATCHED BY SOURCE AND u.title = 'missing' THEN DELETE",
        )
        .expect("parse BY SOURCE"),
        &catalog,
    )
    .expect_err("BY SOURCE source reference");
    assert_eq!(by_source.sql_state, UNDEFINED_TABLE);

    let missing_into = parse(
        "MERGE documents AS d USING documents AS u ON d.id = u.id \
         WHEN MATCHED THEN DELETE",
    )
    .expect_err("missing INTO");
    assert_eq!(missing_into.sql_state, SYNTAX_ERROR);

    let error = bind(
        parse(
            "MERGE INTO documents AS d USING documents AS u ON d.id = u.id \
             WHEN MATCHED THEN UPDATE SET missing = u.title",
        )
        .expect("parse missing target column"),
        &catalog,
    )
    .expect_err("missing target column");
    assert_eq!(error.sql_state, UNDEFINED_COLUMN);
}

#[test]
fn rejects_invalid_or_vendor_conflict_actions() {
    let catalog = catalog_with_documents();

    let error = bind(
        parse("INSERT INTO documents VALUES (1, 'one') ON CONFLICT (title) DO NOTHING")
            .expect("parse non-unique conflict target"),
        &catalog,
    )
    .expect_err("non-unique target");
    assert_eq!(error.sql_state, "42P10");

    let error = bind(
        parse(
            "INSERT INTO documents VALUES (1, 'one') \
             ON CONFLICT DO UPDATE SET title = excluded.title",
        )
        .expect("parse targetless conflict update"),
        &catalog,
    )
    .expect_err("targetless update");
    assert_eq!(error.sql_state, SYNTAX_ERROR);

    let error = parse_with_dialect(
        "INSERT INTO documents VALUES (1, 'one') \
         ON DUPLICATE KEY UPDATE title = 'changed'",
        SqlDialect::MySql,
    )
    .expect_err("vendor conflict action");
    assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);
}

#[test]
fn reports_unsupported_vendor_features_with_the_selected_dialect() {
    let error = parse_with_dialect(
        "INSERT IGNORE INTO documents (id, title) VALUES (1, 'ignored')",
        SqlDialect::MySql,
    )
    .expect_err("insert ignore");
    assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);
    assert!(error.message.contains("MySQL"), "{error:?}");
    assert!(error.hint.is_some());

    let error = parse_with_dialect(
        "SELECT TOP 10 PERCENT [id] FROM [documents]",
        SqlDialect::SqlServer,
    )
    .expect_err("top percent");
    assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);
    assert!(error.message.contains("SQL Server"), "{error:?}");
}

#[test]
fn parses_and_binds_enum_domain_and_named_column_types() {
    let enum_statement =
        parse("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')").expect("parse enum");
    assert!(matches!(
        enum_statement,
        ParsedStatement::CreateEnumType { ref labels, .. }
            if labels == &["sad", "ok", "happy"]
    ));

    let domain_statement = parse(
        "CREATE DOMAIN positive_int AS integer DEFAULT 1 NOT NULL \
         CONSTRAINT positive CHECK (VALUE > 0)",
    )
    .expect("parse domain");
    assert!(matches!(
        bind(domain_statement, &Catalog::default()).expect("bind domain"),
        BoundStatement::CreateDomain { not_null: true, ref checks, .. }
            if checks.len() == 1
                && checks[0].name.as_ref().is_some_and(|name| name.as_str() == "positive")
    ));

    let mut catalog = catalog_with_documents();
    let type_id = catalog
        .create_enum_type(
            &Identifier::unquoted("public"),
            Identifier::unquoted("mood"),
            vec!["sad".into(), "ok".into(), "happy".into()],
        )
        .expect("catalog enum");
    let domain_id = catalog
        .create_domain(
            &Identifier::unquoted("public"),
            Identifier::unquoted("positive_int"),
            ScalarType::Int32,
            true,
            None,
            Vec::new(),
        )
        .expect("catalog domain");
    let add_value = bind(
        parse("ALTER TYPE mood ADD VALUE IF NOT EXISTS 'calm' BEFORE 'happy'")
            .expect("parse enum add value"),
        &catalog,
    )
    .expect("bind enum add value");
    assert!(matches!(
        add_value,
        BoundStatement::AlterEnumAddValue {
            type_id: altered_type_id,
            ref label,
            position: Some(EnumValuePosition::Before(ref neighbor)),
            if_not_exists: true,
        } if altered_type_id == type_id && label == "calm" && neighbor == "happy"
    ));
    assert!(matches!(
        bind(
            parse("ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'")
                .expect("parse enum rename value"),
            &catalog,
        )
        .expect("bind enum rename value"),
        BoundStatement::AlterEnumRenameValue {
            type_id: altered_type_id,
            ref old_label,
            ref new_label,
        } if altered_type_id == type_id && old_label == "ok" && new_label == "fine"
    ));
    assert!(matches!(
        bind(
            parse("ALTER DOMAIN positive_int SET DEFAULT 2")
                .expect("parse domain default"),
            &catalog,
        )
        .expect("bind domain default"),
        BoundStatement::AlterDomain {
            type_id: altered_type_id,
            operation: BoundAlterDomainOperation::SetDefault(ref default),
        } if altered_type_id == domain_id && default.sql == "2"
    ));
    assert!(matches!(
        bind(
            parse(
                "ALTER DOMAIN positive_int ADD CONSTRAINT below_limit CHECK (VALUE < 100)",
            )
            .expect("parse domain constraint"),
            &catalog,
        )
        .expect("bind domain constraint"),
        BoundStatement::AlterDomain {
            type_id: altered_type_id,
            operation: BoundAlterDomainOperation::AddConstraint(ref constraint),
        } if altered_type_id == domain_id
            && constraint.name.as_ref().is_some_and(|name| name.as_str() == "below_limit")
    ));
    assert_eq!(
        bind(
            parse("ALTER DOMAIN mood SET NOT NULL").expect("parse wrong domain kind"),
            &catalog,
        )
        .expect_err("enum is not a domain")
        .sql_state,
        "42809"
    );
    let bound = bind(
        parse("CREATE TABLE feelings (current_mood mood NOT NULL)").expect("parse table"),
        &catalog,
    )
    .expect("bind table");
    assert!(matches!(
        bound,
        BoundStatement::CreateTable { ref columns, .. }
            if columns[0].declared_type == Some(type_id)
                && columns[0].data_type == ScalarType::Enum {
                    type_id,
                    labels: vec!["sad".into(), "ok".into(), "happy".into()],
                }
    ));

    let cast = bind(
        parse(
            "SELECT $1::mood, 'sad'::mood, $2::positive_int, \
             ARRAY['sad', 'happy']::mood[] FROM documents",
        )
        .expect("parse named casts"),
        &catalog,
    )
    .expect("bind named casts");
    let BoundStatement::Select { projection, .. } = cast else {
        panic!("expected named cast select");
    };
    let enum_type = ScalarType::Enum {
        type_id,
        labels: vec!["sad".into(), "ok".into(), "happy".into()],
    };
    assert_eq!(projection[0].expr.data_type, enum_type);
    assert_eq!(projection[1].expr.data_type, enum_type);
    assert_eq!(projection[2].expr.data_type, ScalarType::Int32);
    assert_eq!(
        projection[3].expr.data_type,
        ScalarType::Array {
            element: Box::new(enum_type),
        }
    );

    let function = bind(
        parse(
            "CREATE FUNCTION echo_mood(value mood) RETURNS mood \
             LANGUAGE plpgsql AS $$ BEGIN RETURN value; END $$",
        )
        .expect("parse named type function"),
        &catalog,
    )
    .expect("bind named type function");
    assert!(matches!(
        function,
        BoundStatement::CreateRoutine {
            ref arguments,
            return_declared_type: Some(return_type_id),
            ..
        } if arguments[0].declared_type == Some(type_id) && return_type_id == type_id
    ));

    let alter = bind(
        parse("ALTER TABLE documents ALTER COLUMN title TYPE mood")
            .expect("parse named type alter"),
        &catalog,
    )
    .expect("bind named type alter");
    assert!(matches!(
        alter,
        BoundStatement::AlterTable { ref operations, .. }
            if matches!(
                operations.as_slice(),
                [BoundAlterTableOperation::SetDataType {
                    declared_type: Some(alter_type_id),
                    ..
                }] if *alter_type_id == type_id
            )
    ));

    let error = bind(
        parse("SELECT 'value'::missing_type FROM documents").expect("parse missing named cast"),
        &catalog,
    )
    .expect_err("undefined named cast type");
    assert_eq!(error.sql_state, "42704");

    let drop = bind(parse("DROP TYPE mood").expect("parse drop"), &catalog).expect("bind drop");
    assert!(matches!(
        drop,
        BoundStatement::DropObjects {
            kind: DdlObjectKind::Type,
            ..
        }
    ));

    let error = bind(
        parse("CREATE TABLE missing_type (value unknown_named_type)")
            .expect("parse unknown type"),
        &catalog,
    )
    .expect_err("undefined named type");
    assert_eq!(error.sql_state, "42704");

    let error = bind(
        parse("CREATE DOMAIN bad_check AS integer CHECK (missing > 0)")
            .expect("parse invalid domain check"),
        &catalog,
    )
    .expect_err("invalid domain check");
    assert_eq!(error.sql_state, UNDEFINED_COLUMN);

    assert!(!create_domain_is_not_null(
        "CREATE DOMAIN nullable_flag AS boolean DEFAULT NULL IS NOT NULL"
    ));

    for sql in [
        "CREATE TYPE shell_type",
        "CREATE TYPE inventory_item AS (name text)",
    ] {
        let error = parse(sql).expect_err("unsupported type definition");
        assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED, "{sql}");
    }
}

#[test]
fn binds_named_domain_bases_catalog_expressions_and_routine_identity() {
    use ordadb_catalog::{DomainBaseType, NewRoutine};

    let mut catalog = Catalog::default();
    let mood_id = catalog
        .create_enum_type(
            &Identifier::unquoted("public"),
            Identifier::unquoted("mood"),
            vec!["sad".into(), "ok".into(), "happy".into()],
        )
        .expect("create mood");
    let mood_type = catalog.type_by_id(mood_id).expect("mood").logical_type();
    let mood_domain = bind(
        parse(
            "CREATE DOMAIN cheerful_mood AS mood DEFAULT 'ok'::mood \
             CHECK (VALUE <> 'sad'::mood)",
        )
        .expect("parse enum domain"),
        &catalog,
    )
    .expect("bind enum domain");
    assert!(matches!(
        mood_domain,
        BoundStatement::CreateDomain {
            base_declared_type: Some(type_id),
            ref base_type,
            ..
        } if type_id == mood_id && base_type == &mood_type
    ));
    let cheerful_id = catalog
        .create_domain_with_declared_type(
            &Identifier::unquoted("public"),
            Identifier::unquoted("cheerful_mood"),
            DomainBaseType::new(mood_type, Some(mood_id)),
            false,
            Some(CatalogExpression::new("'ok'::mood")),
            Vec::new(),
        )
        .expect("create enum domain");
    assert!(matches!(
        bind(
            parse("ALTER DOMAIN cheerful_mood SET DEFAULT 'happy'::mood")
                .expect("parse named domain default"),
            &catalog,
        )
        .expect("bind named domain default"),
        BoundStatement::AlterDomain {
            type_id,
            operation: BoundAlterDomainOperation::SetDefault(ref default),
        } if type_id == cheerful_id && default.sql == "'happy' :: mood"
    ));
    assert_eq!(
        bind(
            parse("CREATE DOMAIN nested_mood AS cheerful_mood").expect("parse nested domain"),
            &catalog,
        )
        .expect_err("nested domain base is explicit unsupported")
        .sql_state,
        FEATURE_NOT_SUPPORTED
    );

    let positive_id = catalog
        .create_domain(
            &Identifier::unquoted("public"),
            Identifier::unquoted("positive_int"),
            ScalarType::Int32,
            false,
            None,
            Vec::new(),
        )
        .expect("positive domain");
    let nonnegative_id = catalog
        .create_domain(
            &Identifier::unquoted("public"),
            Identifier::unquoted("nonnegative_int"),
            ScalarType::Int32,
            false,
            None,
            Vec::new(),
        )
        .expect("nonnegative domain");
    let create_routine = |name: &str, type_id: TypeId| NewRoutine {
        name: Identifier::unquoted(name),
        kind: RoutineKind::Function,
        arguments: vec![RoutineArgument {
            name: Some(Identifier::unquoted("value")),
            data_type: ScalarType::Int32,
            declared_type: Some(type_id),
            mode: RoutineArgumentMode::In,
        }],
        return_type: Some(ScalarType::Int32),
        return_declared_type: None,
        returns_set: false,
        language: "plpgsql".into(),
        body: "BEGIN RETURN value; END".into(),
        replace: false,
        references: vec![CatalogObjectRef::Type(type_id)],
    };
    let positive_routine = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            create_routine("choose_value", positive_id),
        )
        .expect("positive overload");
    catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            create_routine("choose_value", nonnegative_id),
        )
        .expect("nonnegative overload");

    assert!(matches!(
        bind(
            parse("SELECT choose_value(1::positive_int)")
                .expect("parse exact overload"),
            &catalog,
        )
        .expect("bind exact overload"),
        BoundStatement::RoutineSelect { routine_id, .. }
            if routine_id == positive_routine
    ));
    assert_eq!(
        bind(
            parse("SELECT choose_value(1)").expect("parse ambiguous overload"),
            &catalog,
        )
        .expect_err("same-base domains remain ambiguous without an exact declared type")
        .sql_state,
        "42725"
    );
    assert!(matches!(
        bind(
            parse("DROP FUNCTION choose_value(positive_int)")
                .expect("parse named drop signature"),
            &catalog,
        )
        .expect("bind named drop signature"),
        BoundStatement::DropRoutine { routine_id, .. }
            if routine_id == positive_routine
    ));
}

fn parameter_indices(expression: &ParsedExpr) -> Vec<usize> {
    let mut parameters = Vec::new();
    let mut stack = vec![expression];
    while let Some(expression) = stack.pop() {
        match &expression.kind {
            ParsedExprKind::Parameter(index)
            | ParsedExprKind::ResolvedParameter { index, .. } => parameters.push(*index),
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                stack.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => stack.extend(elements),
            ParsedExprKind::Function { arguments, .. } => stack.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                stack.extend(list);
                stack.push(expr);
            }
            ParsedExprKind::InSubquery { expr, .. } => stack.push(expr),
            ParsedExprKind::QuantifiedSubquery { left, .. } => stack.push(left),
            ParsedExprKind::RowSubquery { left, .. } => stack.extend(left),
            ParsedExprKind::ScalarSubquery(_) | ParsedExprKind::Exists { .. } => {}
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    stack.push(filter);
                }
                if let Some(argument) = argument {
                    stack.push(argument);
                }
            }
            ParsedExprKind::Window { call, spec } => {
                if let Some(filter) = &call.filter {
                    stack.push(filter);
                }
                stack.extend(&call.arguments);
                stack.extend(spec.order_by.iter().map(|order| &order.expr));
                stack.extend(&spec.partition_by);
            }
            ParsedExprKind::NamedWindow { call, .. } => {
                if let Some(filter) = &call.filter {
                    stack.push(filter);
                }
                stack.extend(&call.arguments);
            }
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    parameters
}
