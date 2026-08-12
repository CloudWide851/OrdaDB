
#[test]
fn user_defined_types_round_trip_and_track_column_dependencies() {
    let mut catalog = Catalog::default();
    for (name, labels, sql_state) in [
        ("empty_enum", Vec::new(), "42601"),
        ("empty_label", vec![String::new()], "42601"),
        (
            "duplicate_label",
            vec!["same".to_owned(), "same".to_owned()],
            "42710",
        ),
        ("long_label", vec!["界".repeat(22)], "42622"),
    ] {
        let error = catalog
            .create_enum_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted(name),
                labels,
            )
            .expect_err("invalid enum labels");
        assert_eq!(error.sql_state, sql_state);
    }
    let enum_id = catalog
        .create_enum_type(
            &Identifier::unquoted("public"),
            Identifier::unquoted("mood"),
            vec!["sad".into(), "ok".into(), "happy".into()],
        )
        .expect("enum");
    let domain_id = catalog
        .create_domain(
            &Identifier::unquoted("public"),
            Identifier::unquoted("positive_int"),
            ScalarType::Int32,
            true,
            Some(CatalogExpression::new("1")),
            vec![DomainConstraint {
                id: None,
                name: Some(Identifier::unquoted("positive")),
                expression: CatalogExpression::new("VALUE > 0"),
            }],
        )
        .expect("domain");
    let mut mood = NewColumn::new(Identifier::unquoted("mood"), ScalarType::Text);
    mood.declared_type = Some(enum_id);
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("feelings"),
            vec![mood],
        )
        .expect("table");
    let enum_data_type = catalog
        .type_by_id(enum_id)
        .expect("enum type")
        .logical_type();
    let enum_domain_id = catalog
        .create_domain_with_declared_type(
            &Identifier::unquoted("public"),
            Identifier::unquoted("cheerful_mood"),
            DomainBaseType::new(enum_data_type.clone(), Some(enum_id)),
            false,
            Some(CatalogExpression::new("'ok'::mood")),
            Vec::new(),
        )
        .expect("enum domain");
    assert_eq!(
        catalog
            .dependencies()
            .references(CatalogObjectRef::Type(enum_domain_id))
            .collect::<Vec<_>>(),
        vec![CatalogObjectRef::Type(enum_id)]
    );
    let mut cheerful = NewColumn::new(Identifier::unquoted("cheerful"), enum_data_type.clone());
    cheerful.declared_type = Some(enum_domain_id);
    let enum_domain_table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("cheerful_feelings"),
            vec![cheerful],
        )
        .expect("enum domain table");
    let routine_id = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            NewRoutine {
                name: Identifier::unquoted("echo_mood"),
                kind: RoutineKind::Function,
                arguments: vec![RoutineArgument {
                    name: Some(Identifier::unquoted("value")),
                    data_type: enum_data_type,
                    declared_type: Some(enum_id),
                    mode: Default::default(),
                }],
                return_type: Some(ScalarType::Int32),
                return_declared_type: Some(domain_id),
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN 1; END".into(),
                replace: false,
                references: vec![
                    CatalogObjectRef::Type(enum_id),
                    CatalogObjectRef::Type(domain_id),
                ],
            },
        )
        .expect("routine");

    assert!(
        catalog
            .alter_enum_add_value(
                enum_id,
                "calm".into(),
                Some(EnumValuePosition::Before("happy".into())),
                false,
            )
            .expect("add enum label")
    );
    assert!(
        !catalog
            .alter_enum_add_value(enum_id, "calm".into(), None, true)
            .expect("duplicate enum label is a no-op")
    );
    catalog
        .alter_enum_rename_value(enum_id, "ok", "fine".into())
        .expect("rename enum label");
    let expected_enum = ScalarType::Enum {
        type_id: enum_id,
        labels: vec!["sad".into(), "fine".into(), "calm".into(), "happy".into()],
    };
    assert_eq!(
        catalog.table_by_id(table_id).expect("table").columns()[0].data_type,
        expected_enum
    );
    assert_eq!(
        catalog
            .routine_by_id(routine_id)
            .expect("routine")
            .arguments[0]
            .data_type,
        expected_enum
    );
    assert!(matches!(
        &catalog
            .type_by_id(enum_domain_id)
            .expect("enum domain")
            .definition,
        UserDefinedTypeKind::Domain {
            base_type,
            base_declared_type: Some(base_type_id),
            ..
        } if base_type == &expected_enum && *base_type_id == enum_id
    ));
    assert_eq!(
        catalog
            .table_by_id(enum_domain_table_id)
            .expect("enum domain table")
            .columns()[0]
            .data_type,
        expected_enum
    );
    catalog
        .alter_domain_default(domain_id, Some(CatalogExpression::new("2")))
        .expect("alter domain default");
    catalog
        .alter_domain_not_null(domain_id, false)
        .expect("drop domain not null");
    catalog
        .add_domain_constraint(
            domain_id,
            DomainConstraint {
                id: None,
                name: Some(Identifier::unquoted("below_limit")),
                expression: CatalogExpression::new("VALUE < 100"),
            },
        )
        .expect("add domain constraint");
    assert!(
        catalog
            .drop_domain_constraint(domain_id, &Identifier::unquoted("positive"), false,)
            .expect("drop domain constraint")
    );

    let error = catalog
        .drop_type(enum_id, DropBehavior::Restrict)
        .expect_err("column dependency");
    assert_eq!(error.sql_state, "2BP01");
    let error = catalog
        .drop_type(domain_id, DropBehavior::Restrict)
        .expect_err("routine return dependency");
    assert_eq!(error.sql_state, "2BP01");
    assert!(matches!(
        &catalog.type_by_id(domain_id).expect("domain").definition,
        UserDefinedTypeKind::Domain {
            not_null: false,
            default: Some(default),
            checks,
            ..
        } if default.sql == "2"
            && checks.len() == 1
            && checks[0].name.as_ref().is_some_and(|name| name.as_str() == "below_limit")
    ));

    let encoded = serde_json::to_vec(&catalog).expect("serialize");
    let decoded: Catalog = serde_json::from_slice(&encoded).expect("deserialize");
    assert_eq!(decoded, catalog);
    assert_eq!(
        decoded.table_by_id(table_id).expect("table").columns()[0].declared_type,
        Some(enum_id)
    );
    let routine = decoded.routine_by_id(routine_id).expect("routine");
    assert_eq!(routine.arguments[0].declared_type, Some(enum_id));
    assert_eq!(routine.return_declared_type, Some(domain_id));
}

#[test]
fn assigns_unique_postgres_oids_to_every_catalog_object_kind() {
    let fixture = oid_fixture();
    let catalog = &fixture.catalog;
    let objects = [
        PostgresOidObject::Database(catalog.database().id),
        PostgresOidObject::Schema(fixture.schema_id),
        PostgresOidObject::Table(fixture.table_id),
        PostgresOidObject::Column(fixture.table_id, fixture.column_id),
        PostgresOidObject::Index(fixture.index_id),
        PostgresOidObject::Constraint(fixture.constraint_id),
        PostgresOidObject::Sequence(fixture.sequence_id),
        PostgresOidObject::View(fixture.view_id),
        PostgresOidObject::View(fixture.materialized_view_id),
        PostgresOidObject::Routine(fixture.routine_id),
        PostgresOidObject::Trigger(fixture.trigger_id),
        PostgresOidObject::Type(fixture.type_id),
    ];
    let mut oids = BTreeSet::new();
    for object in objects {
        let oid = catalog.postgres_oid(object).expect("object OID");
        assert!(oid.get() >= POSTGRES_OID_FIRST_USER);
        assert!(oids.insert(oid));
        assert_eq!(catalog.postgres_oid_object(oid), Some(object));
    }
    catalog
        .validate_postgres_oid_registry()
        .expect("valid registry");
}

#[test]
fn postgres_oids_survive_renames_replacements_and_alterations() {
    let mut fixture = oid_fixture();
    let before = fixture
        .catalog
        .postgres_oid_registry()
        .mappings()
        .collect::<BTreeMap<_, _>>();
    fixture
        .catalog
        .rename_schema(fixture.schema_id, Identifier::unquoted("renamed"))
        .expect("rename schema");
    fixture
        .catalog
        .rename_table(fixture.table_id, Identifier::unquoted("renamed_items"))
        .expect("rename table");
    fixture
        .catalog
        .rename_column(
            fixture.table_id,
            fixture.column_id,
            Identifier::unquoted("renamed_label"),
        )
        .expect("rename column");
    fixture
        .catalog
        .rename_index(fixture.index_id, Identifier::unquoted("renamed_items_idx"))
        .expect("rename index");
    fixture
        .catalog
        .rename_sequence(
            fixture.sequence_id,
            Identifier::unquoted("renamed_items_seq"),
        )
        .expect("rename sequence");
    fixture
        .catalog
        .rename_view(fixture.view_id, Identifier::unquoted("renamed_item_view"))
        .expect("rename view");
    fixture
        .catalog
        .replace_view(
            fixture.view_id,
            "SELECT id FROM renamed_items".into(),
            Schema::empty(),
            true,
            [CatalogObjectRef::Table(fixture.table_id)],
        )
        .expect("replace view");
    let replaced_routine = fixture
        .catalog
        .create_or_replace_routine(
            &Identifier::unquoted("renamed"),
            NewRoutine {
                name: Identifier::unquoted("touch_item"),
                kind: RoutineKind::Function,
                arguments: Vec::new(),
                return_type: None,
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN NULL; RETURN; END".into(),
                replace: true,
                references: vec![CatalogObjectRef::View(fixture.view_id)],
            },
        )
        .expect("replace routine");
    assert_eq!(replaced_routine, fixture.routine_id);
    fixture
        .catalog
        .set_trigger_enabled(fixture.trigger_id, false)
        .expect("alter trigger");
    assert!(
        fixture
            .catalog
            .alter_enum_add_value(fixture.type_id, "archived".into(), None, false)
            .expect("alter type")
    );
    assert_eq!(
        fixture
            .catalog
            .postgres_oid_registry()
            .mappings()
            .collect::<BTreeMap<_, _>>(),
        before
    );
}

#[test]
fn dropped_postgres_oids_are_removed_and_never_reused_after_reopen() {
    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("old_items"),
            vec![NewColumn::new(
                Identifier::unquoted("id"),
                ScalarType::Int64,
            )],
        )
        .expect("old table");
    let old_object = PostgresOidObject::Table(table_id);
    let old_oid = catalog.postgres_oid(old_object).expect("old OID");
    catalog
        .drop_table(table_id, DropBehavior::Restrict)
        .expect("drop old table");
    assert!(catalog.postgres_oid_registry().oid(old_object).is_none());
    assert_eq!(
        catalog
            .postgres_oid(old_object)
            .expect_err("dropped object has no live OID")
            .sql_state,
        "22023"
    );

    let encoded = serde_json::to_vec(&catalog).expect("serialize dropped registry");
    let mut reopened: Catalog = serde_json::from_slice(&encoded).expect("reopen catalog");
    let new_table_id = reopened
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("new_items"),
            vec![NewColumn::new(
                Identifier::unquoted("id"),
                ScalarType::Int64,
            )],
        )
        .expect("new table");
    let new_oid = reopened
        .postgres_oid(PostgresOidObject::Table(new_table_id))
        .expect("new OID");
    assert!(new_oid.get() > old_oid.get());
}

#[test]
fn legacy_catalog_reconstructs_deterministic_stable_postgres_oids() {
    let fixture = oid_fixture();
    let mut legacy = serde_json::to_value(&fixture.catalog).expect("serialize legacy source");
    legacy
        .as_object_mut()
        .expect("catalog object")
        .remove("postgres_oid_registry");
    let first: Catalog = serde_json::from_value(legacy.clone()).expect("first legacy reopen");
    let second: Catalog = serde_json::from_value(legacy).expect("second legacy reopen");
    assert_eq!(
        first
            .postgres_oid_registry()
            .mappings()
            .collect::<BTreeMap<_, _>>(),
        second
            .postgres_oid_registry()
            .mappings()
            .collect::<BTreeMap<_, _>>()
    );

    let first_encoding = serde_json::to_vec(&first).expect("serialize reconstructed registry");
    let reopened: Catalog =
        serde_json::from_slice(&first_encoding).expect("reopen reconstructed registry");
    let second_encoding = serde_json::to_vec(&reopened).expect("reserialize registry");
    assert_eq!(first_encoding, second_encoding);
    assert_eq!(first, reopened);
}

#[test]
fn rejects_duplicate_and_corrupt_postgres_oid_mappings() {
    let fixture = oid_fixture();
    let mut duplicate = serde_json::to_value(&fixture.catalog).expect("serialize registry");
    let mappings = duplicate
        .get_mut("postgres_oid_registry")
        .and_then(|registry| registry.get_mut("mappings"))
        .and_then(serde_json::Value::as_array_mut)
        .expect("registry mappings");
    let duplicate_oid = mappings[0].get("oid").cloned().expect("first OID");
    mappings[1]
        .as_object_mut()
        .expect("mapping")
        .insert("oid".into(), duplicate_oid);
    let error = serde_json::from_value::<Catalog>(duplicate).expect_err("duplicate OID");
    assert!(error.to_string().contains("XX001"));

    let mut corrupt = serde_json::to_value(&fixture.catalog).expect("serialize registry");
    corrupt
        .get_mut("postgres_oid_registry")
        .and_then(|registry| registry.get_mut("mappings"))
        .and_then(serde_json::Value::as_array_mut)
        .expect("registry mappings")
        .pop();
    let error = serde_json::from_value::<Catalog>(corrupt).expect_err("missing mapping");
    assert!(error.to_string().contains("XX001"));
}

#[test]
fn postgres_oid_exhaustion_is_atomic_and_explicit() {
    let mut catalog = Catalog::default();
    catalog.postgres_oid_registry.next_oid = POSTGRES_OID_EXHAUSTED;
    catalog
        .validate_postgres_oid_registry()
        .expect("exhausted cursor is durable");
    let before = catalog.clone();
    let error = catalog
        .create_schema(Identifier::unquoted("exhausted"))
        .expect_err("OID exhaustion");
    assert_eq!(error.sql_state, "54000");
    assert_eq!(catalog, before);
}

#[test]
fn cloned_catalog_candidates_do_not_publish_rolled_back_oids() {
    let committed = Catalog::default();
    let mut rolled_back = committed.clone();
    let rolled_back_schema = rolled_back
        .create_schema(Identifier::unquoted("candidate"))
        .expect("candidate schema");
    let rolled_back_oid = rolled_back
        .postgres_oid(PostgresOidObject::Schema(rolled_back_schema))
        .expect("candidate OID");
    assert_eq!(
        committed
            .postgres_oid(PostgresOidObject::Schema(rolled_back_schema))
            .expect_err("unpublished candidate")
            .sql_state,
        "22023"
    );
    drop(rolled_back);

    let mut retried = committed.clone();
    let retried_schema = retried
        .create_schema(Identifier::unquoted("candidate"))
        .expect("retried schema");
    assert_eq!(retried_schema, rolled_back_schema);
    assert_eq!(
        retried
            .postgres_oid(PostgresOidObject::Schema(retried_schema))
            .expect("retried OID"),
        rolled_back_oid
    );
}

#[test]
fn system_relation_descriptors_are_unique_stable_and_lookupable() {
    let catalog = Catalog::default();
    let relations = system_relations();
    assert_eq!(relations.len(), 27);

    let mut table_ids = BTreeSet::new();
    let mut relation_oids = BTreeSet::new();
    let mut qualified_names = BTreeSet::new();
    let mut column_ids = BTreeSet::new();
    for relation in relations {
        assert!(table_ids.insert(relation.table_id));
        assert!(relation_oids.insert(relation.oid));
        assert!(relation.oid.is_builtin());
        assert!(qualified_names.insert((relation.schema, relation.name)));
        assert!(Catalog::is_system_schema(&Identifier::unquoted(
            relation.schema
        )));
        assert!(Catalog::is_system_table(relation.table_id));
        assert_eq!(system_relation(relation.table_id), Some(relation));

        let table = catalog
            .table(
                &Identifier::unquoted(relation.schema),
                &Identifier::unquoted(relation.name),
            )
            .expect("system relation by name");
        assert_eq!(catalog.table_by_id(relation.table_id), Some(table));
        assert_eq!(table.schema_id, relation.schema_id);
        assert_eq!(table.columns().len(), relation.columns.len());
        for (column, descriptor) in table.columns().iter().zip(relation.columns) {
            assert!(column_ids.insert(column.id));
            assert_eq!(column.id, descriptor.id);
            assert_eq!(column.name.as_str(), descriptor.name);
            assert_eq!(column.data_type, descriptor.data_type);
            assert_eq!(column.nullable, descriptor.nullable);
        }
    }

    let namespace = system_relation(PG_NAMESPACE_TABLE_ID).expect("pg_namespace");
    assert_eq!(namespace.schema, "pg_catalog");
    assert_eq!(namespace.name, "pg_namespace");
    assert_eq!(namespace.oid.get(), 2_615);
    assert_eq!(
        system_relation(PG_AM_TABLE_ID).expect("pg_am").oid.get(),
        2_601
    );
    assert_eq!(
        system_relation(PG_COLLATION_TABLE_ID)
            .expect("pg_collation")
            .oid
            .get(),
        3_456
    );
    assert_eq!(
        system_relation(PG_DESCRIPTION_TABLE_ID)
            .expect("pg_description")
            .oid
            .get(),
        2_609
    );
    assert_eq!(
        namespace
            .columns
            .iter()
            .map(|column| (column.name, &column.data_type, column.nullable))
            .collect::<Vec<_>>(),
        vec![
            ("oid", &ScalarType::Oid, false),
            ("nspname", &ScalarType::Name, false),
            ("nspowner", &ScalarType::Oid, false),
        ]
    );
    assert_eq!(
        catalog
            .schema(&Identifier::quoted("pg_catalog"))
            .expect("quoted system schema")
            .id,
        PG_CATALOG_SCHEMA_ID
    );
    assert!(
        catalog
            .schema(&Identifier::quoted("pg_catalog"))
            .expect("quoted system schema")
            .table(&Identifier::quoted("pg_namespace"))
            .is_some()
    );
}

#[test]
fn system_relations_are_not_serialized_or_registered_as_user_objects() {
    let catalog = Catalog::default();
    let encoded = serde_json::to_string(&catalog).expect("serialize catalog");
    assert!(!encoded.contains("pg_catalog"));
    assert!(!encoded.contains("information_schema"));
    for relation in system_relations() {
        assert!(
            !catalog
                .object_refs()
                .contains(&CatalogObjectRef::Table(relation.table_id))
        );
        assert!(
            catalog
                .postgres_oid_registry()
                .oid(PostgresOidObject::Table(relation.table_id))
                .is_none()
        );
    }

    let reopened: Catalog = serde_json::from_str(&encoded).expect("reopen catalog");
    assert_eq!(reopened, catalog);
    assert!(reopened.table_by_id(PG_NAMESPACE_TABLE_ID).is_some());
}

#[test]
fn system_catalog_mutations_fail_atomically_with_insufficient_privilege() {
    fn assert_read_only<T: std::fmt::Debug>(result: ordadb_types::Result<T>) {
        let error = result.expect_err("system catalog mutation must fail");
        assert_eq!(error.sql_state, "42501");
    }

    let mut catalog = Catalog::default();
    let before = catalog.clone();
    let schema = Identifier::unquoted("pg_catalog");
    let table_id = PG_NAMESPACE_TABLE_ID;
    let column_id = system_relation(table_id).expect("pg_namespace").columns[0].id;

    assert_read_only(catalog.create_schema(schema.clone()));
    assert_read_only(catalog.create_schema(Identifier::quoted("pg_catalog")));
    assert_read_only(catalog.rename_schema(
        PG_CATALOG_SCHEMA_ID,
        Identifier::unquoted("renamed_catalog"),
    ));
    assert_read_only(catalog.drop_schema(PG_CATALOG_SCHEMA_ID, DropBehavior::Cascade));
    assert_read_only(catalog.create_table(
        &schema,
        Identifier::unquoted("blocked"),
        vec![NewColumn::new(
            Identifier::unquoted("id"),
            ScalarType::Int64,
        )],
    ));
    assert_read_only(catalog.rename_table(table_id, Identifier::unquoted("blocked")));
    assert_read_only(catalog.drop_table(table_id, DropBehavior::Cascade));
    assert_read_only(catalog.rename_column(
        table_id,
        column_id,
        Identifier::unquoted("blocked"),
    ));
    assert_read_only(catalog.add_column(
        table_id,
        NewColumn::new(Identifier::unquoted("blocked"), ScalarType::Text),
    ));
    assert_read_only(catalog.alter_column(
        table_id,
        column_id,
        Some(ScalarType::Text),
        None,
        None,
        None,
    ));
    assert_read_only(catalog.drop_column(table_id, column_id, DropBehavior::Cascade));
    assert_read_only(catalog.create_index(
        table_id,
        NewIndex {
            name: Identifier::unquoted("blocked_idx"),
            key_columns: vec![Identifier::unquoted("oid")],
            include_columns: Vec::new(),
            unique: false,
            method: IndexMethod::BTree,
            options: IndexOptions::BTree,
        },
    ));
    assert_read_only(catalog.create_constraint(
        table_id,
        NewConstraint {
            name: Identifier::unquoted("blocked_check"),
            kind: NewConstraintKind::Check {
                expression: CatalogExpression::new("true"),
            },
        },
    ));
    assert_read_only(catalog.create_sequence(
        &schema,
        NewSequence::new(Identifier::unquoted("blocked_seq")),
    ));
    assert_read_only(catalog.create_view(
        &schema,
        NewView {
            name: Identifier::unquoted("blocked_view"),
            kind: ViewKind::Regular,
            query: "SELECT 1".into(),
            output: Schema::empty(),
            materialized_table_id: None,
            populated: true,
            references: Vec::new(),
        },
    ));
    assert_read_only(catalog.create_or_replace_routine(
        &schema,
        NewRoutine {
            name: Identifier::unquoted("blocked_routine"),
            kind: RoutineKind::Function,
            arguments: Vec::new(),
            return_type: None,
            return_declared_type: None,
            returns_set: false,
            language: "plpgsql".into(),
            body: "BEGIN RETURN; END".into(),
            replace: false,
            references: Vec::new(),
        },
    ));
    assert_read_only(catalog.create_trigger(
        table_id,
        Identifier::unquoted("blocked_trigger"),
        TriggerTiming::Before,
        BTreeSet::new(),
        RoutineId::new(999),
    ));
    assert_read_only(catalog.set_table_statistics(table_id, TableStatistics::default()));
    assert_read_only(catalog.table_by_id_mut(table_id));

    assert_eq!(catalog, before);
}

#[test]
fn legacy_serialized_system_names_remain_read_only_by_id() {
    let mut catalog = Catalog::default();
    let schema_id = SchemaId::new(99);
    let table_id = TableId::new(99);
    let mut table =
        TableDefinition::expression_scope(Identifier::unquoted("oid"), ScalarType::Int64);
    table.id = table_id;
    table.schema_id = schema_id;
    table.name = Identifier::unquoted("legacy_relation");
    let schema_name = Identifier::unquoted("pg_catalog");
    catalog.database.schemas.insert(
        schema_name.clone(),
        SchemaDefinition {
            id: schema_id,
            database_id: catalog.database.id,
            name: schema_name,
            tables: BTreeMap::from([(table.name.clone(), table)]),
            sequences: BTreeMap::new(),
            views: BTreeMap::new(),
            routines: BTreeMap::new(),
            types: BTreeMap::new(),
        },
    );
    let before = catalog.clone();

    let schema_error = catalog
        .rename_schema(schema_id, Identifier::unquoted("renamed"))
        .expect_err("legacy system schema rename");
    assert_eq!(schema_error.sql_state, "42501");
    let table_error = catalog
        .rename_table(table_id, Identifier::unquoted("renamed"))
        .expect_err("legacy system table rename");
    assert_eq!(table_error.sql_state, "42501");
    assert_eq!(catalog, before);
}

#[test]
fn routine_modes_use_input_signatures_and_legacy_defaults() {
    let legacy: RoutineArgument = serde_json::from_value(serde_json::json!({
        "name": null,
        "data_type": "int64",
        "declared_type": null
    }))
    .expect("legacy routine argument");
    assert_eq!(legacy.mode, RoutineArgumentMode::In);

    let mut catalog = Catalog::default();
    let routine = |output_type| NewRoutine {
        name: Identifier::unquoted("mode_probe"),
        kind: RoutineKind::Procedure,
        arguments: vec![
            RoutineArgument {
                name: Some(Identifier::unquoted("input_value")),
                data_type: ScalarType::Int64,
                declared_type: None,
                mode: RoutineArgumentMode::In,
            },
            RoutineArgument {
                name: Some(Identifier::unquoted("output_value")),
                data_type: output_type,
                declared_type: None,
                mode: RoutineArgumentMode::Out,
            },
        ],
        return_type: None,
        return_declared_type: None,
        returns_set: false,
        language: "plpgsql".into(),
        body: "BEGIN RETURN; END".into(),
        replace: false,
        references: Vec::new(),
    };
    catalog
        .create_or_replace_routine(&Identifier::unquoted("public"), routine(ScalarType::Text))
        .expect("first input signature");
    let duplicate = catalog
        .create_or_replace_routine(&Identifier::unquoted("public"), routine(ScalarType::Int32))
        .expect_err("OUT type does not change the input signature");
    assert_eq!(duplicate.sql_state, "42723");
}

#[test]
fn trigger_level_defaults_to_row_and_validates_activation() {
    let legacy: TriggerDefinition = serde_json::from_value(serde_json::json!({
        "id": 1,
        "table_id": 1,
        "name": "u:legacy_trigger",
        "timing": "before",
        "events": ["insert"],
        "routine_id": 1,
        "enabled": true
    }))
    .expect("legacy trigger definition");
    assert_eq!(legacy.level, TriggerLevel::Row);
    assert_eq!(legacy.target, TriggerTarget::Table(TableId::new(1)));

    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("trigger_level_probe"),
            vec![NewColumn::new(
                Identifier::unquoted("id"),
                ScalarType::Int64,
            )],
        )
        .expect("table");
    let routine_id = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            NewRoutine {
                name: Identifier::unquoted("trigger_level_fn"),
                kind: RoutineKind::Function,
                arguments: Vec::new(),
                return_type: None,
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN; END".into(),
                replace: false,
                references: Vec::new(),
            },
        )
        .expect("trigger routine");
    let trigger_id = catalog
        .create_trigger_with_level(
            table_id,
            Identifier::unquoted("statement_trigger"),
            TriggerTiming::AfterStatement,
            TriggerLevel::Statement,
            BTreeSet::from([TriggerEvent::Insert]),
            routine_id,
        )
        .expect("statement trigger");
    assert_eq!(
        catalog.trigger_by_id(trigger_id).expect("trigger").level,
        TriggerLevel::Statement
    );
    let invalid = catalog
        .create_trigger_with_level(
            table_id,
            Identifier::unquoted("invalid_trigger"),
            TriggerTiming::After,
            TriggerLevel::Statement,
            BTreeSet::from([TriggerEvent::Insert]),
            routine_id,
        )
        .expect_err("row timing cannot be statement level");
    assert_eq!(invalid.sql_state, "0A000");
}
