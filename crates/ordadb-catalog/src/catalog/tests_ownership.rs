use std::collections::{BTreeMap, BTreeSet};

use ordadb_types::{
    ColumnId, ConstraintId, Identifier, IndexId, RoutineId, ScalarType, Schema, SchemaId,
    SequenceId, TableId, TriggerId, TypeId, ViewId,
};

use super::{
    Catalog, CatalogExpression, CatalogObjectRef, CatalogOwner, ConstraintKind,
    DependencyGraph, DomainBaseType, DomainConstraint, DropBehavior, EnumValuePosition,
    FullTextAnalyzer, IndexDefinition, IndexMethod, IndexOptions, NewColumn, NewConstraint,
    NewConstraintKind, NewIndex, NewRoutine, NewSequence, NewView, PG_AM_TABLE_ID,
    PG_CATALOG_SCHEMA_ID, PG_COLLATION_TABLE_ID, PG_DESCRIPTION_TABLE_ID,
    PG_NAMESPACE_TABLE_ID, POSTGRES_OID_EXHAUSTED, POSTGRES_OID_FIRST_USER,
    POSTGRES_OID_LAST_BUILTIN, PostgresOid, PostgresOidObject, ReferentialAction,
    RoutineArgument, RoutineArgumentMode, RoutineKind, SchemaDefinition, TableDefinition,
    TableStatistics, TriggerDefinition, TriggerEvent, TriggerLevel, TriggerTarget,
    TriggerTiming, UserDefinedTypeKind, VectorDistanceMetric, ViewKind, system_relation,
    system_relations,
};

struct OidFixture {
    catalog: Catalog,
    schema_id: SchemaId,
    table_id: TableId,
    column_id: ColumnId,
    index_id: IndexId,
    constraint_id: ConstraintId,
    sequence_id: SequenceId,
    view_id: ViewId,
    materialized_view_id: ViewId,
    routine_id: RoutineId,
    trigger_id: TriggerId,
    type_id: TypeId,
}

fn oid_fixture() -> OidFixture {
    let mut catalog = Catalog::default();
    let schema_id = catalog
        .create_schema(Identifier::unquoted("app"))
        .expect("schema");
    let type_id = catalog
        .create_enum_type(
            &Identifier::unquoted("app"),
            Identifier::unquoted("status"),
            vec!["ready".into(), "done".into()],
        )
        .expect("type");
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("app"),
            Identifier::unquoted("items"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("label"), ScalarType::Text),
            ],
        )
        .expect("table");
    let column_id = catalog.table_by_id(table_id).expect("table").columns()[1].id;
    let index_id = catalog
        .create_index(
            table_id,
            NewIndex {
                name: Identifier::unquoted("items_label_idx"),
                key_columns: vec![Identifier::unquoted("label")],
                include_columns: Vec::new(),
                unique: false,
                method: IndexMethod::BTree,
                options: IndexOptions::BTree,
            },
        )
        .expect("index");
    let constraint_id = catalog
        .create_constraint(
            table_id,
            NewConstraint {
                name: Identifier::unquoted("items_id_positive"),
                kind: NewConstraintKind::Check {
                    expression: CatalogExpression::new("id > 0"),
                },
            },
        )
        .expect("constraint");
    let sequence_id = catalog
        .create_sequence(
            &Identifier::unquoted("app"),
            NewSequence::new(Identifier::unquoted("items_id_seq")),
        )
        .expect("sequence");
    let view_id = catalog
        .create_view(
            &Identifier::unquoted("app"),
            NewView {
                name: Identifier::unquoted("item_view"),
                kind: ViewKind::Regular,
                query: "SELECT id, label FROM items".into(),
                output: Schema::empty(),
                materialized_table_id: None,
                populated: true,
                references: vec![CatalogObjectRef::Table(table_id)],
            },
        )
        .expect("view");
    let backing_table_id = catalog
        .create_table(
            &Identifier::unquoted("app"),
            Identifier::unquoted("item_rollup_storage"),
            vec![NewColumn::new(
                Identifier::unquoted("count"),
                ScalarType::Int64,
            )],
        )
        .expect("materialized backing table");
    let materialized_view_id = catalog
        .create_view(
            &Identifier::unquoted("app"),
            NewView {
                name: Identifier::unquoted("item_rollup"),
                kind: ViewKind::Materialized,
                query: "SELECT count(*) FROM items".into(),
                output: Schema::empty(),
                materialized_table_id: Some(backing_table_id),
                populated: true,
                references: vec![CatalogObjectRef::Table(table_id)],
            },
        )
        .expect("materialized view");
    let routine_id = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("app"),
            NewRoutine {
                name: Identifier::unquoted("touch_item"),
                kind: RoutineKind::Function,
                arguments: Vec::new(),
                return_type: None,
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN; END".into(),
                replace: false,
                references: vec![CatalogObjectRef::View(view_id)],
            },
        )
        .expect("routine");
    let trigger_id = catalog
        .create_trigger(
            table_id,
            Identifier::unquoted("items_touch"),
            TriggerTiming::Before,
            BTreeSet::from([TriggerEvent::Insert]),
            routine_id,
        )
        .expect("trigger");
    OidFixture {
        catalog,
        schema_id,
        table_id,
        column_id,
        index_id,
        constraint_id,
        sequence_id,
        view_id,
        materialized_view_id,
        routine_id,
        trigger_id,
        type_id,
    }
}

#[test]
fn bootstraps_public_schema_with_deterministic_ids() {
    let catalog = Catalog::default();
    assert_eq!(catalog.database().id.get(), 1);
    assert_eq!(
        catalog
            .schema(&Identifier::unquoted("PUBLIC"))
            .expect("public schema")
            .id,
        SchemaId::new(1)
    );
    assert_eq!(POSTGRES_OID_LAST_BUILTIN, 16_383);
    assert_eq!(
        catalog
            .postgres_oid(PostgresOidObject::Database(catalog.database().id))
            .expect("database OID")
            .get(),
        POSTGRES_OID_FIRST_USER
    );
    assert_eq!(
        catalog
            .postgres_oid(PostgresOidObject::Schema(SchemaId::new(1)))
            .expect("public schema OID")
            .get(),
        POSTGRES_OID_FIRST_USER + 1
    );
    assert!(
        PostgresOid::new(POSTGRES_OID_LAST_BUILTIN)
            .expect("built-in OID")
            .is_builtin()
    );
    assert_eq!(
        PostgresOid::new(0).expect_err("zero is invalid").sql_state,
        "22023"
    );
}

#[test]
fn creates_and_resolves_normalized_schema_and_table_names() {
    let mut catalog = Catalog::default();
    assert_eq!(
        catalog
            .create_schema(Identifier::unquoted("Analytics"))
            .expect("create schema"),
        SchemaId::new(2)
    );
    assert_eq!(
        catalog
            .create_table(
                &Identifier::unquoted("ANALYTICS"),
                Identifier::unquoted("Events"),
                vec![NewColumn::new(
                    Identifier::unquoted("ID"),
                    ScalarType::Int64,
                )],
            )
            .expect("create table"),
        TableId::new(1)
    );
    assert!(
        catalog
            .table(
                &Identifier::unquoted("analytics"),
                &Identifier::unquoted("events")
            )
            .is_some()
    );
}

#[test]
fn rejects_duplicate_objects_and_columns() {
    let mut catalog = Catalog::default();
    let duplicate_schema = catalog
        .create_schema(Identifier::unquoted("PUBLIC"))
        .expect_err("duplicate schema");
    assert_eq!(duplicate_schema.sql_state, "42P06");

    let duplicate_column = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("items"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("ID"), ScalarType::Int64),
            ],
        )
        .expect_err("duplicate column");
    assert_eq!(duplicate_column.sql_state, "42701");
}

#[test]
fn primary_keys_are_not_nullable_and_are_unique() {
    let mut catalog = Catalog::default();
    let mut id = NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64);
    id.primary_key = true;
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("documents"),
            vec![id],
        )
        .expect("create table");

    let column = &catalog
        .table_by_id(table_id)
        .expect("table by id")
        .columns()[0];
    assert!(!column.nullable);
    assert!(column.unique);
    let index = catalog
        .table_by_id(table_id)
        .expect("table")
        .indexes()
        .next()
        .expect("primary index");
    assert!(index.primary);
    assert!(index.unique);
}

#[test]
fn creates_composite_covering_indexes_and_rejects_overlap() {
    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("events"),
            vec![
                NewColumn::new(Identifier::unquoted("tenant"), ScalarType::Int64),
                NewColumn::new(
                    Identifier::unquoted("created_at"),
                    ScalarType::Timestamp {
                        with_timezone: false,
                    },
                ),
                NewColumn::new(Identifier::unquoted("payload"), ScalarType::Jsonb),
            ],
        )
        .expect("table");
    let index_id = catalog
        .create_index(
            table_id,
            NewIndex {
                name: Identifier::unquoted("events_tenant_created"),
                key_columns: vec![
                    Identifier::unquoted("tenant"),
                    Identifier::unquoted("created_at"),
                ],
                include_columns: vec![Identifier::unquoted("payload")],
                unique: false,
                method: IndexMethod::BTree,
                options: IndexOptions::BTree,
            },
        )
        .expect("index");
    let index = catalog.index_by_id(index_id).expect("index by id");
    assert_eq!(index.key_columns.len(), 2);
    assert_eq!(index.include_columns.len(), 1);

    let overlap = catalog
        .create_index(
            table_id,
            NewIndex {
                name: Identifier::unquoted("bad"),
                key_columns: vec![Identifier::unquoted("tenant")],
                include_columns: vec![Identifier::unquoted("tenant")],
                unique: false,
                method: IndexMethod::BTree,
                options: IndexOptions::BTree,
            },
        )
        .expect_err("overlap");
    assert_eq!(overlap.sql_state, "42701");
}

#[test]
fn creates_search_indexes_and_preserves_btree_serde_defaults() {
    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("documents"),
            vec![
                NewColumn::new(Identifier::unquoted("title"), ScalarType::Text),
                NewColumn::new(
                    Identifier::unquoted("embedding"),
                    ScalarType::Vector {
                        dimensions: Some(3),
                    },
                ),
            ],
        )
        .expect("table");
    let full_text_id = catalog
        .create_index(
            table_id,
            NewIndex {
                name: Identifier::unquoted("documents_fts"),
                key_columns: vec![Identifier::unquoted("title")],
                include_columns: Vec::new(),
                unique: false,
                method: IndexMethod::FullText,
                options: IndexOptions::FullText {
                    analyzer: FullTextAnalyzer::Whitespace,
                },
            },
        )
        .expect("full-text index");
    let hnsw_id = catalog
        .create_index(
            table_id,
            NewIndex {
                name: Identifier::unquoted("documents_embedding_hnsw"),
                key_columns: vec![Identifier::unquoted("embedding")],
                include_columns: Vec::new(),
                unique: false,
                method: IndexMethod::Hnsw,
                options: IndexOptions::Hnsw {
                    metric: VectorDistanceMetric::Cosine,
                    dimensions: 3,
                    m: 16,
                    ef_construction: 64,
                    ef_search: 40,
                },
            },
        )
        .expect("HNSW index");
    assert_eq!(
        catalog.index_by_id(full_text_id).expect("full-text").method,
        IndexMethod::FullText
    );
    assert_eq!(
        catalog.index_by_id(hnsw_id).expect("HNSW").method,
        IndexMethod::Hnsw
    );

    let definition = IndexDefinition {
        id: ordadb_types::IndexId::new(999),
        table_id,
        name: Identifier::unquoted("legacy"),
        key_columns: vec![
            catalog
                .table_by_id(table_id)
                .expect("table")
                .columns()
                .first()
                .expect("column")
                .id,
        ],
        include_columns: Vec::new(),
        unique: false,
        primary: false,
        method: IndexMethod::BTree,
        options: IndexOptions::BTree,
    };
    let mut encoded = serde_json::to_value(definition).expect("serialize legacy definition");
    let object = encoded.as_object_mut().expect("definition object");
    object.remove("method");
    object.remove("options");
    let decoded: IndexDefinition =
        serde_json::from_value(encoded).expect("decode old B+Tree definition");
    assert_eq!(decoded.method, IndexMethod::BTree);
    assert_eq!(decoded.options, IndexOptions::BTree);
}

#[test]
fn dependency_graph_rejects_cycles_and_orders_cascade_iteratively() {
    let table = CatalogObjectRef::Table(TableId::new(1));
    let first_view = CatalogObjectRef::View(ordadb_types::ViewId::new(1));
    let second_view = CatalogObjectRef::View(ordadb_types::ViewId::new(2));
    let mut graph = DependencyGraph::default();
    graph.add(first_view, table).expect("view depends on table");
    graph
        .add(second_view, first_view)
        .expect("nested view dependency");

    let restrict = graph
        .drop_order(table, DropBehavior::Restrict)
        .expect_err("restrict must fail");
    assert_eq!(restrict.sql_state, "2BP01");
    assert_eq!(
        graph
            .drop_order(table, DropBehavior::Cascade)
            .expect("cascade order"),
        vec![second_view, first_view, table]
    );

    let cycle = graph.add(table, second_view).expect_err("dependency cycle");
    assert_eq!(cycle.sql_state, "2BP01");
}

#[test]
fn persists_constraints_sequences_views_routines_and_triggers() {
    let mut catalog = Catalog::default();
    let mut parent_id = NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64);
    parent_id.primary_key = true;
    let parent = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("parents"),
            vec![parent_id],
        )
        .expect("parent");
    let child = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("children"),
            vec![NewColumn::new(
                Identifier::unquoted("parent_id"),
                ScalarType::Int64,
            )],
        )
        .expect("child");
    let referenced_column = catalog.table_by_id(parent).expect("parent table").columns()[0].id;
    catalog
        .create_constraint(
            child,
            NewConstraint {
                name: Identifier::unquoted("children_parent_fk"),
                kind: NewConstraintKind::ForeignKey {
                    columns: vec![Identifier::unquoted("parent_id")],
                    referenced_table: parent,
                    referenced_columns: vec![referenced_column],
                    on_delete: ReferentialAction::Cascade,
                    on_update: ReferentialAction::Restrict,
                },
            },
        )
        .expect("foreign key");
    assert!(matches!(
        catalog
            .table_by_id(child)
            .expect("child table")
            .constraints()
            .next()
            .expect("constraint")
            .kind,
        ConstraintKind::ForeignKey { .. }
    ));

    let sequence = catalog
        .create_sequence(
            &Identifier::unquoted("public"),
            NewSequence::new(Identifier::unquoted("children_id_seq")),
        )
        .expect("sequence");
    assert_eq!(catalog.next_sequence_value(sequence).expect("first"), 1);
    assert_eq!(catalog.next_sequence_value(sequence).expect("second"), 2);

    let view = catalog
        .create_view(
            &Identifier::unquoted("public"),
            NewView {
                name: Identifier::unquoted("child_view"),
                kind: ViewKind::Regular,
                query: "SELECT parent_id FROM children".into(),
                output: Schema::empty(),
                materialized_table_id: None,
                populated: true,
                references: vec![CatalogObjectRef::Table(child)],
            },
        )
        .expect("view");
    let routine = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            NewRoutine {
                name: Identifier::unquoted("touch_child"),
                kind: RoutineKind::Function,
                arguments: vec![RoutineArgument {
                    name: Some(Identifier::unquoted("value")),
                    data_type: ScalarType::Int64,
                    declared_type: None,
                    mode: Default::default(),
                }],
                return_type: Some(ScalarType::Int64),
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN value; END".into(),
                replace: false,
                references: vec![CatalogObjectRef::View(view)],
            },
        )
        .expect("routine");
    let trigger = catalog
        .create_trigger(
            child,
            Identifier::unquoted("children_touch"),
            TriggerTiming::Before,
            BTreeSet::from([TriggerEvent::Insert]),
            routine,
        )
        .expect("trigger");
    assert_eq!(
        catalog.trigger_by_id(trigger).expect("trigger").routine_id,
        routine
    );

    let encoded = serde_json::to_vec(&catalog).expect("serialize catalog");
    let decoded: Catalog = serde_json::from_slice(&encoded).expect("deserialize catalog");
    assert_eq!(decoded, catalog);
    assert!(decoded.sequence_by_id(sequence).is_some());
    assert!(decoded.view_by_id(view).is_some());
    assert!(decoded.routine_by_id(routine).is_some());
    assert!(decoded.trigger_by_id(trigger).is_some());
}

#[test]
fn renames_alters_and_drops_catalog_objects_without_stale_names() {
    let mut catalog = Catalog::default();
    let schema_id = catalog
        .create_schema(Identifier::unquoted("app"))
        .expect("schema");
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("app"),
            Identifier::unquoted("items"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("label"), ScalarType::Text),
            ],
        )
        .expect("table");
    let column_id = catalog.table_by_id(table_id).expect("table").columns()[1].id;
    catalog
        .rename_schema(schema_id, Identifier::unquoted("core"))
        .expect("rename schema");
    catalog
        .rename_table(table_id, Identifier::unquoted("entries"))
        .expect("rename table");
    catalog
        .rename_column(table_id, column_id, Identifier::unquoted("title"))
        .expect("rename column");
    catalog
        .alter_column(
            table_id,
            column_id,
            None,
            Some(false),
            Some(Some(super::CatalogExpression::new("'untitled'"))),
            None,
        )
        .expect("alter column");

    let table = catalog
        .table(
            &Identifier::unquoted("core"),
            &Identifier::unquoted("entries"),
        )
        .expect("renamed table");
    let column = table
        .column(&Identifier::unquoted("title"))
        .expect("renamed column");
    assert!(!column.nullable);
    assert_eq!(
        column.default.as_ref().map(|value| value.sql.as_str()),
        Some("'untitled'")
    );

    let removed = catalog
        .drop_table(table_id, DropBehavior::Restrict)
        .expect("drop table");
    assert!(removed.contains(&CatalogObjectRef::Table(table_id)));
    assert!(
        catalog
            .table(
                &Identifier::unquoted("core"),
                &Identifier::unquoted("entries")
            )
            .is_none()
    );
    catalog
        .drop_schema(schema_id, DropBehavior::Restrict)
        .expect("drop empty schema");
    assert!(catalog.schema(&Identifier::unquoted("core")).is_none());
}

#[test]
fn ownership_round_trips_and_cascade_removes_owned_children() {
    let previous = Catalog::default();
    let mut catalog = previous.clone();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("owned_items"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("label"), ScalarType::Text),
            ],
        )
        .expect("table");
    let owner = CatalogOwner::new("alice").expect("owner");
    catalog
        .assign_new_object_owners(&previous, &owner)
        .expect("assign ownership");
    let created = catalog
        .object_refs()
        .difference(&previous.object_refs())
        .copied()
        .collect::<Vec<_>>();
    assert!(!created.is_empty());
    assert!(created.iter().all(|object| {
        catalog.owner_of(*object).map(CatalogOwner::as_str) == Some("alice")
    }));
    assert!(
        catalog
            .owner_of(CatalogObjectRef::Schema(SchemaId::new(1)))
            .is_none()
    );

    let encoded = serde_json::to_vec(&catalog).expect("serialize ownership");
    let mut reopened: Catalog =
        serde_json::from_slice(&encoded).expect("deserialize ownership");
    assert!(created.iter().all(|object| {
        reopened.owner_of(*object).map(CatalogOwner::as_str) == Some("alice")
    }));

    reopened
        .drop_table(table_id, DropBehavior::Cascade)
        .expect("drop owned table");
    assert!(
        created
            .iter()
            .all(|object| reopened.owner_of(*object).is_none())
    );
}

#[test]
fn restrict_and_cascade_follow_external_dependencies() {
    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("source"),
            vec![NewColumn::new(
                Identifier::unquoted("id"),
                ScalarType::Int64,
            )],
        )
        .expect("table");
    let view_id = catalog
        .create_view(
            &Identifier::unquoted("public"),
            NewView {
                name: Identifier::unquoted("source_view"),
                kind: ViewKind::Regular,
                query: "SELECT id FROM source".into(),
                output: Schema::empty(),
                materialized_table_id: None,
                populated: true,
                references: vec![CatalogObjectRef::Table(table_id)],
            },
        )
        .expect("view");

    let error = catalog
        .drop_table(table_id, DropBehavior::Restrict)
        .expect_err("restrict dependent view");
    assert_eq!(error.sql_state, "2BP01");
    let removed = catalog
        .drop_table(table_id, DropBehavior::Cascade)
        .expect("cascade table");
    assert!(removed.contains(&CatalogObjectRef::View(view_id)));
    assert!(catalog.view_by_id(view_id).is_none());
    assert!(catalog.table_by_id(table_id).is_none());
}
