
#[test]
fn regular_view_instead_of_triggers_round_trip_drop_with_owner_and_fail_closed() {
    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("view_trigger_rows"),
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
                name: Identifier::unquoted("view_trigger_target"),
                kind: ViewKind::Regular,
                query: "SELECT id FROM view_trigger_rows".into(),
                output: Schema::new(vec![ordadb_types::Field::new(
                    "id",
                    ScalarType::Int64,
                    false,
                )]),
                materialized_table_id: None,
                populated: true,
                references: vec![CatalogObjectRef::Table(table_id)],
            },
        )
        .expect("view");
    let routine_id = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            NewRoutine {
                name: Identifier::unquoted("view_trigger_fn"),
                kind: RoutineKind::Function,
                arguments: Vec::new(),
                return_type: None,
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN NEW; END".into(),
                replace: false,
                references: Vec::new(),
            },
        )
        .expect("routine");
    let trigger_id = catalog
        .create_trigger_on_target_with_level(
            TriggerTarget::View(view_id),
            Identifier::unquoted("view_trigger"),
            TriggerTiming::InsteadOf,
            TriggerLevel::Row,
            BTreeSet::from([TriggerEvent::Insert]),
            routine_id,
        )
        .expect("view trigger");
    assert_eq!(
        catalog.trigger_by_id(trigger_id).expect("trigger").target,
        TriggerTarget::View(view_id)
    );

    let encoded = serde_json::to_value(&catalog).expect("serialize catalog");
    let reopened: Catalog = serde_json::from_value(encoded.clone()).expect("reopen catalog");
    assert_eq!(reopened, catalog);

    let mut downgraded = encoded;
    downgraded["database"]["schemas"]["u:public"]["views"]["u:view_trigger_target"]
        .as_object_mut()
        .expect("view object")
        .remove("triggers");
    let error = serde_json::from_value::<Catalog>(downgraded)
        .expect_err("old projection must not silently discard a view trigger");
    assert!(error.to_string().contains("OID registry"));

    let removed = catalog
        .drop_view(view_id, DropBehavior::Restrict)
        .expect("owned view trigger drops with view");
    assert!(removed.contains(&CatalogObjectRef::Trigger(trigger_id)));
    assert!(catalog.trigger_by_id(trigger_id).is_none());
    assert!(catalog.view_by_id(view_id).is_none());
}
