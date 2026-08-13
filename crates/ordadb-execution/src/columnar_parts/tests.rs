
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_vectors_round_trip_nulls_and_variable_width_values() {
        let rows = vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Text("alpha".into()),
                Value::Null,
            ]),
            Row::new(vec![Value::Int64(2), Value::Null, Value::Boolean(true)]),
        ];
        let chunk = DataChunk::from_rows(&rows).expect("chunk");
        assert_eq!(chunk.len(), 2);
        assert_eq!(chunk.into_rows().expect("rows"), rows);
    }

    #[test]
    fn selection_vector_filters_and_truncates_without_moving_columns() {
        let rows = (0..6)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let mut chunk = DataChunk::from_rows(&rows).expect("chunk");
        chunk
            .selection_mut()
            .retain(|physical| Ok(physical % 2 == 0))
            .expect("filter");
        chunk.selection_mut().truncate(2);
        assert_eq!(
            chunk.into_rows().expect("rows"),
            vec![
                Row::new(vec![Value::Int64(0)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
    }

    #[test]
    fn literal_comparison_filters_a_typed_column_without_materializing_values() {
        let mut chunk = DataChunk::from_rows(&[
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Int64(3)]),
            Row::new(vec![Value::Int64(4)]),
        ])
        .expect("chunk");
        chunk
            .retain_literal_comparison(0, &Value::Int64(3), BinaryOperator::GtEq)
            .expect("typed fast path")
            .expect("filter");
        assert_eq!(chunk.selection().indexes, [2, 3]);
    }

    #[test]
    fn row_backed_comparison_defers_mismatched_physical_literal_types() {
        let snapshot = Arc::new(vec![Row::new(vec![Value::Int64(7)])]);
        let chunk = DataChunk::from_row_snapshot(snapshot, 0, 1).expect("row-backed data chunk");

        assert!(
            chunk
                .compare_literal(0, 0, &Value::Int32(7), BinaryOperator::Eq)
                .is_none()
        );
    }

    #[test]
    fn selected_row_estimate_combines_fixed_and_variable_width_columns() {
        let mut chunk = DataChunk::from_rows(&[
            Row::new(vec![Value::Int64(1), Value::Text("alpha".into())]),
            Row::new(vec![Value::Int64(2), Value::Text("beta".into())]),
        ])
        .expect("chunk");
        chunk.selection_mut().truncate(1);
        assert_eq!(
            chunk.estimated_selected_row_bytes().expect("estimate"),
            std::mem::size_of::<Row>() + std::mem::size_of::<Value>() * 2 + "alpha".len()
        );
    }

    #[test]
    fn pool_reuses_only_compatible_bounded_chunks() {
        let rows = vec![Row::new(vec![Value::Int64(1)])];
        let grant = MemoryGrant::new(1_024, 4_096).expect("grant");
        let mut pool = ChunkPool::new(2, 1);
        let (chunk, reservation) = pool.materialize(&rows, &grant).expect("first");
        pool.recycle(chunk, reservation);
        assert_eq!(pool.retained(), 1);
        assert!(grant.current_bytes() > 0);
        let (chunk, reservation) = pool.materialize(&rows, &grant).expect("reused");
        assert_eq!(chunk.into_rows().expect("rows"), rows);
        drop(reservation);
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn identity_row_snapshot_materializes_selected_rows_without_column_rebuild() {
        let snapshot = Arc::new(vec![
            Row::new(vec![Value::Int64(1), Value::Text("one".into())]),
            Row::new(vec![Value::Int64(2), Value::Text("two".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("three".into())]),
        ]);
        let mut chunk = DataChunk::from_row_snapshot(Arc::clone(&snapshot), 0, snapshot.len())
            .expect("row-backed chunk");
        let columns = chunk.columns.as_ptr();
        assert!(
            chunk
                .project_columns_in_place(&[(0, ScalarType::Int64), (1, ScalarType::Text)])
                .expect("identity projection")
        );
        assert_eq!(chunk.columns.as_ptr(), columns);
        chunk.selection.indexes = vec![2, 0];

        assert_eq!(
            chunk.into_rows().expect("selected rows"),
            vec![snapshot[2].clone(), snapshot[0].clone()]
        );
    }

    #[test]
    fn projected_row_snapshot_materializes_only_the_requested_columns() {
        let snapshot = Arc::new(vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("one".into()),
        ])]);
        let mut chunk = DataChunk::from_row_snapshot(Arc::clone(&snapshot), 0, snapshot.len())
            .expect("row-backed chunk");
        assert!(
            chunk
                .project_columns_in_place(&[(1, ScalarType::Text)])
                .expect("projection")
        );

        assert_eq!(
            chunk.into_rows().expect("projected rows"),
            vec![Row::new(vec![Value::Text("one".into())])]
        );
    }

    #[test]
    fn pool_releases_incompatible_capacity_before_materializing_a_replacement() {
        let rows = (0..32)
            .map(|value| Row::new(vec![Value::Int64(value), Value::Int64(value)]))
            .collect::<Vec<_>>();
        let input_bytes = rows.iter().map(crate::estimated_row_bytes).sum();
        let grant = MemoryGrant::new(256, input_bytes).expect("grant");
        let mut pool = ChunkPool::new(32, 1);
        let (mut projected, mut reservation) =
            pool.materialize(&rows, &grant).expect("input chunk");
        assert!(
            projected
                .project_columns_in_place(&[(0, ScalarType::Int64)])
                .expect("projection")
        );
        reservation
            .resize(projected.estimated_bytes())
            .expect("projected reservation");
        pool.recycle(projected, reservation);

        let (replacement, reservation) = pool
            .materialize(&rows, &grant)
            .expect("replacement input chunk");
        assert_eq!(replacement.columns().len(), 2);
        drop((replacement, reservation));
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn mixed_physical_types_are_rejected() {
        let error = DataChunk::from_rows(&[
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Text("one".into())]),
        ])
        .expect_err("mixed");
        assert_eq!(error.sql_state, "42804");
    }
}
