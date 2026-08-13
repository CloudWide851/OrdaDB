
impl SpillManager {
    fn partition_paths(&mut self, label: &str, count: usize) -> Result<Vec<PathBuf>> {
        let query_dir = self.ensure_query_dir()?;
        Ok((0..count)
            .map(|partition| query_dir.join(format!("{label}-{partition}.spill")))
            .collect())
    }

    fn write_partitioned_rows(
        &mut self,
        label: &str,
        rows: &[Row],
        key_index: usize,
        count: usize,
        memory: &QueryMemoryContext,
    ) -> Result<Vec<PathBuf>> {
        let paths = self.partition_paths(label, count)?;
        let mut writers = paths
            .iter()
            .map(|path| create_spill_writer(path, memory))
            .collect::<Result<Vec<_>>>()?;
        for row in rows {
            let value = row
                .values
                .get(key_index)
                .ok_or_else(|| DbError::internal("spill key is out of bounds"))?;
            if value.is_null() {
                continue;
            }
            let key = encode_hash_value(value)?;
            let partition = stable_partition(&key, count);
            write_spill_record(&mut writers[partition], row, memory)?;
        }
        for writer in &mut writers {
            writer.flush().map_err(spill_io_error)?;
        }
        Ok(paths)
    }

    fn read_matching_rows(
        &self,
        path: &Path,
        key_index: usize,
        key: &[u8],
        memory: &QueryMemoryContext,
    ) -> Result<ReservedValues<Row>> {
        let mut reservation = memory.try_reserve(0)?;
        if !path.exists() {
            return Ok(ReservedValues {
                values: Vec::new(),
                reservation,
            });
        }
        let mut rows = Vec::new();
        let mut reader = open_spill_reader(path, memory)?;
        while let Some(record) = read_spill_record::<Row>(&mut reader, memory)? {
            let row = record.value;
            let value = row
                .values
                .get(key_index)
                .ok_or_else(|| DbError::new("XX001", "hash join spill key is missing"))?;
            if encode_hash_value(value)? == key {
                let row_bytes = estimated_row_bytes(&row);
                reservation.grow(row_bytes)?;
                rows.push(row);
            }
        }
        Ok(ReservedValues {
            values: rows,
            reservation,
        })
    }

    fn write_group_partials(
        &self,
        paths: &[PathBuf],
        groups: &[GroupAccumulator],
        memory: &QueryMemoryContext,
    ) -> Result<()> {
        let mut writers = paths
            .iter()
            .map(|path| {
                if path.exists() {
                    let mut writer = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(spill_io_error)?;
                    writer.seek(SeekFrom::End(0)).map_err(spill_io_error)?;
                    reserve_spill_writer(writer, memory)
                } else {
                    create_spill_writer(path, memory)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        for group in groups {
            let key = serde_json::to_vec(&group.key).map_err(|error| {
                DbError::new("58030", "aggregate spill key encoding failed")
                    .with_detail(error.to_string())
            })?;
            let partition = stable_partition(&key, paths.len());
            write_spill_record(&mut writers[partition], group, memory)?;
        }
        for writer in &mut writers {
            writer.flush().map_err(spill_io_error)?;
        }
        Ok(())
    }

    fn read_and_merge_groups(
        &self,
        path: &Path,
        memory: &QueryMemoryContext,
        specs: &[AggregateSpec],
    ) -> Result<ReservedValues<GroupAccumulator>> {
        let mut reader = open_spill_reader(path, memory)?;
        let mut groups = Vec::<GroupAccumulator>::new();
        let mut reservation = memory.try_reserve(0)?;
        while let Some(record) = read_spill_record::<GroupAccumulator>(&mut reader, memory)? {
            let incoming = record.value;
            if incoming.aggregates.len() != specs.len() {
                return Err(DbError::new(
                    "XX001",
                    "aggregate spill state width is invalid",
                ));
            }
            if let Some(group) = groups.iter_mut().find(|group| group.key == incoming.key) {
                let before = group.estimated_bytes();
                group.merge(incoming, specs)?;
                let after = group.estimated_bytes();
                if after > before {
                    reservation.grow(after - before)?;
                } else if before > after {
                    reservation.resize(reservation.bytes().saturating_sub(before - after))?;
                }
            } else {
                let group_bytes = incoming.estimated_bytes();
                reservation.grow(group_bytes)?;
                groups.push(incoming);
            }
        }
        groups.sort_by_key(|group| group.first_ordinal);
        Ok(ReservedValues {
            values: groups,
            reservation,
        })
    }
}

fn equi_join_columns(expr: &BoundExpr, right_offset: usize) -> Option<(usize, usize)> {
    let BoundExprKind::Binary {
        left,
        op: BinaryOperator::Eq,
        right,
    } = &expr.kind
    else {
        return None;
    };
    let (BoundExprKind::Column { index: left_index }, BoundExprKind::Column { index: right_index }) =
        (&left.kind, &right.kind)
    else {
        return None;
    };
    if *left_index < right_offset && *right_index >= right_offset {
        Some((*left_index, *right_index))
    } else if *right_index < right_offset && *left_index >= right_offset {
        Some((*right_index, *left_index))
    } else {
        None
    }
}

fn encode_hash_value(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| {
        DbError::internal("hash key encoding failed").with_detail(error.to_string())
    })
}

fn stable_partition(key: &[u8], count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % count.max(1)
}

fn limit_from_value(value: Value) -> Result<Option<usize>> {
    match value {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map(Some)
            .map_err(|_| DbError::new("22003", "LIMIT value is out of range")),
        Value::Null => Ok(None),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn offset_from_value(value: Value) -> Result<usize> {
    match value {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::new("22003", "OFFSET value is out of range")),
        Value::Null => Ok(0),
        _ => Err(DbError::new(
            "2201X",
            "OFFSET must be a non-negative integer",
        )),
    }
}

#[cfg(test)]
mod tests {
    include!("window_and_resource_tests.rs");
    include!("join_aggregate_and_sort_tests.rs");
}
