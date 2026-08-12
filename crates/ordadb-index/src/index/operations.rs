
impl BPlusTree {
    #[must_use]
    pub fn new(unique: bool) -> Self {
        Self::with_order(unique, DEFAULT_ORDER)
    }

    #[must_use]
    pub fn with_order(unique: bool, order: usize) -> Self {
        let order = order.max(3);
        Self {
            nodes: vec![Node::Leaf {
                entries: Vec::new(),
                next: None,
            }],
            root: 0,
            order,
            unique,
            key_width: None,
            entry_count: 0,
        }
    }

    pub fn from_entries(
        unique: bool,
        entries: impl IntoIterator<Item = IndexEntry>,
    ) -> Result<Self> {
        let mut tree = Self::new(unique);
        for entry in entries {
            tree.insert(entry)?;
        }
        tree.validate()?;
        Ok(tree)
    }

    pub fn insert(&mut self, entry: IndexEntry) -> Result<()> {
        match self.key_width {
            Some(width) if width != entry.key.len() => {
                return Err(DbError::new(
                    "22023",
                    "index entry key width does not match the tree",
                ));
            }
            _ => {}
        }
        if self.unique && !entry.key.contains_null() && self.contains_key_fast(&entry.key) {
            return Err(DbError::new("23505", "duplicate key violates unique index"));
        }
        if self.nodes.len() > MAX_INDEX_NODES {
            return Err(index_complexity_error(format!(
                "node count {} exceeds the limit of {MAX_INDEX_NODES}",
                self.nodes.len()
            )));
        }
        let next_entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or_else(|| index_complexity_error("entry count overflow"))?;
        let key_width = entry.key.len();
        let mut node_id = self.root;
        let mut parent_path = Vec::new();
        loop {
            let node = self
                .nodes
                .get(node_id)
                .ok_or_else(|| DbError::internal("B+Tree child points outside the node arena"))?;
            match node {
                Node::Leaf { .. } => break,
                Node::Internal { keys, children } => {
                    if parent_path.len() >= MAX_INDEX_DEPTH {
                        return Err(index_complexity_error(format!(
                            "insert path exceeds the depth limit of {MAX_INDEX_DEPTH}"
                        )));
                    }
                    let child_index = keys.partition_point(|key| &entry.key >= key);
                    let child_id = children.get(child_index).copied().ok_or_else(|| {
                        DbError::internal("B+Tree internal child cardinality is invalid")
                    })?;
                    parent_path.push((node_id, child_index));
                    node_id = child_id;
                }
            }
        }

        let mut split = self.insert_leaf(node_id, entry)?;
        while split.is_some() {
            let Some((parent_id, child_index)) = parent_path.pop() else {
                break;
            };
            let (separator, right) = split
                .take()
                .ok_or_else(|| DbError::internal("B+Tree split state disappeared"))?;
            let Node::Internal { keys, children } = &mut self.nodes[parent_id] else {
                return Err(DbError::internal(
                    "B+Tree parent changed kind during insert",
                ));
            };
            keys.insert(child_index, separator);
            children.insert(child_index + 1, right);
            if children.len() <= self.order {
                split = None;
                break;
            }

            let midpoint = keys.len() / 2;
            let right_children = children.split_off(midpoint + 1);
            let right_keys = keys.split_off(midpoint + 1);
            let promoted = keys
                .pop()
                .ok_or_else(|| DbError::internal("B+Tree split has no separator"))?;
            self.nodes.push(Node::Internal {
                keys: right_keys,
                children: right_children,
            });
            split = Some((promoted, self.nodes.len() - 1));
        }

        if let Some((separator, right)) = split {
            let old_root = self.root;
            self.nodes.push(Node::Internal {
                keys: vec![separator],
                children: vec![old_root, right],
            });
            self.root = self.nodes.len() - 1;
        }
        if self.key_width.is_none() {
            self.key_width = Some(key_width);
        }
        self.entry_count = next_entry_count;
        Ok(())
    }

    fn insert_leaf(
        &mut self,
        node_id: usize,
        entry: IndexEntry,
    ) -> Result<Option<(IndexKey, usize)>> {
        let right_id = self.nodes.len();
        let Node::Leaf { entries, next } = &mut self.nodes[node_id] else {
            return Err(DbError::internal("expected a B+Tree leaf"));
        };
        let position = entries
            .binary_search_by(|existing| {
                existing
                    .key
                    .cmp(&entry.key)
                    .then_with(|| existing.row_id.cmp(&entry.row_id))
            })
            .unwrap_or_else(|position| position);
        entries.insert(position, entry);
        if entries.len() <= self.order {
            return Ok(None);
        }

        let right_entries = entries.split_off(entries.len().div_ceil(2));
        let separator = right_entries
            .first()
            .ok_or_else(|| DbError::internal("B+Tree leaf split produced an empty right leaf"))?
            .key
            .clone();
        let old_next = *next;
        *next = Some(right_id);
        self.nodes.push(Node::Leaf {
            entries: right_entries,
            next: old_next,
        });
        Ok(Some((separator, right_id)))
    }

    #[must_use]
    pub fn get(&self, key: &IndexKey) -> Vec<&IndexEntry> {
        self.get_iter(key).collect()
    }

    #[must_use]
    pub fn range(&self, lower: Bound<&IndexKey>, upper: Bound<&IndexKey>) -> Vec<&IndexEntry> {
        self.range_iter(lower, upper).collect()
    }

    #[must_use]
    pub fn entries(&self) -> Vec<&IndexEntry> {
        self.iter().collect()
    }

    #[must_use]
    pub fn iter(&self) -> BPlusTreeIter<'_> {
        BPlusTreeIter {
            tree: self,
            leaf: self.leftmost_leaf(),
            offset: 0,
            upper: Bound::Unbounded,
        }
    }

    #[must_use]
    pub fn get_iter(&self, key: &IndexKey) -> BPlusTreeIter<'_> {
        self.range_iter(Bound::Included(key), Bound::Included(key))
    }

    #[must_use]
    pub fn range_iter(
        &self,
        lower: Bound<&IndexKey>,
        upper: Bound<&IndexKey>,
    ) -> BPlusTreeIter<'_> {
        let (leaf, offset) = self.seek_lower_bound(lower);
        BPlusTreeIter {
            tree: self,
            leaf,
            offset,
            upper: clone_bound(upper),
        }
    }

    #[must_use]
    pub fn owned_iter(self: &Arc<Self>) -> BPlusTreeOwnedIter {
        BPlusTreeOwnedIter {
            tree: Arc::clone(self),
            leaf: self.leftmost_leaf(),
            offset: 0,
            upper: Bound::Unbounded,
        }
    }

    #[must_use]
    pub fn owned_get_iter(self: &Arc<Self>, key: IndexKey) -> BPlusTreeOwnedIter {
        self.owned_range_iter(Bound::Included(key.clone()), Bound::Included(key))
    }

    #[must_use]
    pub fn owned_range_iter(
        self: &Arc<Self>,
        lower: Bound<IndexKey>,
        upper: Bound<IndexKey>,
    ) -> BPlusTreeOwnedIter {
        let (leaf, offset) = self.seek_lower_bound(bound_ref(&lower));
        BPlusTreeOwnedIter {
            tree: Arc::clone(self),
            leaf,
            offset,
            upper,
        }
    }

    #[must_use]
    pub fn height(&self) -> usize {
        let mut height = 1;
        let mut node_id = self.root;
        while let Node::Internal { children, .. } = &self.nodes[node_id] {
            height += 1;
            node_id = children[0];
        }
        height
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn validate(&self) -> Result<()> {
        if self.root >= self.nodes.len() {
            return Err(corruption("B+Tree root points outside the node arena"));
        }
        if self.nodes.len() > MAX_INDEX_NODES {
            return Err(index_complexity_error(format!(
                "node count {} exceeds the limit of {MAX_INDEX_NODES}",
                self.nodes.len()
            )));
        }
        let mut leaf_depth = None;
        let mut visited = vec![false; self.nodes.len()];
        let mut stack = vec![(self.root, 0_usize, true)];
        let mut arena_leaf_count = 0_usize;
        while let Some((node_id, depth, root)) = stack.pop() {
            if depth > MAX_INDEX_DEPTH {
                return Err(index_complexity_error(format!(
                    "validation path exceeds the depth limit of {MAX_INDEX_DEPTH}"
                )));
            }
            if node_id >= self.nodes.len() || visited[node_id] {
                return Err(corruption(
                    "B+Tree contains an invalid or cyclic child link",
                ));
            }
            visited[node_id] = true;
            match &self.nodes[node_id] {
                Node::Leaf { entries, .. } => {
                    arena_leaf_count = arena_leaf_count
                        .checked_add(1)
                        .ok_or_else(|| index_complexity_error("leaf count overflow"))?;
                    if !root && entries.is_empty() {
                        return Err(corruption("non-root B+Tree leaf is empty"));
                    }
                    if entries.len() > self.order {
                        return Err(corruption("B+Tree leaf exceeds configured order"));
                    }
                    if entries
                        .iter()
                        .any(|entry| Some(entry.key.len()) != self.key_width)
                    {
                        return Err(corruption("B+Tree entry key width is inconsistent"));
                    }
                    if let Some(expected) = leaf_depth {
                        if expected != depth {
                            return Err(corruption("B+Tree leaves are not at the same depth"));
                        }
                    } else {
                        leaf_depth = Some(depth);
                    }
                }
                Node::Internal { keys, children } => {
                    if children.len() != keys.len() + 1 || children.len() > self.order {
                        return Err(corruption(
                            "B+Tree internal child/separator cardinality is invalid",
                        ));
                    }
                    if !root && children.len() < 2 {
                        return Err(corruption("non-root B+Tree internal node is underfull"));
                    }
                    if keys.windows(2).any(|pair| pair[0] > pair[1]) {
                        return Err(corruption("B+Tree internal separators are not ordered"));
                    }
                    for child in children.iter().rev() {
                        stack.push((*child, depth + 1, false));
                    }
                }
            }
        }
        if visited.iter().any(|visited| !visited) {
            return Err(corruption("B+Tree contains an unreachable node"));
        }

        let mut leaf = self.leftmost_leaf();
        let mut chain_seen = vec![false; self.nodes.len()];
        let mut chain_leaf_count = 0_usize;
        let mut counted_entries = 0_usize;
        let mut previous: Option<&IndexEntry> = None;
        while let Some(node_id) = leaf {
            if node_id >= self.nodes.len() || chain_seen[node_id] {
                return Err(corruption("B+Tree leaf chain is invalid or cyclic"));
            }
            chain_seen[node_id] = true;
            let Node::Leaf { entries, next } = &self.nodes[node_id] else {
                return Err(corruption("B+Tree leaf chain points to an internal node"));
            };
            chain_leaf_count = chain_leaf_count
                .checked_add(1)
                .ok_or_else(|| index_complexity_error("leaf-chain count overflow"))?;
            for entry in entries {
                if let Some(previous) = previous {
                    let ordering = previous
                        .key
                        .cmp(&entry.key)
                        .then_with(|| previous.row_id.cmp(&entry.row_id));
                    if ordering == Ordering::Greater {
                        return Err(corruption("B+Tree leaves are not globally ordered"));
                    }
                    if self.unique && !previous.key.contains_null() && previous.key == entry.key {
                        return Err(corruption("unique B+Tree contains a duplicate key"));
                    }
                }
                previous = Some(entry);
                counted_entries = counted_entries
                    .checked_add(1)
                    .ok_or_else(|| index_complexity_error("entry count overflow"))?;
            }
            leaf = *next;
        }
        if chain_leaf_count != arena_leaf_count {
            return Err(corruption(
                "B+Tree leaf chain does not contain every reachable leaf",
            ));
        }
        if counted_entries != self.entry_count {
            return Err(corruption(format!(
                "B+Tree stored entry count {} does not match {counted_entries}",
                self.entry_count
            )));
        }
        Ok(())
    }

    fn leftmost_leaf(&self) -> Option<usize> {
        let mut node_id = self.root;
        let mut depth = 0_usize;
        loop {
            match self.nodes.get(node_id)? {
                Node::Leaf { .. } => return Some(node_id),
                Node::Internal { children, .. } => {
                    if depth >= MAX_INDEX_DEPTH {
                        return None;
                    }
                    depth += 1;
                    node_id = *children.first()?;
                }
            }
        }
    }

    fn seek_lower_bound(&self, lower: Bound<&IndexKey>) -> (Option<usize>, usize) {
        let (Bound::Included(key) | Bound::Excluded(key)) = lower else {
            return (self.leftmost_leaf(), 0);
        };
        let mut node_id = self.root;
        let mut depth = 0_usize;
        loop {
            let Some(node) = self.nodes.get(node_id) else {
                return (None, 0);
            };
            match node {
                Node::Leaf { .. } => {
                    let mut leaf = Some(node_id);
                    while let Some(leaf_id) = leaf {
                        let Some(Node::Leaf { entries, next }) = self.nodes.get(leaf_id) else {
                            return (None, 0);
                        };
                        let offset = match lower {
                            Bound::Included(_) => entries.partition_point(|entry| &entry.key < key),
                            Bound::Excluded(_) => {
                                entries.partition_point(|entry| &entry.key <= key)
                            }
                            Bound::Unbounded => 0,
                        };
                        if offset < entries.len() {
                            return (Some(leaf_id), offset);
                        }
                        leaf = *next;
                    }
                    return (None, 0);
                }
                Node::Internal { keys, children } => {
                    if depth >= MAX_INDEX_DEPTH {
                        return (None, 0);
                    }
                    depth += 1;
                    let child_index = keys.partition_point(|separator| separator < key);
                    let Some(child_id) = children.get(child_index) else {
                        return (None, 0);
                    };
                    node_id = *child_id;
                }
            }
        }
    }

    fn contains_key_fast(&self, key: &IndexKey) -> bool {
        self.get_iter(key).next().is_some()
    }
}

fn clone_bound(bound: Bound<&IndexKey>) -> Bound<IndexKey> {
    match bound {
        Bound::Included(key) => Bound::Included(key.clone()),
        Bound::Excluded(key) => Bound::Excluded(key.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn bound_ref(bound: &Bound<IndexKey>) -> Bound<&IndexKey> {
    match bound {
        Bound::Included(key) => Bound::Included(key),
        Bound::Excluded(key) => Bound::Excluded(key),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn exceeds_upper_bound(key: &IndexKey, upper: &Bound<IndexKey>) -> bool {
    match upper {
        Bound::Included(upper) => key > upper,
        Bound::Excluded(upper) => key >= upper,
        Bound::Unbounded => false,
    }
}

fn index_complexity_error(detail: impl Into<String>) -> DbError {
    DbError::new("54001", "B+Tree complexity limit exceeded")
        .with_detail(detail)
        .with_hint("Reduce index depth or rebuild the index with a supported structure.")
}

fn corruption(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use ordadb_types::{PgArray, ScalarType, TypeId, Value};

    use super::{BPlusTree, IndexEntry, IndexKey, MAX_INDEX_DEPTH, Node, RowId};

    fn entry(key: i64, row_id: u64) -> IndexEntry {
        IndexEntry::new(&[Value::Int64(key)], RowId::new(row_id), Vec::new()).expect("entry")
    }

    #[test]
    fn splits_multiple_levels_and_preserves_point_lookup() {
        let mut tree = BPlusTree::with_order(false, 3);
        for value in (0..100).rev() {
            tree.insert(entry(value, value as u64)).expect("insert");
        }
        tree.validate().expect("valid tree");
        assert!(tree.height() >= 4);
        assert_eq!(tree.len(), 100);
        let key = IndexKey::from_values(&[Value::Int64(42)]).expect("key");
        assert_eq!(tree.get(&key)[0].row_id, RowId::new(42));
    }

    #[test]
    fn range_scan_honors_bounds_across_leaf_splits() {
        let mut tree = BPlusTree::with_order(false, 3);
        for value in 0..20 {
            tree.insert(entry(value, value as u64)).expect("insert");
        }
        let lower = IndexKey::from_values(&[Value::Int64(5)]).expect("lower");
        let upper = IndexKey::from_values(&[Value::Int64(9)]).expect("upper");
        let rows = tree.range(Bound::Excluded(&lower), Bound::Included(&upper));
        assert_eq!(
            rows.iter()
                .map(|entry| entry.row_id.get())
                .collect::<Vec<_>>(),
            vec![6, 7, 8, 9]
        );
    }

    #[test]
    fn unique_indexes_allow_multiple_nulls_but_reject_values() {
        let mut tree = BPlusTree::with_order(true, 3);
        tree.insert(entry(1, 1)).expect("first value");
        let duplicate = tree.insert(entry(1, 2)).expect_err("duplicate");
        assert_eq!(duplicate.sql_state, "23505");

        let null_one =
            IndexEntry::new(&[Value::Null], RowId::new(3), Vec::new()).expect("null entry");
        let null_two =
            IndexEntry::new(&[Value::Null], RowId::new(4), Vec::new()).expect("null entry");
        tree.insert(null_one).expect("first null");
        tree.insert(null_two).expect("second null");
        tree.validate().expect("valid");
    }

    #[test]
    fn composite_and_covering_entries_are_lexicographic() {
        let mut tree = BPlusTree::with_order(false, 3);
        tree.insert(
            IndexEntry::new(
                &[Value::Int64(2), Value::Text("a".into())],
                RowId::new(2),
                vec![Value::Text("covered".into())],
            )
            .expect("entry"),
        )
        .expect("insert");
        tree.insert(
            IndexEntry::new(
                &[Value::Int64(1), Value::Text("z".into())],
                RowId::new(1),
                Vec::new(),
            )
            .expect("entry"),
        )
        .expect("insert");
        assert_eq!(tree.entries()[0].row_id, RowId::new(1));
        assert_eq!(tree.entries()[1].included.len(), 1);
    }

    #[test]
    fn enum_keys_follow_declaration_order_instead_of_label_order() {
        let enum_type = ScalarType::Enum {
            type_id: TypeId::new(5),
            labels: vec!["zeta".into(), "alpha".into()],
        };
        let key_types = [enum_type];
        let mut tree = BPlusTree::new(false);
        tree.insert(
            IndexEntry::new_typed(
                &[Value::Text("alpha".into())],
                &key_types,
                RowId::new(2),
                Vec::new(),
            )
            .expect("alpha entry"),
        )
        .expect("insert alpha");
        tree.insert(
            IndexEntry::new_typed(
                &[Value::Text("zeta".into())],
                &key_types,
                RowId::new(1),
                Vec::new(),
            )
            .expect("zeta entry"),
        )
        .expect("insert zeta");

        assert_eq!(
            tree.iter()
                .map(|entry| entry.row_id.get())
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let alpha = IndexKey::from_typed_values(&[Value::Text("alpha".into())], &key_types)
            .expect("alpha key");
        assert_eq!(tree.get(&alpha)[0].row_id, RowId::new(2));

        let invalid = IndexKey::from_typed_values(&[Value::Text("blocked".into())], &key_types)
            .expect_err("invalid enum label");
        assert_eq!(invalid.sql_state, "22P02");
    }

    #[test]
    fn array_keys_compare_elements_then_dimensions_with_enum_semantics() {
        let enum_type = ScalarType::Enum {
            type_id: TypeId::new(8),
            labels: vec!["zeta".into(), "alpha".into()],
        };
        let array_type = ScalarType::Array {
            element: Box::new(enum_type.clone()),
        };
        let key_types = [array_type];
        let key = |values| {
            let array = PgArray::one_dimensional(enum_type.clone(), values).expect("enum array");
            IndexKey::from_typed_values(&[Value::Array(array)], &key_types).expect("array key")
        };

        assert!(key(vec![Value::Text("zeta".into())]) < key(vec![Value::Text("alpha".into())]));
        assert!(key(vec![Value::Text("alpha".into())]) < key(vec![Value::Null]));
        assert!(
            key(vec![Value::Text("zeta".into())])
                < key(vec![
                    Value::Text("zeta".into()),
                    Value::Text("alpha".into()),
                ])
        );
    }

    #[test]
    fn borrowed_iterators_seek_and_cross_leaf_links() {
        let mut tree = BPlusTree::with_order(false, 3);
        for value in 0..40 {
            tree.insert(entry(value / 4, value as u64))
                .expect("insert duplicate range");
        }
        let key = IndexKey::from_values(&[Value::Int64(5)]).expect("key");
        assert_eq!(
            tree.get_iter(&key)
                .map(|entry| entry.row_id.get())
                .collect::<Vec<_>>(),
            vec![20, 21, 22, 23]
        );
        let lower = IndexKey::from_values(&[Value::Int64(3)]).expect("lower");
        let upper = IndexKey::from_values(&[Value::Int64(6)]).expect("upper");
        let rows = tree
            .range_iter(Bound::Excluded(&lower), Bound::Excluded(&upper))
            .map(|entry| entry.row_id.get())
            .collect::<Vec<_>>();
        assert_eq!(rows, (16..24).collect::<Vec<_>>());
        assert_eq!(tree.iter().count(), tree.len());
        tree.validate().expect("valid iterated tree");
    }

    #[test]
    fn owned_iterator_keeps_a_shallow_tree_snapshot_alive() {
        let mut tree = BPlusTree::with_order(false, 3);
        for value in 0..24 {
            tree.insert(entry(value / 3, value as u64))
                .expect("insert duplicate range");
        }
        let tree = Arc::new(tree);
        let lower = IndexKey::from_values(&[Value::Int64(2)]).expect("lower");
        let upper = IndexKey::from_values(&[Value::Int64(5)]).expect("upper");
        let iterator = tree.owned_range_iter(Bound::Excluded(lower), Bound::Included(upper));
        drop(tree);
        assert_eq!(
            iterator.map(|entry| entry.row_id.get()).collect::<Vec<_>>(),
            (9..18).collect::<Vec<_>>()
        );
    }

    #[test]
    fn stored_count_changes_only_after_successful_insert() {
        let mut tree = BPlusTree::with_order(true, 3);
        tree.insert(entry(7, 1)).expect("first entry");
        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());
        let error = tree.insert(entry(7, 2)).expect_err("duplicate");
        assert_eq!(error.sql_state, "23505");
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.iter().count(), 1);
        tree.validate().expect("count remains valid");
    }

    #[test]
    fn deep_index_paths_fail_with_program_limit() {
        let separator = IndexKey::from_values(&[Value::Int64(1)]).expect("separator");
        let mut nodes = vec![Node::Leaf {
            entries: Vec::new(),
            next: None,
        }];
        let mut child = 0;
        for _ in 0..=MAX_INDEX_DEPTH {
            let side_leaf = nodes.len();
            nodes.push(Node::Leaf {
                entries: Vec::new(),
                next: None,
            });
            let parent = nodes.len();
            nodes.push(Node::Internal {
                keys: vec![separator.clone()],
                children: vec![child, side_leaf],
            });
            child = parent;
        }
        let mut tree = BPlusTree {
            nodes,
            root: child,
            order: 3,
            unique: false,
            key_width: None,
            entry_count: 0,
        };

        let insert_error = tree.insert(entry(0, 0)).expect_err("depth-limited insert");
        assert_eq!(insert_error.sql_state, "54001");
        assert_eq!(tree.len(), 0);
        let validation_error = tree.validate().expect_err("depth-limited validation");
        assert_eq!(validation_error.sql_state, "54001");
    }
}
