//! Ordered secondary-index primitives for OrdaDB.
//!
//! The tree deliberately owns no page or transaction types. Storage persists
//! sorted [`IndexEntry`] values, and the transaction layer will later decide
//! when those entries become visible.

use std::cmp::Ordering;
use std::ops::Bound;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_types::{DbError, Result, Value};
use rust_decimal::Decimal;
use uuid::Uuid;

const DEFAULT_ORDER: usize = 32;

#[derive(Debug, Clone)]
pub enum IndexScalar {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    Decimal(Decimal),
    Text(String),
    Binary(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    Uuid(Uuid),
}

impl IndexScalar {
    pub fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Boolean(value) => Ok(Self::Boolean(*value)),
            Value::Int16(value) => Ok(Self::Int16(*value)),
            Value::Int32(value) => Ok(Self::Int32(*value)),
            Value::Int64(value) => Ok(Self::Int64(*value)),
            Value::Float32(value) => Ok(Self::Float32(value.to_bits())),
            Value::Float64(value) => Ok(Self::Float64(value.to_bits())),
            Value::Decimal(value) => Ok(Self::Decimal(*value)),
            Value::Text(value) => Ok(Self::Text(value.clone())),
            Value::Binary(value) => Ok(Self::Binary(value.clone())),
            Value::Date(value) => Ok(Self::Date(*value)),
            Value::Time(value) => Ok(Self::Time(*value)),
            Value::Timestamp(value) => Ok(Self::Timestamp(*value)),
            Value::Uuid(value) => Ok(Self::Uuid(*value)),
            Value::Json(_) | Value::Jsonb(_) | Value::Vector(_) => {
                Err(DbError::new("42804", "value type has no B+Tree ordering"))
            }
        }
    }

    #[must_use]
    pub fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Boolean(value) => Value::Boolean(*value),
            Self::Int16(value) => Value::Int16(*value),
            Self::Int32(value) => Value::Int32(*value),
            Self::Int64(value) => Value::Int64(*value),
            Self::Float32(value) => Value::Float32(f32::from_bits(*value)),
            Self::Float64(value) => Value::Float64(f64::from_bits(*value)),
            Self::Decimal(value) => Value::Decimal(*value),
            Self::Text(value) => Value::Text(value.clone()),
            Self::Binary(value) => Value::Binary(value.clone()),
            Self::Date(value) => Value::Date(*value),
            Self::Time(value) => Value::Time(*value),
            Self::Timestamp(value) => Value::Timestamp(*value),
            Self::Uuid(value) => Value::Uuid(*value),
        }
    }

    const fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Int16(_) => 2,
            Self::Int32(_) => 3,
            Self::Int64(_) => 4,
            Self::Float32(_) => 5,
            Self::Float64(_) => 6,
            Self::Decimal(_) => 7,
            Self::Text(_) => 8,
            Self::Binary(_) => 9,
            Self::Date(_) => 10,
            Self::Time(_) => 11,
            Self::Timestamp(_) => 12,
            Self::Uuid(_) => 13,
        }
    }
}

impl PartialEq for IndexScalar {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for IndexScalar {}

impl PartialOrd for IndexScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexScalar {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.rank() != other.rank() {
            return self.rank().cmp(&other.rank());
        }
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::Int16(left), Self::Int16(right)) => left.cmp(right),
            (Self::Int32(left), Self::Int32(right)) => left.cmp(right),
            (Self::Int64(left), Self::Int64(right)) => left.cmp(right),
            (Self::Float32(left), Self::Float32(right)) => {
                f32::from_bits(*left).total_cmp(&f32::from_bits(*right))
            }
            (Self::Float64(left), Self::Float64(right)) => {
                f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
            }
            (Self::Decimal(left), Self::Decimal(right)) => left.cmp(right),
            (Self::Text(left), Self::Text(right)) => left.cmp(right),
            (Self::Binary(left), Self::Binary(right)) => left.cmp(right),
            (Self::Date(left), Self::Date(right)) => left.cmp(right),
            (Self::Time(left), Self::Time(right)) => left.cmp(right),
            (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
            (Self::Uuid(left), Self::Uuid(right)) => left.cmp(right),
            _ => Ordering::Equal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexKey(Vec<IndexScalar>);

impl IndexKey {
    pub fn from_values(values: &[Value]) -> Result<Self> {
        values
            .iter()
            .map(IndexScalar::from_value)
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }

    #[must_use]
    pub fn values(&self) -> Vec<Value> {
        self.0.iter().map(IndexScalar::to_value).collect()
    }

    #[must_use]
    pub fn contains_null(&self) -> bool {
        self.0
            .iter()
            .any(|value| matches!(value, IndexScalar::Null))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(u64);

impl RowId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    pub key: IndexKey,
    pub row_id: RowId,
    pub included: Vec<Value>,
}

impl IndexEntry {
    pub fn new(key_values: &[Value], row_id: RowId, included: Vec<Value>) -> Result<Self> {
        let key = IndexKey::from_values(key_values)?;
        if key.is_empty() {
            return Err(DbError::new("22023", "index key cannot be empty"));
        }
        Ok(Self {
            key,
            row_id,
            included,
        })
    }
}

#[derive(Debug, Clone)]
enum Node {
    Leaf {
        entries: Vec<IndexEntry>,
        next: Option<usize>,
    },
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<usize>,
    },
}

#[derive(Debug, Clone)]
pub struct BPlusTree {
    nodes: Vec<Node>,
    root: usize,
    order: usize,
    unique: bool,
    key_width: Option<usize>,
}

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
            None => self.key_width = Some(entry.key.len()),
            _ => {}
        }
        if self.unique && !entry.key.contains_null() && self.contains_key_fast(&entry.key) {
            return Err(DbError::new("23505", "duplicate key violates unique index"));
        }

        if let Some((separator, right)) = self.insert_node(self.root, entry)? {
            let old_root = self.root;
            self.nodes.push(Node::Internal {
                keys: vec![separator],
                children: vec![old_root, right],
            });
            self.root = self.nodes.len() - 1;
        }
        Ok(())
    }

    fn insert_node(
        &mut self,
        node_id: usize,
        entry: IndexEntry,
    ) -> Result<Option<(IndexKey, usize)>> {
        match &self.nodes[node_id] {
            Node::Leaf { .. } => self.insert_leaf(node_id, entry),
            Node::Internal { keys, .. } => {
                let child_index = keys.partition_point(|key| &entry.key >= key);
                let child_id = match &self.nodes[node_id] {
                    Node::Internal { children, .. } => children[child_index],
                    Node::Leaf { .. } => unreachable!(),
                };
                let Some((separator, right)) = self.insert_node(child_id, entry)? else {
                    return Ok(None);
                };
                let Node::Internal { keys, children } = &mut self.nodes[node_id] else {
                    return Err(DbError::internal("B+Tree node changed kind during insert"));
                };
                keys.insert(child_index, separator);
                children.insert(child_index + 1, right);
                if children.len() <= self.order {
                    return Ok(None);
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
                Ok(Some((promoted, self.nodes.len() - 1)))
            }
        }
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
        self.entries()
            .into_iter()
            .filter(|entry| &entry.key == key)
            .collect()
    }

    #[must_use]
    pub fn range(&self, lower: Bound<&IndexKey>, upper: Bound<&IndexKey>) -> Vec<&IndexEntry> {
        self.entries()
            .into_iter()
            .filter(|entry| {
                let lower_matches = match lower {
                    Bound::Included(key) => entry.key >= *key,
                    Bound::Excluded(key) => entry.key > *key,
                    Bound::Unbounded => true,
                };
                let upper_matches = match upper {
                    Bound::Included(key) => entry.key <= *key,
                    Bound::Excluded(key) => entry.key < *key,
                    Bound::Unbounded => true,
                };
                lower_matches && upper_matches
            })
            .collect()
    }

    #[must_use]
    pub fn entries(&self) -> Vec<&IndexEntry> {
        let mut entries = Vec::new();
        let mut leaf = self.leftmost_leaf();
        while let Some(node_id) = leaf {
            match &self.nodes[node_id] {
                Node::Leaf {
                    entries: values,
                    next,
                } => {
                    entries.extend(values);
                    leaf = *next;
                }
                Node::Internal { .. } => break,
            }
        }
        entries
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
        self.entries().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn validate(&self) -> Result<()> {
        if self.root >= self.nodes.len() {
            return Err(corruption("B+Tree root points outside the node arena"));
        }
        let mut leaf_depth = None;
        let mut visited = vec![false; self.nodes.len()];
        self.validate_node(self.root, 0, true, &mut leaf_depth, &mut visited)?;
        if visited.iter().any(|visited| !visited) {
            return Err(corruption("B+Tree contains an unreachable node"));
        }

        let entries = self.entries();
        for pair in entries.windows(2) {
            let ordering = pair[0]
                .key
                .cmp(&pair[1].key)
                .then_with(|| pair[0].row_id.cmp(&pair[1].row_id));
            if ordering == Ordering::Greater {
                return Err(corruption("B+Tree leaves are not globally ordered"));
            }
            if self.unique && !pair[0].key.contains_null() && pair[0].key == pair[1].key {
                return Err(corruption("unique B+Tree contains a duplicate key"));
            }
        }
        Ok(())
    }

    fn validate_node(
        &self,
        node_id: usize,
        depth: usize,
        root: bool,
        leaf_depth: &mut Option<usize>,
        visited: &mut [bool],
    ) -> Result<()> {
        if node_id >= self.nodes.len() || visited[node_id] {
            return Err(corruption(
                "B+Tree contains an invalid or cyclic child link",
            ));
        }
        visited[node_id] = true;
        match &self.nodes[node_id] {
            Node::Leaf { entries, .. } => {
                if !root && entries.is_empty() {
                    return Err(corruption("non-root B+Tree leaf is empty"));
                }
                if entries.len() > self.order {
                    return Err(corruption("B+Tree leaf exceeds configured order"));
                }
                if let Some(expected) = leaf_depth {
                    if *expected != depth {
                        return Err(corruption("B+Tree leaves are not at the same depth"));
                    }
                } else {
                    *leaf_depth = Some(depth);
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
                for child in children {
                    self.validate_node(*child, depth + 1, false, leaf_depth, visited)?;
                }
            }
        }
        Ok(())
    }

    fn leftmost_leaf(&self) -> Option<usize> {
        let mut node_id = self.root;
        loop {
            match self.nodes.get(node_id)? {
                Node::Leaf { .. } => return Some(node_id),
                Node::Internal { children, .. } => node_id = *children.first()?,
            }
        }
    }

    fn contains_key_fast(&self, key: &IndexKey) -> bool {
        let mut node_id = self.root;
        loop {
            match &self.nodes[node_id] {
                Node::Leaf { entries, .. } => {
                    return entries.binary_search_by(|entry| entry.key.cmp(key)).is_ok();
                }
                Node::Internal { keys, children } => {
                    node_id = children[keys.partition_point(|separator| key >= separator)];
                }
            }
        }
    }
}

fn corruption(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use ordadb_types::Value;

    use super::{BPlusTree, IndexEntry, IndexKey, RowId};

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
}
