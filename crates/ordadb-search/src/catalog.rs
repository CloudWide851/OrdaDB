use std::collections::BTreeMap;
use std::sync::Arc;

use ordadb_catalog::{
    Catalog, FullTextAnalyzer as CatalogAnalyzer, IndexMethod, IndexOptions, TableDefinition,
    VectorDistanceMetric,
};
use ordadb_types::{DbError, IndexId, Result, Row, TableId, Value};

use crate::{
    FullTextAnalyzer, FullTextIndex, HnswConfig, HnswIndex, HybridSearchHit, HybridSearchRequest,
    SearchDocument, SearchLimits, SearchRowId, TextSearchHit, TextSearchRequest, VectorMetric,
    VectorRecord, VectorSearchHit, VectorSearchRequest, fuse_hybrid_hits,
};

#[derive(Debug, Clone, Default)]
pub struct SearchCatalog {
    full_text: BTreeMap<IndexId, Arc<FullTextIndex>>,
    hnsw: BTreeMap<IndexId, Arc<HnswIndex>>,
    limits: SearchLimits,
}

impl SearchCatalog {
    pub fn build(
        catalog: &Catalog,
        rows: &BTreeMap<TableId, Arc<Vec<Row>>>,
        limits: SearchLimits,
    ) -> Result<Self> {
        limits.validate()?;
        validate_catalog_limits(catalog, rows, &limits)?;
        let mut full_text = BTreeMap::new();
        let mut hnsw = BTreeMap::new();
        for schema in catalog.database().schemas() {
            for table in schema.tables() {
                let table_rows = rows.get(&table.id).map_or(&[][..], |rows| rows.as_slice());
                let built = build_table_indexes(table, table_rows, &limits)?;
                full_text.extend(built.full_text);
                hnsw.extend(built.hnsw);
            }
        }
        Ok(Self {
            full_text,
            hnsw,
            limits,
        })
    }

    pub fn rebuild_table(
        &self,
        catalog: &Catalog,
        rows: &BTreeMap<TableId, Arc<Vec<Row>>>,
        table_id: TableId,
    ) -> Result<Self> {
        self.limits.validate()?;
        validate_catalog_limits(catalog, rows, &self.limits)?;
        let mut rebuilt = self.reconciled(catalog);
        rebuilt.full_text.retain(|index_id, _| {
            catalog.index_by_id(*index_id).is_some_and(|index| {
                index.table_id != table_id && index.method == IndexMethod::FullText
            })
        });
        rebuilt.hnsw.retain(|index_id, _| {
            catalog.index_by_id(*index_id).is_some_and(|index| {
                index.table_id != table_id && index.method == IndexMethod::Hnsw
            })
        });
        if let Some(table) = catalog.table_by_id(table_id) {
            let table_rows = rows.get(&table_id).map_or(&[][..], |rows| rows.as_slice());
            let built = build_table_indexes(table, table_rows, &self.limits)?;
            rebuilt.full_text.extend(built.full_text);
            rebuilt.hnsw.extend(built.hnsw);
        }
        Ok(rebuilt)
    }

    pub fn reconcile(
        &self,
        catalog: &Catalog,
        rows: &BTreeMap<TableId, Arc<Vec<Row>>>,
    ) -> Result<Self> {
        self.limits.validate()?;
        validate_catalog_limits(catalog, rows, &self.limits)?;
        Ok(self.reconciled(catalog))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.full_text.is_empty() && self.hnsw.is_empty()
    }

    pub fn text_search(&self, request: &TextSearchRequest) -> Result<Vec<TextSearchHit>> {
        self.full_text
            .get(&request.index_id)
            .ok_or_else(|| search_index_error(request.index_id, IndexMethod::FullText))?
            .search(request)
    }

    pub fn vector_search(&self, request: &VectorSearchRequest) -> Result<Vec<VectorSearchHit>> {
        self.hnsw
            .get(&request.index_id)
            .ok_or_else(|| search_index_error(request.index_id, IndexMethod::Hnsw))?
            .search(request)
    }

    pub fn hybrid_search(&self, request: &HybridSearchRequest) -> Result<Vec<HybridSearchHit>> {
        let text = self.text_search(&request.text)?;
        let vector = self.vector_search(&request.vector)?;
        fuse_hybrid_hits(request, &text, &vector, &self.limits)
    }

    fn reconciled(&self, catalog: &Catalog) -> Self {
        let mut full_text = self.full_text.clone();
        full_text.retain(|index_id, _| {
            catalog
                .index_by_id(*index_id)
                .is_some_and(|index| index.method == IndexMethod::FullText)
        });
        let mut hnsw = self.hnsw.clone();
        hnsw.retain(|index_id, _| {
            catalog
                .index_by_id(*index_id)
                .is_some_and(|index| index.method == IndexMethod::Hnsw)
        });
        Self {
            full_text,
            hnsw,
            limits: self.limits.clone(),
        }
    }
}

#[derive(Default)]
struct TableSearchIndexes {
    full_text: BTreeMap<IndexId, Arc<FullTextIndex>>,
    hnsw: BTreeMap<IndexId, Arc<HnswIndex>>,
}

fn build_table_indexes(
    table: &TableDefinition,
    table_rows: &[Row],
    limits: &SearchLimits,
) -> Result<TableSearchIndexes> {
    let mut built = TableSearchIndexes::default();
    for definition in table.indexes() {
        if definition.method != definition.options.method() {
            return Err(DbError::new(
                "22023",
                format!(
                    "index {} method and options do not describe the same index kind",
                    definition.name
                ),
            ));
        }
        match &definition.options {
            IndexOptions::BTree => {}
            IndexOptions::FullText { analyzer } => {
                let positions = definition
                    .key_columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            DbError::internal("full-text index column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let documents = table_rows
                    .iter()
                    .enumerate()
                    .map(|(row_index, row)| {
                        Ok(SearchDocument {
                            row_id: search_row_id(row_index)?,
                            fields: positions
                                .iter()
                                .map(|position| search_text_value(&row.values[*position]))
                                .collect::<Result<Vec<_>>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let analyzer = match analyzer {
                    CatalogAnalyzer::Standard => FullTextAnalyzer::Standard,
                    CatalogAnalyzer::Whitespace => FullTextAnalyzer::Whitespace,
                };
                let index =
                    FullTextIndex::build(positions.len(), analyzer, &documents, limits.clone())?;
                built.full_text.insert(definition.id, Arc::new(index));
            }
            IndexOptions::Hnsw {
                metric,
                dimensions,
                m,
                ef_construction,
                ef_search,
            } => {
                let [column_id] = definition.key_columns.as_slice() else {
                    return Err(DbError::new(
                        "22023",
                        format!("HNSW index {} requires one column", definition.name),
                    ));
                };
                let position = table.column_index_by_id(*column_id).ok_or_else(|| {
                    DbError::internal("HNSW index column is absent from its table")
                })?;
                let records = table_rows
                    .iter()
                    .enumerate()
                    .filter_map(|(row_index, row)| match row.values.get(position) {
                        Some(Value::Null) => None,
                        Some(Value::Vector(vector)) => {
                            Some(search_row_id(row_index).map(|row_id| VectorRecord {
                                row_id,
                                vector: vector.clone(),
                            }))
                        }
                        Some(_) => Some(Err(DbError::new(
                            "42804",
                            format!(
                                "HNSW index {} encountered a non-vector value",
                                definition.name
                            ),
                        ))),
                        None => Some(Err(DbError::internal(
                            "HNSW row is narrower than its table definition",
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()?;
                let metric = match metric {
                    VectorDistanceMetric::Cosine => VectorMetric::Cosine,
                    VectorDistanceMetric::L2 => VectorMetric::L2,
                    VectorDistanceMetric::Dot => VectorMetric::Dot,
                };
                let index = HnswIndex::build(
                    HnswConfig {
                        seed: definition.id.get(),
                        dimensions: *dimensions,
                        metric,
                        m: *m,
                        ef_construction: *ef_construction,
                        ef_search: *ef_search,
                    },
                    &records,
                    limits.clone(),
                )?;
                built.hnsw.insert(definition.id, Arc::new(index));
            }
        }
    }
    Ok(built)
}

fn validate_catalog_limits(
    catalog: &Catalog,
    rows: &BTreeMap<TableId, Arc<Vec<Row>>>,
    limits: &SearchLimits,
) -> Result<()> {
    let mut index_count = 0_usize;
    let mut document_count = 0_usize;
    for schema in catalog.database().schemas() {
        for table in schema.tables() {
            let table_rows = rows.get(&table.id).map_or(0, |rows| rows.len());
            for index in table
                .indexes()
                .filter(|index| index.method != IndexMethod::BTree)
            {
                index_count = index_count
                    .checked_add(1)
                    .ok_or_else(|| DbError::new("54000", "search index count overflow"))?;
                document_count = document_count
                    .checked_add(table_rows)
                    .ok_or_else(|| DbError::new("54000", "search document count overflow"))?;
                if index_count > limits.max_indexes {
                    return Err(DbError::new(
                        "54000",
                        format!(
                            "search catalog contains {index_count} indexes, exceeding limit {}",
                            limits.max_indexes
                        ),
                    ));
                }
                if document_count > limits.max_total_documents {
                    return Err(DbError::new(
                        "54000",
                        format!(
                            "search catalog contains {document_count} indexed row references, exceeding limit {}",
                            limits.max_total_documents
                        ),
                    ));
                }
                if index.method != index.options.method() {
                    return Err(DbError::new(
                        "22023",
                        format!(
                            "index {} method and options do not describe the same index kind",
                            index.name
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn search_row_id(row_index: usize) -> Result<SearchRowId> {
    u64::try_from(row_index)
        .map(SearchRowId::new)
        .map_err(|_| DbError::new("54000", "table row count exceeds search index limits"))
}

fn search_text_value(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Text(value) => Ok(value.clone()),
        _ => Err(DbError::new(
            "42804",
            "full-text index encountered a non-character value",
        )),
    }
}

fn search_index_error(index_id: IndexId, method: IndexMethod) -> DbError {
    DbError::new(
        "42704",
        format!(
            "{method:?} search index {} does not exist in the committed snapshot",
            index_id.get()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_catalog::{
        DropBehavior, FullTextAnalyzer as CatalogFullTextAnalyzer, NewColumn, NewIndex,
    };
    use ordadb_types::{Identifier, ScalarType};

    fn add_text_table(
        catalog: &mut Catalog,
        table_name: &str,
        index_name: &str,
    ) -> (TableId, IndexId) {
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted(table_name),
                vec![NewColumn::new(
                    Identifier::unquoted("body"),
                    ScalarType::Text,
                )],
            )
            .expect("create table");
        let index_id = catalog
            .create_index(
                table_id,
                NewIndex {
                    name: Identifier::unquoted(index_name),
                    key_columns: vec![Identifier::unquoted("body")],
                    include_columns: Vec::new(),
                    unique: false,
                    method: IndexMethod::FullText,
                    options: IndexOptions::FullText {
                        analyzer: CatalogFullTextAnalyzer::Standard,
                    },
                },
            )
            .expect("create index");
        (table_id, index_id)
    }

    fn text_rows(value: &str) -> Arc<Vec<Row>> {
        Arc::new(vec![Row::new(vec![Value::Text(value.to_owned())])])
    }

    #[test]
    fn rebuilding_one_table_reuses_unaffected_index_snapshots() {
        let mut catalog = Catalog::default();
        let (changed_table, changed_index) =
            add_text_table(&mut catalog, "changed_docs", "changed_docs_fts");
        let (stable_table, stable_index) =
            add_text_table(&mut catalog, "stable_docs", "stable_docs_fts");
        let mut rows = BTreeMap::from([
            (changed_table, text_rows("before")),
            (stable_table, text_rows("stable")),
        ]);
        let original =
            SearchCatalog::build(&catalog, &rows, SearchLimits::default()).expect("build");
        let changed_before = Arc::clone(
            original
                .full_text
                .get(&changed_index)
                .expect("changed index"),
        );
        let stable_before =
            Arc::clone(original.full_text.get(&stable_index).expect("stable index"));

        rows.insert(changed_table, text_rows("after"));
        let rebuilt = original
            .rebuild_table(&catalog, &rows, changed_table)
            .expect("rebuild table");

        assert!(!Arc::ptr_eq(
            &changed_before,
            rebuilt
                .full_text
                .get(&changed_index)
                .expect("rebuilt changed index")
        ));
        assert!(Arc::ptr_eq(
            &stable_before,
            rebuilt
                .full_text
                .get(&stable_index)
                .expect("reused stable index")
        ));
        let hits = rebuilt
            .text_search(&TextSearchRequest {
                index_id: changed_index,
                query: "after".to_owned(),
                limit: 1,
                allowed_rows: None,
            })
            .expect("search rebuilt index");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn reconcile_drops_stale_indexes_without_rebuilding_survivors() {
        let mut catalog = Catalog::default();
        let (removed_table, removed_index) =
            add_text_table(&mut catalog, "removed_docs", "removed_docs_fts");
        let (stable_table, stable_index) =
            add_text_table(&mut catalog, "stable_docs", "stable_docs_fts");
        let rows = BTreeMap::from([
            (removed_table, text_rows("removed")),
            (stable_table, text_rows("stable")),
        ]);
        let original =
            SearchCatalog::build(&catalog, &rows, SearchLimits::default()).expect("build");
        let stable_before =
            Arc::clone(original.full_text.get(&stable_index).expect("stable index"));
        catalog
            .drop_index(removed_index, DropBehavior::Restrict)
            .expect("drop index");

        let reconciled = original.reconcile(&catalog, &rows).expect("reconcile");

        assert!(!reconciled.full_text.contains_key(&removed_index));
        assert!(Arc::ptr_eq(
            &stable_before,
            reconciled
                .full_text
                .get(&stable_index)
                .expect("reused stable index")
        ));
    }

    #[test]
    fn catalog_build_enforces_global_index_and_document_limits() {
        let mut catalog = Catalog::default();
        let (first_table, _) = add_text_table(&mut catalog, "first_docs", "first_docs_fts");
        let (second_table, _) = add_text_table(&mut catalog, "second_docs", "second_docs_fts");
        let rows = BTreeMap::from([
            (first_table, text_rows("first")),
            (second_table, text_rows("second")),
        ]);

        let index_error = SearchCatalog::build(
            &catalog,
            &rows,
            SearchLimits {
                max_indexes: 1,
                ..SearchLimits::default()
            },
        )
        .expect_err("index limit");
        assert_eq!(index_error.sql_state, "54000");

        let document_error = SearchCatalog::build(
            &catalog,
            &rows,
            SearchLimits {
                max_total_documents: 1,
                ..SearchLimits::default()
            },
        )
        .expect_err("document limit");
        assert_eq!(document_error.sql_state, "54000");
    }
}
