use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ordadb_types::{DbError, Result};
use tantivy::collector::{FilterCollector, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{
    FAST, Field, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions,
    Value as TantivyValue,
};
use tantivy::tokenizer::{LowerCaser, TextAnalyzer, WhitespaceTokenizer};
use tantivy::{Index, IndexReader, TantivyDocument};

use crate::types::{
    FullTextAnalyzer, SearchDocument, SearchLimits, SearchRowId, TextSearchHit, TextSearchRequest,
};

const ROW_ID_FIELD: &str = "ordadb_row_id";
const WHITESPACE_TOKENIZER: &str = "ordadb_whitespace";
const MAX_TEXT_FIELDS: usize = 16;

pub struct FullTextIndex {
    index: Index,
    reader: IndexReader,
    row_id_field: Field,
    search_fields: Vec<Field>,
    limits: SearchLimits,
    documents: usize,
}

impl fmt::Debug for FullTextIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FullTextIndex")
            .field("documents", &self.documents)
            .field("fields", &self.search_fields.len())
            .finish_non_exhaustive()
    }
}

impl FullTextIndex {
    pub fn build(
        field_count: usize,
        analyzer: FullTextAnalyzer,
        documents: &[SearchDocument],
        limits: SearchLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if field_count == 0 || field_count > MAX_TEXT_FIELDS {
            return Err(DbError::new(
                "22023",
                format!("full-text field count must be between 1 and {MAX_TEXT_FIELDS}"),
            ));
        }
        if documents.len() > limits.max_documents {
            return Err(DbError::new(
                "54000",
                format!(
                    "full-text index contains {} documents, exceeding limit {}",
                    documents.len(),
                    limits.max_documents
                ),
            ));
        }

        let tokenizer = match analyzer {
            FullTextAnalyzer::Standard => "default",
            FullTextAnalyzer::Whitespace => WHITESPACE_TOKENIZER,
        };
        let indexing = TextFieldIndexing::default()
            .set_tokenizer(tokenizer)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default().set_indexing_options(indexing);
        let mut builder = Schema::builder();
        let row_id_field = builder.add_u64_field(ROW_ID_FIELD, FAST | STORED);
        let field_names = (0..field_count)
            .map(|position| format!("field_{position}"))
            .collect::<Vec<_>>();
        let search_fields = field_names
            .iter()
            .map(|name| builder.add_text_field(name, text_options.clone()))
            .collect::<Vec<_>>();
        let index = Index::create_in_ram(builder.build());
        if analyzer == FullTextAnalyzer::Whitespace {
            let tokenizer = TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .build();
            index.tokenizers().register(WHITESPACE_TOKENIZER, tokenizer);
        }

        let mut seen = BTreeSet::new();
        let mut writer = index
            .writer_with_num_threads(1, limits.tantivy_writer_bytes)
            .map_err(tantivy_error("failed to create full-text writer"))?;
        let mut ordered_documents = documents.iter().collect::<Vec<_>>();
        ordered_documents.sort_by_key(|document| document.row_id);
        for document in ordered_documents {
            if !seen.insert(document.row_id) {
                return Err(DbError::new(
                    "22023",
                    format!("duplicate search row ID {}", document.row_id.get()),
                ));
            }
            if document.fields.len() != field_count {
                return Err(DbError::new(
                    "22023",
                    format!(
                        "search document {} has {} fields, expected {field_count}",
                        document.row_id.get(),
                        document.fields.len()
                    ),
                ));
            }
            let document_bytes = document.fields.iter().try_fold(0_usize, |total, field| {
                total
                    .checked_add(field.len())
                    .ok_or_else(|| DbError::new("54000", "full-text document size overflow"))
            })?;
            if document_bytes > limits.max_document_bytes {
                return Err(DbError::new(
                    "54000",
                    format!(
                        "search document {} contains {document_bytes} bytes, exceeding limit {}",
                        document.row_id.get(),
                        limits.max_document_bytes
                    ),
                ));
            }
            let mut tantivy_document = TantivyDocument::default();
            tantivy_document.add_u64(row_id_field, document.row_id.get());
            for (field, value) in search_fields.iter().zip(&document.fields) {
                tantivy_document.add_text(*field, value);
            }
            writer
                .add_document(tantivy_document)
                .map_err(tantivy_error("failed to add full-text document"))?;
        }
        writer
            .commit()
            .map_err(tantivy_error("failed to commit full-text index"))?;
        let reader = index
            .reader()
            .map_err(tantivy_error("failed to open full-text reader"))?;
        Ok(Self {
            index,
            reader,
            row_id_field,
            search_fields,
            limits,
            documents: documents.len(),
        })
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.documents
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.documents == 0
    }

    pub fn search(&self, request: &TextSearchRequest) -> Result<Vec<TextSearchHit>> {
        self.limits.validate_limit(request.limit)?;
        self.limits.validate_query(&request.query)?;
        if request
            .allowed_rows
            .as_ref()
            .is_some_and(|allowed| allowed.is_empty())
        {
            return Ok(Vec::new());
        }
        let parser = QueryParser::for_index(&self.index, self.search_fields.clone());
        let query = parser.parse_query(&request.query).map_err(|error| {
            DbError::new("42601", "invalid full-text query").with_detail(error.to_string())
        })?;
        let searcher = self.reader.searcher();
        let top_docs = if let Some(allowed) = &request.allowed_rows {
            let allowed = Arc::clone(allowed);
            let collector = FilterCollector::new(
                ROW_ID_FIELD.to_owned(),
                move |row_id: u64| allowed.contains(&SearchRowId::new(row_id)),
                TopDocs::with_limit(request.limit).order_by_score(),
            );
            searcher
                .search(&query, &collector)
                .map_err(tantivy_error("full-text search failed"))?
        } else {
            searcher
                .search(&query, &TopDocs::with_limit(request.limit).order_by_score())
                .map_err(tantivy_error("full-text search failed"))?
        };

        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, address) in top_docs {
            let document: TantivyDocument = searcher
                .doc(address)
                .map_err(tantivy_error("failed to read full-text result"))?;
            let row_id = document
                .get_first(self.row_id_field)
                .and_then(|value| value.as_u64())
                .ok_or_else(|| DbError::internal("full-text result is missing its row ID"))?;
            hits.push(TextSearchHit {
                row_id: SearchRowId::new(row_id),
                score,
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        Ok(hits)
    }
}

fn tantivy_error(context: &'static str) -> impl FnOnce(tantivy::TantivyError) -> DbError {
    move |error| {
        DbError::new("XX000", context)
            .with_detail(error.to_string())
            .with_hint("Rebuild the derived search index from committed heap rows.")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::FullTextIndex;
    use crate::{FullTextAnalyzer, SearchDocument, SearchLimits, SearchRowId, TextSearchRequest};

    fn documents() -> Vec<SearchDocument> {
        vec![
            SearchDocument {
                row_id: SearchRowId::new(1),
                fields: vec!["reliable database engine".to_owned()],
            },
            SearchDocument {
                row_id: SearchRowId::new(2),
                fields: vec!["database query planner".to_owned()],
            },
            SearchDocument {
                row_id: SearchRowId::new(3),
                fields: vec!["vector search engine".to_owned()],
            },
        ]
    }

    #[test]
    fn searches_phrases_with_stable_row_ids_and_prefilter() {
        let index = FullTextIndex::build(
            1,
            FullTextAnalyzer::Standard,
            &documents(),
            SearchLimits::default(),
        )
        .expect("build");
        let hits = index
            .search(&TextSearchRequest {
                index_id: ordadb_types::IndexId::new(1),
                query: "\"database engine\"".to_owned(),
                limit: 3,
                allowed_rows: None,
            })
            .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_id, SearchRowId::new(1));

        let allowed = Arc::new(BTreeSet::from([SearchRowId::new(2)]));
        let filtered = index
            .search(&TextSearchRequest {
                index_id: ordadb_types::IndexId::new(1),
                query: "database".to_owned(),
                limit: 3,
                allowed_rows: Some(allowed),
            })
            .expect("filtered");
        assert_eq!(
            filtered.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
            [SearchRowId::new(2)]
        );
    }

    #[test]
    fn rejects_duplicate_rows_and_bounded_queries() {
        let mut duplicated = documents();
        duplicated.push(duplicated[0].clone());
        assert_eq!(
            FullTextIndex::build(
                1,
                FullTextAnalyzer::Whitespace,
                &duplicated,
                SearchLimits::default()
            )
            .expect_err("duplicate")
            .sql_state,
            "22023"
        );
        let limits = SearchLimits {
            max_query_bytes: 4,
            ..SearchLimits::default()
        };
        let index = FullTextIndex::build(1, FullTextAnalyzer::Standard, &documents(), limits)
            .expect("build");
        assert_eq!(
            index
                .search(&TextSearchRequest {
                    index_id: ordadb_types::IndexId::new(1),
                    query: "database".to_owned(),
                    limit: 1,
                    allowed_rows: None,
                })
                .expect_err("bounded")
                .sql_state,
            "54000"
        );
    }
}
