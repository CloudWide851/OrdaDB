use std::collections::BTreeSet;
use std::sync::Arc;

use ordadb_types::{DbError, IndexId, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_DOCUMENTS: usize = 10_000_000;
pub const DEFAULT_MAX_SEARCH_INDEXES: usize = 128;
pub const DEFAULT_MAX_TOTAL_DOCUMENTS: usize = 20_000_000;
pub const DEFAULT_MAX_DOCUMENT_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_QUERY_BYTES: usize = 16_384;
pub const DEFAULT_MAX_QUERY_TERMS: usize = 1_024;
pub const DEFAULT_MAX_RESULTS: usize = 10_000;
pub const DEFAULT_MAX_VECTOR_DIMENSIONS: usize = 4_096;
pub const DEFAULT_MAX_EF_SEARCH: usize = 4_096;
pub const DEFAULT_TANTIVY_WRITER_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SearchRowId(u64);

impl SearchRowId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

pub type AllowedRows = Arc<BTreeSet<SearchRowId>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FullTextAnalyzer {
    Standard,
    Whitespace,
}

impl Default for FullTextAnalyzer {
    fn default() -> Self {
        Self::Standard
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VectorMetric {
    Cosine,
    L2,
    Dot,
}

impl Default for VectorMetric {
    fn default() -> Self {
        Self::Cosine
    }
}

impl VectorMetric {
    pub(crate) fn distance(self, left: &[f32], right: &[f32]) -> Result<f32> {
        if left.len() != right.len() {
            return Err(DbError::new(
                "22023",
                format!(
                    "vector dimension mismatch: expected {}, received {}",
                    left.len(),
                    right.len()
                ),
            ));
        }
        validate_finite_vector(left)?;
        validate_finite_vector(right)?;
        match self {
            Self::Cosine => {
                let mut dot = 0.0_f64;
                let mut left_norm = 0.0_f64;
                let mut right_norm = 0.0_f64;
                for (left, right) in left.iter().zip(right) {
                    let left = f64::from(*left);
                    let right = f64::from(*right);
                    dot += left * right;
                    left_norm += left * left;
                    right_norm += right * right;
                }
                if left_norm == 0.0 || right_norm == 0.0 {
                    return Err(DbError::new(
                        "22023",
                        "cosine distance does not accept a zero-norm vector",
                    ));
                }
                checked_f32(1.0 - dot / (left_norm.sqrt() * right_norm.sqrt()))
            }
            Self::L2 => {
                let mut squared = 0.0_f64;
                for (left, right) in left.iter().zip(right) {
                    let delta = f64::from(*left) - f64::from(*right);
                    squared += delta * delta;
                }
                checked_f32(squared.sqrt())
            }
            Self::Dot => {
                let mut dot = 0.0_f64;
                for (left, right) in left.iter().zip(right) {
                    dot += f64::from(*left) * f64::from(*right);
                }
                checked_f32(-dot)
            }
        }
    }

    pub(crate) fn similarity(self, distance: f32) -> f32 {
        match self {
            Self::Cosine => 1.0 - distance,
            Self::L2 => 1.0 / (1.0 + distance.max(0.0)),
            Self::Dot => -distance,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchLimits {
    pub max_documents: usize,
    pub max_indexes: usize,
    pub max_total_documents: usize,
    pub max_document_bytes: usize,
    pub max_query_bytes: usize,
    pub max_query_terms: usize,
    pub max_results: usize,
    pub max_vector_dimensions: usize,
    pub max_ef_search: usize,
    pub tantivy_writer_bytes: usize,
}

impl Default for SearchLimits {
    fn default() -> Self {
        Self {
            max_documents: DEFAULT_MAX_DOCUMENTS,
            max_indexes: DEFAULT_MAX_SEARCH_INDEXES,
            max_total_documents: DEFAULT_MAX_TOTAL_DOCUMENTS,
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            max_query_terms: DEFAULT_MAX_QUERY_TERMS,
            max_results: DEFAULT_MAX_RESULTS,
            max_vector_dimensions: DEFAULT_MAX_VECTOR_DIMENSIONS,
            max_ef_search: DEFAULT_MAX_EF_SEARCH,
            tantivy_writer_bytes: DEFAULT_TANTIVY_WRITER_BYTES,
        }
    }
}

impl SearchLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_documents == 0
            || self.max_indexes == 0
            || self.max_total_documents == 0
            || self.max_document_bytes == 0
            || self.max_query_bytes == 0
            || self.max_query_terms == 0
            || self.max_results == 0
            || self.max_vector_dimensions == 0
            || self.max_ef_search == 0
            || self.tantivy_writer_bytes < 15_000_000
        {
            return Err(DbError::new(
                "22023",
                "search limits must be positive and Tantivy writer memory must be at least 15000000 bytes",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_limit(&self, limit: usize) -> Result<()> {
        if limit == 0 || limit > self.max_results {
            return Err(DbError::new(
                "22023",
                format!("search limit must be between 1 and {}", self.max_results),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_query(&self, query: &str) -> Result<()> {
        if query.is_empty() {
            return Err(DbError::new("22023", "search query cannot be empty"));
        }
        if query.len() > self.max_query_bytes {
            return Err(DbError::new(
                "54000",
                format!(
                    "search query contains {} bytes, exceeding limit {}",
                    query.len(),
                    self.max_query_bytes
                ),
            ));
        }
        let terms = query.split_whitespace().count();
        if terms > self.max_query_terms {
            return Err(DbError::new(
                "54000",
                format!(
                    "search query contains {terms} terms, exceeding limit {}",
                    self.max_query_terms
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDocument {
    pub row_id: SearchRowId,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TextSearchRequest {
    pub index_id: IndexId,
    pub query: String,
    pub limit: usize,
    pub allowed_rows: Option<AllowedRows>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TextSearchHit {
    pub row_id: SearchRowId,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorRecord {
    pub row_id: SearchRowId,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct HnswConfig {
    pub seed: u64,
    pub dimensions: usize,
    pub metric: VectorMetric,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

impl HnswConfig {
    pub fn validate(&self, limits: &SearchLimits) -> Result<()> {
        limits.validate()?;
        if self.dimensions == 0 || self.dimensions > limits.max_vector_dimensions {
            return Err(DbError::new(
                "22023",
                format!(
                    "HNSW dimensions must be between 1 and {}",
                    limits.max_vector_dimensions
                ),
            ));
        }
        if !(2..=64).contains(&self.m) {
            return Err(DbError::new("22023", "HNSW m must be between 2 and 64"));
        }
        if self.ef_construction < self.m || self.ef_construction > 4_096 {
            return Err(DbError::new(
                "22023",
                "HNSW ef_construction must be at least m and at most 4096",
            ));
        }
        if self.ef_search == 0 || self.ef_search > limits.max_ef_search {
            return Err(DbError::new(
                "22023",
                format!(
                    "HNSW ef_search must be between 1 and {}",
                    limits.max_ef_search
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VectorSearchRequest {
    pub index_id: IndexId,
    pub vector: Vec<f32>,
    pub limit: usize,
    pub ef_search: Option<usize>,
    pub allowed_rows: Option<AllowedRows>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorSearchHit {
    pub row_id: SearchRowId,
    pub distance: f32,
    pub score: f32,
}

#[derive(Debug, Clone)]
pub struct HybridSearchRequest {
    pub text: TextSearchRequest,
    pub vector: VectorSearchRequest,
    pub text_weight: f32,
    pub vector_weight: f32,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HybridSearchHit {
    pub row_id: SearchRowId,
    pub text_score: f32,
    pub vector_score: f32,
    pub combined_score: f32,
}

pub(crate) fn validate_finite_vector(vector: &[f32]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(DbError::new(
            "22003",
            "vectors may contain only finite f32 values",
        ));
    }
    Ok(())
}

fn checked_f32(value: f64) -> Result<f32> {
    let value = value as f32;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(DbError::new(
            "22003",
            "vector distance overflowed finite f32 range",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchLimits, VectorMetric};

    #[test]
    fn validates_limits_and_distance_metrics() {
        SearchLimits::default().validate().expect("default limits");
        let left = [1.0, 0.0];
        let right = [0.0, 1.0];
        assert!(
            (VectorMetric::Cosine
                .distance(&left, &right)
                .expect("cosine")
                - 1.0)
                .abs()
                < 1e-6
        );
        assert!(
            (VectorMetric::L2.distance(&left, &right).expect("l2") - 2.0_f32.sqrt()).abs() < 1e-6
        );
        assert_eq!(
            VectorMetric::Dot.distance(&left, &right).expect("dot"),
            -0.0
        );
        assert_eq!(
            VectorMetric::Cosine
                .distance(&[0.0, 0.0], &right)
                .expect_err("zero norm")
                .sql_state,
            "22023"
        );
        assert_eq!(
            VectorMetric::L2
                .distance(&[f32::NAN], &[0.0])
                .expect_err("non finite")
                .sql_state,
            "22003"
        );
    }
}
