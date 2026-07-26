use std::collections::BTreeMap;

use ordadb_types::{DbError, Result};

use crate::types::{
    HybridSearchHit, HybridSearchRequest, SearchLimits, SearchRowId, TextSearchHit, VectorSearchHit,
};

pub fn fuse_hybrid_hits(
    request: &HybridSearchRequest,
    text_hits: &[TextSearchHit],
    vector_hits: &[VectorSearchHit],
    limits: &SearchLimits,
) -> Result<Vec<HybridSearchHit>> {
    limits.validate_limit(request.limit)?;
    if !request.text_weight.is_finite()
        || !request.vector_weight.is_finite()
        || request.text_weight < 0.0
        || request.vector_weight < 0.0
    {
        return Err(DbError::new(
            "22023",
            "hybrid weights must be finite and non-negative",
        ));
    }
    let weight_sum = request.text_weight + request.vector_weight;
    if !weight_sum.is_finite() || weight_sum <= 0.0 {
        return Err(DbError::new(
            "22023",
            "hybrid weight sum must be finite and greater than zero",
        ));
    }
    let text_weight = request.text_weight / weight_sum;
    let vector_weight = request.vector_weight / weight_sum;
    let (text_min, text_max) = score_bounds(text_hits.iter().map(|hit| hit.score));
    let (vector_min, vector_max) = score_bounds(vector_hits.iter().map(|hit| hit.score));
    let mut hits = BTreeMap::<SearchRowId, HybridSearchHit>::new();
    for hit in text_hits {
        let normalized = normalize(hit.score, text_min, text_max);
        let entry = hits.entry(hit.row_id).or_insert(HybridSearchHit {
            row_id: hit.row_id,
            text_score: 0.0,
            vector_score: 0.0,
            combined_score: 0.0,
        });
        entry.text_score = hit.score;
        entry.combined_score += text_weight * normalized;
    }
    for hit in vector_hits {
        let normalized = normalize(hit.score, vector_min, vector_max);
        let entry = hits.entry(hit.row_id).or_insert(HybridSearchHit {
            row_id: hit.row_id,
            text_score: 0.0,
            vector_score: 0.0,
            combined_score: 0.0,
        });
        entry.vector_score = hit.score;
        entry.combined_score += vector_weight * normalized;
    }
    let mut hits = hits.into_values().collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .combined_score
            .total_cmp(&left.combined_score)
            .then_with(|| right.text_score.total_cmp(&left.text_score))
            .then_with(|| right.vector_score.total_cmp(&left.vector_score))
            .then_with(|| left.row_id.cmp(&right.row_id))
    });
    hits.truncate(request.limit);
    Ok(hits)
}

fn score_bounds(scores: impl Iterator<Item = f32>) -> (Option<f32>, Option<f32>) {
    scores.fold((None, None), |(minimum, maximum), score| {
        (
            Some(minimum.map_or(score, |value: f32| value.min(score))),
            Some(maximum.map_or(score, |value: f32| value.max(score))),
        )
    })
}

fn normalize(score: f32, minimum: Option<f32>, maximum: Option<f32>) -> f32 {
    match (minimum, maximum) {
        (Some(minimum), Some(maximum)) if maximum > minimum => {
            (score - minimum) / (maximum - minimum)
        }
        (Some(_), Some(_)) => 1.0,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::fuse_hybrid_hits;
    use crate::{
        HybridSearchRequest, SearchLimits, SearchRowId, TextSearchHit, TextSearchRequest,
        VectorSearchHit, VectorSearchRequest,
    };

    fn request() -> HybridSearchRequest {
        HybridSearchRequest {
            text: TextSearchRequest {
                index_id: ordadb_types::IndexId::new(1),
                query: "database".to_owned(),
                limit: 10,
                allowed_rows: None,
            },
            vector: VectorSearchRequest {
                index_id: ordadb_types::IndexId::new(2),
                vector: vec![1.0, 0.0],
                limit: 10,
                ef_search: None,
                allowed_rows: None,
            },
            text_weight: 0.6,
            vector_weight: 0.4,
            limit: 3,
        }
    }

    #[test]
    fn fuses_modalities_with_stable_order() {
        let text = [
            TextSearchHit {
                row_id: SearchRowId::new(2),
                score: 4.0,
            },
            TextSearchHit {
                row_id: SearchRowId::new(1),
                score: 2.0,
            },
        ];
        let vectors = [
            VectorSearchHit {
                row_id: SearchRowId::new(1),
                distance: 0.0,
                score: 1.0,
            },
            VectorSearchHit {
                row_id: SearchRowId::new(3),
                distance: 0.5,
                score: 0.5,
            },
        ];
        let hits =
            fuse_hybrid_hits(&request(), &text, &vectors, &SearchLimits::default()).expect("fuse");
        assert_eq!(
            hits.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
            [
                SearchRowId::new(2),
                SearchRowId::new(1),
                SearchRowId::new(3)
            ]
        );
    }

    #[test]
    fn rejects_invalid_weights() {
        let mut request = request();
        request.text_weight = f32::NAN;
        assert_eq!(
            fuse_hybrid_hits(&request, &[], &[], &SearchLimits::default())
                .expect_err("invalid")
                .sql_state,
            "22023"
        );
        request.text_weight = 0.0;
        request.vector_weight = 0.0;
        assert_eq!(
            fuse_hybrid_hits(&request, &[], &[], &SearchLimits::default())
                .expect_err("zero")
                .sql_state,
            "22023"
        );
    }
}
