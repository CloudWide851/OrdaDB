use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};

use ordadb_types::{DbError, Result};

use crate::types::{
    HnswConfig, SearchLimits, SearchRowId, VectorRecord, VectorSearchHit, VectorSearchRequest,
    validate_finite_vector,
};

const MAX_HNSW_LEVEL: usize = 32;

#[derive(Debug, Clone)]
struct Node {
    row_id: SearchRowId,
    vector: Vec<f32>,
    neighbors: Vec<Vec<usize>>,
}

impl Node {
    fn new(record: VectorRecord, level: usize) -> Self {
        Self {
            row_id: record.row_id,
            vector: record.vector,
            neighbors: vec![Vec::new(); level + 1],
        }
    }

    fn level(&self) -> usize {
        self.neighbors.len() - 1
    }
}

#[derive(Debug, Clone, Copy)]
struct Neighbor {
    distance: f32,
    index: usize,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.index == other.index
    }
}

impl Eq for Neighbor {}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.index.cmp(&other.index))
    }
}

#[derive(Debug, Clone)]
pub struct HnswIndex {
    config: HnswConfig,
    limits: SearchLimits,
    nodes: Vec<Node>,
    entry_point: Option<usize>,
    max_level: usize,
}

impl HnswIndex {
    pub fn build(
        config: HnswConfig,
        records: &[VectorRecord],
        limits: SearchLimits,
    ) -> Result<Self> {
        config.validate(&limits)?;
        if records.len() > limits.max_documents {
            return Err(DbError::new(
                "54000",
                format!(
                    "HNSW index contains {} vectors, exceeding limit {}",
                    records.len(),
                    limits.max_documents
                ),
            ));
        }
        let mut records = records.to_vec();
        records.sort_by_key(|record| record.row_id);
        let mut seen = BTreeSet::new();
        let mut index = Self {
            config,
            limits,
            nodes: Vec::with_capacity(records.len()),
            entry_point: None,
            max_level: 0,
        };
        for record in records {
            if !seen.insert(record.row_id) {
                return Err(DbError::new(
                    "22023",
                    format!("duplicate vector row ID {}", record.row_id.get()),
                ));
            }
            index.validate_vector(&record.vector)?;
            index.insert(record)?;
        }
        index.validate_graph()?;
        Ok(index)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn search(&self, request: &VectorSearchRequest) -> Result<Vec<VectorSearchHit>> {
        self.limits.validate_limit(request.limit)?;
        self.validate_vector(&request.vector)?;
        let ef_search = request.ef_search.unwrap_or(self.config.ef_search);
        if ef_search == 0 || ef_search > self.limits.max_ef_search {
            return Err(DbError::new(
                "22023",
                format!(
                    "HNSW ef_search must be between 1 and {}",
                    self.limits.max_ef_search
                ),
            ));
        }
        let Some(mut entry) = self.entry_point else {
            return Ok(Vec::new());
        };
        if request
            .allowed_rows
            .as_ref()
            .is_some_and(|allowed| allowed.is_empty())
        {
            return Ok(Vec::new());
        }
        for level in (1..=self.max_level).rev() {
            entry = self.greedy_closest(&request.vector, entry, level)?;
        }
        let visit_limit = ef_search
            .max(request.limit)
            .checked_mul(self.config.m)
            .and_then(|value| value.checked_mul(4))
            .unwrap_or(usize::MAX)
            .min(self.nodes.len());
        let candidates = self.explore_layer(&request.vector, entry, 0, visit_limit)?;
        let mut hits = candidates
            .into_iter()
            .filter(|neighbor| {
                request
                    .allowed_rows
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(&self.nodes[neighbor.index].row_id))
            })
            .map(|neighbor| VectorSearchHit {
                row_id: self.nodes[neighbor.index].row_id,
                distance: neighbor.distance,
                score: self.config.metric.similarity(neighbor.distance),
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.len() != self.config.dimensions {
            return Err(DbError::new(
                "22023",
                format!(
                    "vector dimension mismatch: expected {}, received {}",
                    self.config.dimensions,
                    vector.len()
                ),
            ));
        }
        validate_finite_vector(vector)?;
        if self.config.metric == crate::VectorMetric::Cosine {
            let norm = vector
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>();
            if norm == 0.0 {
                return Err(DbError::new(
                    "22023",
                    "cosine HNSW does not accept zero-norm vectors",
                ));
            }
        }
        Ok(())
    }

    fn insert(&mut self, record: VectorRecord) -> Result<()> {
        let new_level = deterministic_level(record.row_id, self.config.seed);
        let new_index = self.nodes.len();
        let Some(mut entry) = self.entry_point else {
            self.nodes.push(Node::new(record, new_level));
            self.entry_point = Some(0);
            self.max_level = new_level;
            return Ok(());
        };

        for level in ((new_level + 1)..=self.max_level).rev() {
            entry = self.greedy_closest(&record.vector, entry, level)?;
        }
        self.nodes.push(Node::new(record, new_level));
        let connect_max = new_level.min(self.max_level);
        for level in (0..=connect_max).rev() {
            let candidates = self.search_layer(
                &self.nodes[new_index].vector,
                entry,
                level,
                self.config.ef_construction,
            )?;
            let selected = candidates
                .into_iter()
                .filter(|neighbor| neighbor.index != new_index)
                .take(self.config.m)
                .map(|neighbor| neighbor.index)
                .collect::<Vec<_>>();
            self.nodes[new_index].neighbors[level] = selected.clone();
            for neighbor in selected {
                self.nodes[neighbor].neighbors[level].push(new_index);
                self.prune_neighbors(neighbor, level)?;
            }
            self.prune_neighbors(new_index, level)?;
            if let Some(next) = self.nodes[new_index].neighbors[level].first() {
                entry = *next;
            }
        }
        if new_level > self.max_level {
            self.entry_point = Some(new_index);
            self.max_level = new_level;
        }
        Ok(())
    }

    fn greedy_closest(&self, query: &[f32], mut current: usize, level: usize) -> Result<usize> {
        let mut current_distance = self.distance(query, current)?;
        loop {
            let mut improved = None;
            for neighbor in self.neighbors(current, level) {
                let distance = self.distance(query, *neighbor)?;
                if distance < current_distance
                    || (distance == current_distance && *neighbor < current)
                {
                    current_distance = distance;
                    improved = Some(*neighbor);
                }
            }
            let Some(next) = improved else {
                return Ok(current);
            };
            current = next;
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry: usize,
        level: usize,
        ef: usize,
    ) -> Result<Vec<Neighbor>> {
        let entry_neighbor = Neighbor {
            distance: self.distance(query, entry)?,
            index: entry,
        };
        let mut candidates = BinaryHeap::from([Reverse(entry_neighbor)]);
        let mut best = BinaryHeap::from([entry_neighbor]);
        let mut visited = BTreeSet::from([entry]);
        while let Some(Reverse(candidate)) = candidates.pop() {
            if best.len() >= ef
                && best
                    .peek()
                    .is_some_and(|worst| candidate.distance > worst.distance)
            {
                break;
            }
            for neighbor in self.neighbors(candidate.index, level) {
                if !visited.insert(*neighbor) {
                    continue;
                }
                let next = Neighbor {
                    distance: self.distance(query, *neighbor)?,
                    index: *neighbor,
                };
                let should_add = best.len() < ef || best.peek().is_some_and(|worst| next < *worst);
                if should_add {
                    candidates.push(Reverse(next));
                    best.push(next);
                    if best.len() > ef {
                        best.pop();
                    }
                }
            }
        }
        let mut result = best.into_vec();
        result.sort();
        Ok(result)
    }

    fn explore_layer(
        &self,
        query: &[f32],
        entry: usize,
        level: usize,
        visit_limit: usize,
    ) -> Result<Vec<Neighbor>> {
        let start = Neighbor {
            distance: self.distance(query, entry)?,
            index: entry,
        };
        let mut candidates = BinaryHeap::from([Reverse(start)]);
        let mut visited = BTreeSet::from([entry]);
        let mut result = Vec::new();
        while let Some(Reverse(candidate)) = candidates.pop() {
            result.push(candidate);
            if visited.len() >= visit_limit {
                break;
            }
            for neighbor in self.neighbors(candidate.index, level) {
                if visited.insert(*neighbor) {
                    candidates.push(Reverse(Neighbor {
                        distance: self.distance(query, *neighbor)?,
                        index: *neighbor,
                    }));
                }
            }
        }
        result.sort();
        Ok(result)
    }

    fn prune_neighbors(&mut self, node_index: usize, level: usize) -> Result<()> {
        let origin = self.nodes[node_index].vector.clone();
        let mut neighbors = self.nodes[node_index].neighbors[level].clone();
        neighbors.sort_unstable();
        neighbors.dedup();
        let mut scored = neighbors
            .into_iter()
            .map(|neighbor| {
                self.config
                    .metric
                    .distance(&origin, &self.nodes[neighbor].vector)
                    .map(|distance| Neighbor {
                        distance,
                        index: neighbor,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        scored.sort();
        scored.truncate(self.config.m);
        self.nodes[node_index].neighbors[level] =
            scored.into_iter().map(|neighbor| neighbor.index).collect();
        Ok(())
    }

    fn validate_graph(&self) -> Result<()> {
        let Some(entry) = self.entry_point else {
            if self.nodes.is_empty() {
                return Ok(());
            }
            return Err(DbError::internal("non-empty HNSW has no entry point"));
        };
        if entry >= self.nodes.len() || self.nodes[entry].level() != self.max_level {
            return Err(DbError::internal(
                "HNSW entry point or maximum level is invalid",
            ));
        }
        let mut rows = BTreeSet::new();
        for (node_index, node) in self.nodes.iter().enumerate() {
            if !rows.insert(node.row_id) {
                return Err(DbError::internal("HNSW contains duplicate Row IDs"));
            }
            self.validate_vector(&node.vector)?;
            for (level, neighbors) in node.neighbors.iter().enumerate() {
                if neighbors.len() > self.config.m {
                    return Err(DbError::internal("HNSW neighbor list exceeds m"));
                }
                let mut unique = BTreeSet::new();
                for neighbor in neighbors {
                    if *neighbor >= self.nodes.len()
                        || *neighbor == node_index
                        || self.nodes[*neighbor].level() < level
                        || !unique.insert(*neighbor)
                    {
                        return Err(DbError::internal("HNSW neighbor link is invalid"));
                    }
                }
            }
        }
        Ok(())
    }

    fn neighbors(&self, node: usize, level: usize) -> &[usize] {
        self.nodes
            .get(node)
            .and_then(|node| node.neighbors.get(level))
            .map_or(&[], Vec::as_slice)
    }

    fn distance(&self, query: &[f32], node: usize) -> Result<f32> {
        self.config.metric.distance(query, &self.nodes[node].vector)
    }

    #[cfg(test)]
    fn exact_search(&self, request: &VectorSearchRequest) -> Result<Vec<VectorSearchHit>> {
        self.limits.validate_limit(request.limit)?;
        self.validate_vector(&request.vector)?;
        let mut hits = self
            .nodes
            .iter()
            .filter(|node| {
                request
                    .allowed_rows
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(&node.row_id))
            })
            .map(|node| {
                self.config
                    .metric
                    .distance(&request.vector, &node.vector)
                    .map(|distance| VectorSearchHit {
                        row_id: node.row_id,
                        distance,
                        score: self.config.metric.similarity(distance),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        hits.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }
}

fn deterministic_level(row_id: SearchRowId, seed: u64) -> usize {
    let mut value = row_id
        .get()
        .wrapping_add(seed.rotate_left(17))
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^= value >> 31;
    (value.trailing_zeros() as usize).min(MAX_HNSW_LEVEL)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::HnswIndex;
    use crate::{
        HnswConfig, SearchLimits, SearchRowId, VectorMetric, VectorRecord, VectorSearchRequest,
    };

    fn config(metric: VectorMetric) -> HnswConfig {
        HnswConfig {
            seed: 7,
            dimensions: 2,
            metric,
            m: 8,
            ef_construction: 32,
            ef_search: 64,
        }
    }

    fn records(count: u64) -> Vec<VectorRecord> {
        (1..=count)
            .map(|row_id| VectorRecord {
                row_id: SearchRowId::new(row_id),
                vector: vec![row_id as f32, 1.0],
            })
            .collect()
    }

    #[test]
    fn searches_deterministically_with_prefilter() {
        let index = HnswIndex::build(
            config(VectorMetric::L2),
            &records(100),
            SearchLimits::default(),
        )
        .expect("build");
        let request = VectorSearchRequest {
            index_id: ordadb_types::IndexId::new(1),
            vector: vec![50.2, 1.0],
            limit: 5,
            ef_search: Some(128),
            allowed_rows: None,
        };
        let first = index.search(&request).expect("search");
        let second = index.search(&request).expect("repeat");
        assert_eq!(first, second);
        assert_eq!(first[0].row_id, SearchRowId::new(50));

        let allowed = Arc::new(BTreeSet::from([SearchRowId::new(10), SearchRowId::new(90)]));
        let filtered = index
            .search(&VectorSearchRequest {
                allowed_rows: Some(allowed),
                ..request
            })
            .expect("filtered");
        assert_eq!(
            filtered.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
            [SearchRowId::new(90), SearchRowId::new(10)]
        );
    }

    #[test]
    fn reaches_the_exact_oracle_on_seeded_fixture() {
        let index = HnswIndex::build(
            config(VectorMetric::L2),
            &records(256),
            SearchLimits::default(),
        )
        .expect("build");
        let request = VectorSearchRequest {
            index_id: ordadb_types::IndexId::new(1),
            vector: vec![173.4, 1.0],
            limit: 10,
            ef_search: Some(256),
            allowed_rows: None,
        };
        let approximate = index.search(&request).expect("approximate");
        let exact = index.exact_search(&request).expect("exact");
        let exact_ids = exact.iter().map(|hit| hit.row_id).collect::<BTreeSet<_>>();
        let recalled = approximate
            .iter()
            .filter(|hit| exact_ids.contains(&hit.row_id))
            .count();
        assert!(recalled >= 9, "recall@10 was {recalled}/10");
    }

    #[test]
    fn rejects_invalid_vectors_and_duplicate_ids() {
        let mut invalid = records(2);
        invalid[1].vector = vec![f32::INFINITY, 1.0];
        assert_eq!(
            HnswIndex::build(config(VectorMetric::L2), &invalid, SearchLimits::default())
                .expect_err("finite")
                .sql_state,
            "22003"
        );
        let duplicate = vec![
            VectorRecord {
                row_id: SearchRowId::new(1),
                vector: vec![1.0, 1.0],
            },
            VectorRecord {
                row_id: SearchRowId::new(1),
                vector: vec![2.0, 1.0],
            },
        ];
        assert_eq!(
            HnswIndex::build(
                config(VectorMetric::Dot),
                &duplicate,
                SearchLimits::default()
            )
            .expect_err("duplicate")
            .sql_state,
            "22023"
        );
    }
}
