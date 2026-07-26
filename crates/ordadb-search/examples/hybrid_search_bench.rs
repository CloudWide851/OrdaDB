use std::collections::BTreeSet;
use std::mem::{size_of, zeroed};
use std::sync::Arc;
use std::time::Instant;

use ordadb_search::{
    FullTextAnalyzer, FullTextIndex, HnswConfig, HnswIndex, HybridSearchRequest, SearchDocument,
    SearchLimits, SearchRowId, TextSearchRequest, VectorMetric, VectorRecord, VectorSearchRequest,
    fuse_hybrid_hits,
};
use ordadb_types::IndexId;
use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

const ROWS: u64 = 100_000;
const QUERY_ROW: u64 = 75_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = SearchLimits::default();
    let documents = (0..ROWS)
        .map(|row_id| SearchDocument {
            row_id: SearchRowId::new(row_id),
            fields: vec![format!(
                "database engine tenant {} row {row_id}",
                row_id % 100
            )],
        })
        .collect::<Vec<_>>();
    let vectors = (0..ROWS)
        .map(|row_id| VectorRecord {
            row_id: SearchRowId::new(row_id),
            vector: fixture_vector(row_id),
        })
        .collect::<Vec<_>>();

    let text_started = Instant::now();
    let text = FullTextIndex::build(1, FullTextAnalyzer::Standard, &documents, limits.clone())?;
    let text_build = text_started.elapsed();
    let vector_started = Instant::now();
    let vector = HnswIndex::build(
        HnswConfig {
            seed: 2,
            dimensions: 8,
            metric: VectorMetric::L2,
            m: 8,
            ef_construction: 32,
            ef_search: 256,
        },
        &vectors,
        limits.clone(),
    )?;
    let vector_build = vector_started.elapsed();

    let allowed = Arc::new(
        (0..ROWS)
            .step_by(100)
            .map(SearchRowId::new)
            .collect::<BTreeSet<_>>(),
    );
    let text_request = TextSearchRequest {
        index_id: IndexId::new(1),
        query: "\"database engine\"".to_owned(),
        limit: 20,
        allowed_rows: Some(Arc::clone(&allowed)),
    };
    let vector_request = VectorSearchRequest {
        index_id: IndexId::new(2),
        vector: fixture_vector(QUERY_ROW),
        limit: 20,
        ef_search: Some(4_096),
        allowed_rows: Some(Arc::clone(&allowed)),
    };
    let peak_before_query = peak_working_set_bytes()?;
    let query_started = Instant::now();
    let text_hits = text.search(&text_request)?;
    let vector_hits = vector.search(&vector_request)?;
    let hybrid_hits = fuse_hybrid_hits(
        &HybridSearchRequest {
            text: text_request,
            vector: vector_request,
            text_weight: 0.4,
            vector_weight: 0.6,
            limit: 10,
        },
        &text_hits,
        &vector_hits,
        &limits,
    )?;
    let query_time = query_started.elapsed();
    let peak_after_query = peak_working_set_bytes()?;

    let exact = exact_l2_top(&vectors, &fixture_vector(QUERY_ROW), 10, Some(&allowed));
    let approximate = vector_hits
        .iter()
        .take(10)
        .map(|hit| hit.row_id)
        .collect::<BTreeSet<_>>();
    let recall = exact.intersection(&approximate).count() as f64 / exact.len() as f64;
    if recall < 0.9 {
        return Err(format!("HNSW recall@10 was {recall:.2}, expected at least 0.90").into());
    }
    if hybrid_hits.iter().any(|hit| !allowed.contains(&hit.row_id)) {
        return Err("hybrid prefilter admitted a disallowed row".into());
    }

    println!("rows={ROWS}");
    println!(
        "full_text_build_ms={} docs_per_second={:.0}",
        text_build.as_millis(),
        ROWS as f64 / text_build.as_secs_f64()
    );
    println!(
        "hnsw_build_ms={} vectors_per_second={:.0}",
        vector_build.as_millis(),
        ROWS as f64 / vector_build.as_secs_f64()
    );
    println!(
        "query_ms={} recall_at_10={recall:.2} allowed_rows={} hybrid_hits={}",
        query_time.as_millis(),
        allowed.len(),
        hybrid_hits.len()
    );
    println!("peak_working_set_bytes={peak_after_query}");
    println!(
        "query_peak_working_set_delta_bytes={}",
        peak_after_query.saturating_sub(peak_before_query)
    );
    Ok(())
}

fn fixture_vector(row_id: u64) -> Vec<f32> {
    let coordinate = row_id as f32 / ROWS as f32;
    vec![
        coordinate,
        1.0,
        coordinate * coordinate,
        coordinate.sin(),
        coordinate.cos(),
        coordinate * 0.5,
        0.25,
        0.75,
    ]
}

fn exact_l2_top(
    records: &[VectorRecord],
    query: &[f32],
    limit: usize,
    allowed: Option<&BTreeSet<SearchRowId>>,
) -> BTreeSet<SearchRowId> {
    let mut distances = records
        .iter()
        .filter(|record| allowed.is_none_or(|allowed| allowed.contains(&record.row_id)))
        .map(|record| {
            let distance = record
                .vector
                .iter()
                .zip(query)
                .map(|(left, right)| {
                    let difference = f64::from(*left) - f64::from(*right);
                    difference * difference
                })
                .sum::<f64>()
                .sqrt();
            (distance, record.row_id)
        })
        .collect::<Vec<_>>();
    distances.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    distances
        .into_iter()
        .take(limit)
        .map(|(_, row_id)| row_id)
        .collect()
}

fn peak_working_set_bytes() -> Result<usize, Box<dyn std::error::Error>> {
    // SAFETY: PROCESS_MEMORY_COUNTERS is a plain C output structure. The
    // process pseudo-handle remains valid for this call and the byte size
    // exactly matches the initialized structure supplied to psapi.
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { zeroed() };
    counters.cb = u32::try_from(size_of::<PROCESS_MEMORY_COUNTERS>())?;
    // SAFETY: both pointers and the declared structure size are valid for the
    // duration of this synchronous Windows API call.
    let succeeded =
        unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &raw mut counters, counters.cb) };
    if succeeded == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(counters.PeakWorkingSetSize)
}
