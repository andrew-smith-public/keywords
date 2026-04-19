//! Efficient Parquet data reading with row group and row-level pruning.
//!
//! This module provides optimized reading of Parquet data by using search results to skip
//! unnecessary row groups and rows. Instead of reading entire Parquet files, it uses the
//! keyword index to identify exactly which row groups and rows contain matching data,
//! dramatically reducing I/O and processing time.
//!
//! # Performance Benefits
//!
//! - **Row Group Pruning**: Skip entire row groups that don't contain target data
//! - **Row-Level Filtering**: Within relevant row groups, read only matching rows
//! - **Column Projection**: Read only the columns you need
//! - **Network Optimization**: Minimize range requests for remote files (S3, Azure, etc.)
//!
//! Typical performance gains:
//!
//! # Examples
//!
//! ```no_run
//! use keywords::searching::pruned_reader::PrunedParquetReader;
//! use keywords::searching::keyword_search::KeywordSearcher;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let searcher = KeywordSearcher::load("data.parquet", None).await?;
//!     let result = searcher.search("keyword", None, true).await?;
//!
//!     let reader = PrunedParquetReader::from_path("data.parquet");
//!     // Use the new read_search_result method that accepts SearchResult directly
//!     let batches = reader.read_search_result(&result, None).await?;
//!
//!     println!("Read {} batches", batches.len());
//!     Ok(())
//! }
//! ```

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, RecordBatch, Scalar, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::boolean::or_kleene;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::file::reader::FileReader;
use futures::StreamExt;
use crate::column_parquet_reader::array_to_string_smart;
use crate::searching::search_results::{
    CombinedSearchResult, CombinerKind, SearchResult,
};
use crate::utils::file_interaction_local_and_cloud::get_object_store;
use crate::ParquetSource;
use std::sync::Arc;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

/// Efficiently read Parquet data using search results to prune row groups and rows.
///
/// This reader uses keyword search results to identify exactly which row groups and rows to read,
/// skipping all irrelevant data for optimal performance.
///
/// # Examples
///
/// ```no_run
/// use keywords::searching::pruned_reader::PrunedParquetReader;
/// use keywords::searching::keyword_search::KeywordSearcher;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
///     let result = searcher.search("keyword", None, true).await?;
///
///     let reader = PrunedParquetReader::from_path("data.parquet");
///     // Use read_search_result for SearchResult or convert to SearchResult for read_matching_rows
///     let batches = reader.read_search_result(&result, None).await?;
///
///     println!("Read {} batches", batches.len());
///     Ok(())
/// }
/// ```
pub struct PrunedParquetReader {
    source: ParquetSource,
    /// Optional (offset, length) of the parquet footer, stored in the keyword
    /// index at build time. When set, lets the reader construct
    /// `ArrowReaderMetadata` once from a bounded range request instead of
    /// re-discovering the footer per parallel decode task.
    metadata_cache: Option<(u64, u64)>,
    /// Minimum skipped-rows-per-range for parquet `RowSelection` to be worth
    /// using. Below this, we decode the row group contiguously and apply the
    /// index ranges as a post-decode positional filter — the selector
    /// bookkeeping overhead of many tiny ranges outweighs the decode it avoids.
    /// Tune via [`PrunedParquetReader::with_row_selection_min_skip_per_range`].
    row_selection_min_skip_per_range: usize,
}

/// Default threshold for the `RowSelection` fragmentation heuristic. A range
/// earns its place only if, on average, it causes at least this many rows to
/// be skipped by the decoder. Tuned empirically; override per-reader.
pub const DEFAULT_ROW_SELECTION_MIN_SKIP_PER_RANGE: usize = 32;

/// Convert row ranges to a RowSelection for efficient row-level pruning.
///
/// This function builds a RowSelection that tells the Parquet reader exactly which rows to read,
/// enabling page-level skipping for optimal performance. The reader will automatically use
/// offset indexes (Parquet v2) if available, otherwise falls back to sequential page scanning
/// with early termination.
///
/// # Arguments
///
/// * `ranges` - Sorted vector of (start_row, end_row) tuples (inclusive)
/// * `total_rows` - Total number of rows in the row group
///
/// # Returns
///
/// A RowSelection that specifies which rows to select/skip, or None if all rows should be read
fn build_row_selection(
    ranges: &[(u32, u32)],
    total_rows: usize,
) -> Option<RowSelection> {
    if ranges.is_empty() {
        // No specific ranges - read all rows (don't use RowSelection)
        return None;
    }

    // Sort ranges by start position to ensure correct ordering
    let mut sorted_ranges = ranges.to_vec();
    sorted_ranges.sort_by_key(|r| r.0);

    let mut selectors = Vec::new();
    let mut current_pos = 0u32;
    let mut total_accounted = 0usize;  // Track total as we build

    for &(start, end) in &sorted_ranges {
        // Clamp range to valid bounds
        let start = start.min(total_rows.saturating_sub(1) as u32);
        let end = end.min(total_rows.saturating_sub(1) as u32);

        // Skip if this range is entirely out of bounds or already covered
        if start >= total_rows as u32 || end < current_pos {
            continue;
        }

        // Skip rows before this range
        if start > current_pos {
            let skip_count = (start - current_pos) as usize;
            selectors.push(RowSelector::skip(skip_count));
            total_accounted += skip_count;
            current_pos = start;
        }

        // Handle overlapping ranges: if start < current_pos, adjust to current_pos
        let actual_start = current_pos.max(start);

        // Select rows in this range (from actual_start to end, inclusive)
        if end >= actual_start {
            let select_count = (end - actual_start + 1) as usize;
            selectors.push(RowSelector::select(select_count));
            total_accounted += select_count;
            current_pos = end + 1;
        }
    }

    // Skip remaining rows after last range
    if (current_pos as usize) < total_rows {
        let skip_count = total_rows - current_pos as usize;
        selectors.push(RowSelector::skip(skip_count));
        total_accounted += skip_count;
    }

    // CRITICAL VALIDATION: Ensure we account for exactly total_rows
    if total_accounted != total_rows {
        // Selection is invalid - fall back to reading all rows
        eprintln!("WARNING: RowSelection mismatch! Expected {} rows, got {} accounted. Falling back to full read.",
                  total_rows, total_accounted);
        eprintln!("  Ranges: {:?}", ranges);
        eprintln!("  Selectors created: {}", selectors.len());
        return None;
    }

    if selectors.is_empty() {
        return None;
    }

    Some(RowSelection::from(selectors))
}

/// The query string parsed into each native form that might match a column.
/// Built once per verification read and captured by the predicate closure.
#[derive(Clone)]
struct ParsedQuery {
    raw: Arc<str>,
    as_i64: Option<i64>,
    as_u64: Option<u64>,
    as_bool: Option<bool>,
}

impl ParsedQuery {
    fn new(q: &str) -> Self {
        Self {
            raw: Arc::from(q),
            as_i64: q.parse().ok(),
            as_u64: q.parse().ok(),
            as_bool: match q {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
        }
    }
}

/// A predicate compiled against a concrete batch schema — computed once per
/// task, applied every batch. Pre-resolves:
/// - each check column's index in the decoded batch (avoids linear field-name
///   scans every batch),
/// - each column's query value parsed into its native type (avoids re-parsing
///   decimals / re-canonicalising floats every batch),
/// - columns the query can't possibly match are dropped up-front (skipped
///   entirely at evaluation time).
#[derive(Clone)]
struct CompiledPredicate {
    entries: Vec<CompiledEntry>,
    raw_query: Arc<str>,
}

#[derive(Clone)]
struct CompiledEntry {
    batch_col_idx: usize,
    value: CompiledValue,
}

#[derive(Clone)]
enum CompiledValue {
    Utf8,
    LargeUtf8,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Decimal128 { precision: u8, scale: i8, raw: i128 },
    Float32(f32),
    Float64(f64),
    /// Fall back to `array_to_string_smart` comparison at evaluation time —
    /// used for Decimal256 / Date / Timestamp / Binary and for NaN float
    /// queries where native eq diverges from index parity.
    Fallback,
}

impl CompiledPredicate {
    fn from_query(
        schema: &arrow::datatypes::Schema,
        query: &ParsedQuery,
        check_columns: &[String],
    ) -> Self {
        let mut entries = Vec::with_capacity(check_columns.len());
        for name in check_columns {
            let Some(idx) = schema.fields().iter().position(|f| f.name() == name) else {
                continue;
            };
            let dtype = schema.field(idx).data_type();
            if let Some(value) = CompiledValue::from_type_and_query(dtype, query) {
                entries.push(CompiledEntry { batch_col_idx: idx, value });
            }
        }
        Self { entries, raw_query: query.raw.clone() }
    }
}

impl CompiledValue {
    fn from_type_and_query(dtype: &DataType, query: &ParsedQuery) -> Option<Self> {
        let value = match dtype {
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Boolean => Self::Boolean(query.as_bool?),
            DataType::Int8 => Self::Int8(query.as_i64.and_then(|v| i8::try_from(v).ok())?),
            DataType::Int16 => Self::Int16(query.as_i64.and_then(|v| i16::try_from(v).ok())?),
            DataType::Int32 => Self::Int32(query.as_i64.and_then(|v| i32::try_from(v).ok())?),
            DataType::Int64 => Self::Int64(query.as_i64?),
            DataType::UInt8 => Self::UInt8(query.as_u64.and_then(|v| u8::try_from(v).ok())?),
            DataType::UInt16 => Self::UInt16(query.as_u64.and_then(|v| u16::try_from(v).ok())?),
            DataType::UInt32 => Self::UInt32(query.as_u64.and_then(|v| u32::try_from(v).ok())?),
            DataType::UInt64 => Self::UInt64(query.as_u64?),
            DataType::Decimal128(precision, scale) => {
                let raw = parse_decimal_to_i128(query.raw.as_ref(), *scale)?;
                Self::Decimal128 { precision: *precision, scale: *scale, raw }
            }
            DataType::Float32 => {
                let f = query.raw.parse::<f32>().ok()?;
                if f.is_nan() {
                    return Some(Self::Fallback);
                }
                let canonical = if f.fract() == 0.0 && f.is_finite() {
                    format!("{:.0}", f)
                } else {
                    f.to_string()
                };
                if canonical != query.raw.as_ref() {
                    return None;
                }
                Self::Float32(f)
            }
            DataType::Float64 => {
                let f = query.raw.parse::<f64>().ok()?;
                if f.is_nan() {
                    return Some(Self::Fallback);
                }
                let canonical = if f.fract() == 0.0 && f.is_finite() {
                    format!("{:.0}", f)
                } else {
                    f.to_string()
                };
                if canonical != query.raw.as_ref() {
                    return None;
                }
                Self::Float64(f)
            }
            _ => Self::Fallback,
        };
        Some(value)
    }
}

/// A unified scan + filter plan driving `read_with_plan`. Both the
/// single-term entry point (`read_search_result`) and the multi-term entry
/// point (`read_combined_rows`) build a `MatchPlan` and hand it to the same
/// internal reader — so the fragmentation heuristic, per-batch native-typed
/// equality, short-circuits, and all other optimisations apply uniformly.
///
/// `candidate_ranges` is the scan candidate set per row group (what gets
/// decoded). Each `MatchPlanTerm` carries verified ranges (where membership
/// alone means match) plus an optional pending predicate (rows that must have
/// their `check_columns` values compared to `query` before they count as a
/// match for that term). Term-level match results are then combined via
/// `combiner` (`Or` for single-term and for `combine_or`, `And` for
/// `combine_and`).
struct MatchPlan {
    /// Per-row-group candidate ranges — the union of each term's `(verified ∪
    /// pending)` ranges. Drives `RowSelection` and the fragmentation decision
    /// in the reader. Shared via `Arc` with the source `CombinedSearchResult`
    /// when possible so plan construction is O(1).
    candidate_ranges: crate::searching::search_results::CanonicalRangesByRg,
    terms: Vec<MatchPlanTerm>,
    combiner: MatchCombiner,
}

struct MatchPlanTerm {
    /// Pre-verified rows per row group — membership implies match; no
    /// predicate evaluation needed. Shared via `Arc` with the source.
    verified: crate::searching::search_results::CanonicalRangesByRg,
    /// Optional: rows that must pass `pending.parsed_query` against one of
    /// `pending.check_columns` to count as matching this term.
    pending: Option<PendingMatch>,
}

struct PendingMatch {
    ranges: crate::searching::search_results::CanonicalRangesByRg,
    parsed_query: ParsedQuery,
    check_columns: Vec<String>,
}

#[derive(Clone, Copy)]
enum MatchCombiner {
    Or,
    And,
}

impl From<CombinerKind> for MatchCombiner {
    fn from(c: CombinerKind) -> Self {
        match c {
            CombinerKind::And => MatchCombiner::And,
            CombinerKind::Or => MatchCombiner::Or,
        }
    }
}

/// Per-row-group, per-term state held inside the decode task. Compiled once
/// per task against the post-projection batch schema.
struct TermState {
    verified_ranges: Vec<(u32, u32)>,
    pending: Option<(Vec<(u32, u32)>, CompiledPredicate)>,
}

/// Parse a decimal-text query into its raw `i128` representation at `scale`
/// decimal digits. Returns `None` when the query can't possibly match: bad
/// digits, signs in the fractional part, or a fractional part longer than the
/// column's scale (which would require precision the column doesn't have).
fn parse_decimal_to_i128(s: &str, scale: i8) -> Option<i128> {
    if scale < 0 {
        return None;
    }
    let scale = scale as usize;
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (int_part, frac_part) = rest.split_once('.').unwrap_or((rest, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if frac_part.len() > scale {
        return None;
    }
    let mut combined = String::with_capacity(int_part.len() + scale);
    combined.push_str(int_part);
    combined.push_str(frac_part);
    for _ in 0..(scale - frac_part.len()) {
        combined.push('0');
    }
    let mut v: i128 = combined.parse().ok()?;
    if neg {
        v = -v;
    }
    Some(v)
}

/// Vectorised equality between a column and a `CompiledValue` scalar.
fn eq_compiled(
    array: &ArrayRef,
    value: &CompiledValue,
    raw_query: &str,
) -> Result<BooleanArray, ArrowError> {
    let lhs: &dyn Array = array.as_ref();
    let mask = match value {
        CompiledValue::Utf8 => {
            let rhs = StringArray::new_scalar(raw_query);
            cmp::eq(&lhs, &rhs)?
        }
        CompiledValue::LargeUtf8 => {
            let rhs = LargeStringArray::new_scalar(raw_query);
            cmp::eq(&lhs, &rhs)?
        }
        CompiledValue::Boolean(b) => cmp::eq(&lhs, &BooleanArray::new_scalar(*b))?,
        CompiledValue::Int8(v) => cmp::eq(&lhs, &Int8Array::new_scalar(*v))?,
        CompiledValue::Int16(v) => cmp::eq(&lhs, &Int16Array::new_scalar(*v))?,
        CompiledValue::Int32(v) => cmp::eq(&lhs, &Int32Array::new_scalar(*v))?,
        CompiledValue::Int64(v) => cmp::eq(&lhs, &Int64Array::new_scalar(*v))?,
        CompiledValue::UInt8(v) => cmp::eq(&lhs, &UInt8Array::new_scalar(*v))?,
        CompiledValue::UInt16(v) => cmp::eq(&lhs, &UInt16Array::new_scalar(*v))?,
        CompiledValue::UInt32(v) => cmp::eq(&lhs, &UInt32Array::new_scalar(*v))?,
        CompiledValue::UInt64(v) => cmp::eq(&lhs, &UInt64Array::new_scalar(*v))?,
        CompiledValue::Decimal128 { precision, scale, raw } => {
            let scalar_arr = Decimal128Array::from_iter_values([*raw])
                .with_precision_and_scale(*precision, *scale)?;
            let scalar = Scalar::new(scalar_arr);
            cmp::eq(&lhs, &scalar)?
        }
        CompiledValue::Float32(v) => cmp::eq(&lhs, &Float32Array::new_scalar(*v))?,
        CompiledValue::Float64(v) => cmp::eq(&lhs, &Float64Array::new_scalar(*v))?,
        CompiledValue::Fallback => {
            let tmp = array_to_string_smart(array);
            let tmp_ref: &dyn Array = tmp.as_ref();
            let rhs = StringArray::new_scalar(raw_query);
            cmp::eq(&tmp_ref, &rhs)?
        }
    };
    Ok(mask)
}

/// Compute the OR-of-equality mask for a batch without materialising a filtered
/// batch. Returned mask is null-safe: a row with NULL in every check column
/// gets a NULL bit, which `filter_record_batch` treats as exclude. Callers can
/// combine this with other masks via `and_kleene` before applying.
///
/// Short-circuits once every cell is definitely true (`true_count == len`) —
/// a later `eq` cannot flip anything and the `or_kleene` work would be wasted.
fn compute_predicate_mask(
    batch: &RecordBatch,
    predicate: &CompiledPredicate,
) -> Result<BooleanArray, ArrowError> {
    let mut acc: Option<BooleanArray> = None;
    for entry in &predicate.entries {
        let column = batch.column(entry.batch_col_idx);
        let m = eq_compiled(column, &entry.value, predicate.raw_query.as_ref())?;
        acc = Some(match acc.take() {
            None => m,
            Some(prev) => or_kleene(&prev, &m)?,
        });
        if acc.as_ref().map_or(false, |a| a.true_count() == a.len()) {
            break;
        }
    }
    Ok(acc.unwrap_or_else(|| BooleanArray::from(vec![false; batch.num_rows()])))
}

/// Build a positional BooleanArray over a batch whose rows correspond to
/// absolute row-group positions `positions[i]`. Bit `i` is set iff
/// `positions[i]` falls inside one of the (sorted) ranges. Since `positions`
/// is monotonically non-decreasing (both the full-row-group case and
/// RowSelection-decoded batches preserve row order), we walk both sequences
/// in lock-step for O(N + M) work instead of O(N * log M) or worse.
fn build_range_mask_for_positions(
    positions: &[u32],
    ranges: &[(u32, u32)],
) -> BooleanArray {
    let mut bits = vec![false; positions.len()];
    if ranges.is_empty() {
        return BooleanArray::from(bits);
    }
    let mut range_idx = 0usize;
    for (i, &pos) in positions.iter().enumerate() {
        while range_idx < ranges.len() && ranges[range_idx].1 < pos {
            range_idx += 1;
        }
        if range_idx >= ranges.len() {
            break;
        }
        if pos >= ranges[range_idx].0 {
            bits[i] = true;
        }
    }
    BooleanArray::from(bits)
}


/// Load `ArrowReaderMetadata` once so it can be shared across parallel decode
/// tasks via `ParquetRecordBatchStreamBuilder::new_with_metadata` — each task
/// then skips its own footer-read + metadata-parse.
///
/// Prefers the keyword index's stored `(offset, length)` when available (one
/// bounded range request for the footer bytes — matches the existing pattern
/// used by `read_combined_rows_with_metadata`). Falls back to probing via
/// `ParquetRecordBatchStreamBuilder::new` when no cache is provided, or when
/// the source is in-memory bytes and we can't cheaply reach the backing store.
async fn load_arrow_metadata(
    source: &ParquetSource,
    metadata_cache: Option<(u64, u64)>,
    fallback_reader: ParquetObjectReader,
) -> Result<ArrowReaderMetadata, Box<dyn std::error::Error + Send + Sync>> {
    if let (Some((offset, length)), ParquetSource::Path(p)) = (metadata_cache, source) {
        let (store, path) = get_object_store(p).await?;
        let range = offset..(offset + length);
        let bytes = store.get_range(&path, range).await?;
        let reader = parquet::file::reader::SerializedFileReader::new(bytes)?;
        let parquet_metadata = reader.metadata().clone();
        return Ok(ArrowReaderMetadata::try_new(
            Arc::new(parquet_metadata),
            ArrowReaderOptions::new(),
        )?);
    }

    // Fallback: probe the file via a throwaway builder.
    let builder = ParquetRecordBatchStreamBuilder::new(fallback_reader).await?;
    Ok(ArrowReaderMetadata::try_new(
        builder.metadata().clone(),
        ArrowReaderOptions::new(),
    )?)
}

/// Union per-row-group range sets into a combined scan candidate set.
fn union_range_maps(
    maps: &[&std::collections::HashMap<u16, Vec<(u32, u32)>>],
) -> std::collections::HashMap<u16, Vec<(u32, u32)>> {
    let mut out: std::collections::HashMap<u16, Vec<(u32, u32)>> =
        std::collections::HashMap::new();
    for map in maps {
        for (rg_id, ranges) in *map {
            out.entry(*rg_id)
                .or_insert_with(Vec::new)
                .extend(ranges.iter().copied());
        }
    }
    for ranges in out.values_mut() {
        sort_and_merge_ranges(ranges);
    }
    out
}

fn sort_and_merge_ranges(ranges: &mut Vec<(u32, u32)>) {
    ranges.sort_unstable();
    if ranges.is_empty() {
        return;
    }
    let mut write = 0;
    for read in 1..ranges.len() {
        if ranges[read].0 <= ranges[write].1.saturating_add(1) {
            ranges[write].1 = ranges[write].1.max(ranges[read].1);
        } else {
            write += 1;
            ranges[write] = ranges[read];
        }
    }
    ranges.truncate(write + 1);
}

/// Build a `MatchPlan` from a single-term `SearchResult`. The plan has one
/// term with both verified ranges and the pending predicate (when present);
/// the combiner is `Or` so verified matches OR pending-that-passes are kept.
///
/// Uses the canonical `ranges_by_rg` Arcs directly — no HashMap rebuild;
/// the plan shares storage with the source `SearchResult` via `Arc::clone`
/// (O(1)). The scan candidate set reuses the verified Arc when there's no
/// pending predicate, and otherwise builds a fresh union once.
fn plan_from_search_result(search_result: &SearchResult) -> MatchPlan {
    use std::sync::Arc;
    use std::collections::HashMap;
    use crate::searching::search_results::CanonicalRangesByRg;

    let verified: CanonicalRangesByRg = search_result
        .verified_matches
        .as_ref()
        .map(|d| d.ranges_by_rg.clone())
        .unwrap_or_else(|| Arc::new(HashMap::new()));
    let pending = search_result.needs_verification.as_ref().map(|data| {
        PendingMatch {
            ranges: data.ranges_by_rg.clone(),
            parsed_query: ParsedQuery::new(&search_result.query),
            check_columns: data.columns.clone(),
        }
    });

    let candidate_ranges: CanonicalRangesByRg = match &pending {
        Some(p) => Arc::new(union_range_maps(&[&*verified, &*p.ranges])),
        None => verified.clone(),
    };

    MatchPlan {
        candidate_ranges,
        terms: vec![MatchPlanTerm { verified, pending }],
        combiner: MatchCombiner::Or,
    }
}

/// Build a `MatchPlan` from a `CombinedSearchResult` produced by `combine_and`
/// or `combine_or`. One term per input `SearchResult`; the combiner follows
/// `combined.combiner`.
///
/// Reads the canonical `verified_ranges` / `pending_ranges` / `scan_ranges`
/// Arcs directly via `Arc::clone` (O(1)) — no rebuild from the display-only
/// `Vec<CombinedRowGroupLocation>`, and no HashMap copy.
fn plan_from_combined(combined: &CombinedSearchResult) -> MatchPlan {
    let terms: Vec<MatchPlanTerm> = combined
        .terms
        .iter()
        .map(|t| {
            let verified = t.verified_ranges.clone();
            let pending = if t.pending_ranges.is_empty() {
                None
            } else {
                Some(PendingMatch {
                    ranges: t.pending_ranges.clone(),
                    parsed_query: ParsedQuery::new(&t.query),
                    check_columns: t.check_columns.clone(),
                })
            };
            MatchPlanTerm { verified, pending }
        })
        .collect();

    let candidate_ranges = combined.scan_ranges.clone();

    MatchPlan {
        candidate_ranges,
        terms,
        combiner: combined.combiner.into(),
    }
}

/// Collect row ranges from a KeywordLocationData into per-row-group sorted,
/// non-overlapping ranges. Merges overlapping or adjacent ranges so each
/// physical row appears at most once.
///
/// Only `column_details[0]` is consumed. Safe in every search mode:
/// - Specific-column search (`keyword_search.rs:1163-1187`): the column loop
///   `continue`s past every non-target column, so `column_details` has exactly
///   one entry with that column's ranges.
/// - Aggregate search (`keyword_search.rs:1267-1290`): column 0's single entry
///   is cloned N times into per-column slots for API shape. Every clone holds
///   identical range data — iterating all N would push the same ranges N times
///   into the merge vec and inflate `sort_unstable` for zero new information.
fn collect_merged_ranges(
    data: &crate::searching::search_results::KeywordLocationData,
    row_groups_to_read: &mut std::collections::HashSet<usize>,
    row_group_ranges: &mut std::collections::HashMap<u16, Vec<(u32, u32)>>,
) {
    let Some(col_detail) = data.column_details.first() else {
        return;
    };
    for rg in &col_detail.row_groups {
        row_groups_to_read.insert(rg.row_group_id as usize);
        let ranges = row_group_ranges.entry(rg.row_group_id).or_insert_with(Vec::new);
        for range in &rg.row_ranges {
            ranges.push((range.start_row, range.end_row));
        }
    }

    // Sort and merge overlapping/adjacent ranges per row group
    for ranges in row_group_ranges.values_mut() {
        ranges.sort_unstable();
        let mut write = 0;
        for read in 1..ranges.len() {
            if ranges[read].0 <= ranges[write].1 + 1 {
                ranges[write].1 = ranges[write].1.max(ranges[read].1);
            } else {
                write += 1;
                ranges[write] = ranges[read];
            }
        }
        ranges.truncate(write + 1);
    }
}


impl PrunedParquetReader {
    /// Create a new pruned Parquet reader for the specified source.
    ///
    /// This constructor only stores the source; no I/O is performed until a read method is called.
    ///
    /// # Arguments
    ///
    /// * `source` - Parquet source, either a file path (local or remote like S3/Azure)
    ///   or in-memory bytes. Use `ParquetSource::Path(path)` or `ParquetSource::Bytes(vec)`.
    ///
    /// # Returns
    ///
    /// A new `PrunedParquetReader` instance ready to read from the specified source.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::ParquetSource;
    ///
    /// // From file path
    /// let reader = PrunedParquetReader::new(ParquetSource::Path("/data/users.parquet".to_string()));
    ///
    /// // From S3
    /// let reader = PrunedParquetReader::new(ParquetSource::Path("s3://bucket/data.parquet".to_string()));
    ///
    /// // From in-memory bytes
    /// let parquet_bytes = vec![/* ... */];
    /// let reader = PrunedParquetReader::new(ParquetSource::from(parquet_bytes));
    /// ```
    pub fn new(source: ParquetSource) -> Self {
        Self {
            source,
            metadata_cache: None,
            row_selection_min_skip_per_range: DEFAULT_ROW_SELECTION_MIN_SKIP_PER_RANGE,
        }
    }

    /// Register the parquet footer's byte offset + length so the reader can
    /// construct `ArrowReaderMetadata` once from a bounded range request and
    /// reuse it across parallel decode tasks. Values come from the keyword
    /// index (`filters.parquet_metadata_offset` / `parquet_metadata_length`).
    pub fn with_metadata_cache(mut self, offset: u64, length: u64) -> Self {
        self.metadata_cache = Some((offset, length));
        self
    }

    /// Override the `RowSelection` fragmentation threshold. Per row group,
    /// `RowSelection` is used iff `(rg_size - total_selected) / num_ranges`
    /// meets or exceeds this value — i.e. each range must cause at least this
    /// many rows of decode to be skipped. Below the threshold, the index
    /// ranges are applied as a post-decode positional filter instead.
    ///
    /// Setting to `0` always uses `RowSelection`. A very high value always
    /// uses post-decode filtering. Default:
    /// [`DEFAULT_ROW_SELECTION_MIN_SKIP_PER_RANGE`].
    pub fn with_row_selection_min_skip_per_range(mut self, min: usize) -> Self {
        self.row_selection_min_skip_per_range = min;
        self
    }

    /// Create a reader from a file path (convenience method).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the Parquet file. Can be local path, S3 URL (`s3://bucket/path`),
    ///   or Azure URL (`az://container/path`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    ///
    /// let reader = PrunedParquetReader::from_path("data.parquet");
    /// ```
    pub fn from_path(path: &str) -> Self {
        Self::new(ParquetSource::Path(path.to_string()))
    }

    /// Create a reader from in-memory parquet bytes (convenience method).
    ///
    /// # Arguments
    ///
    /// * `bytes` - Parquet file data as bytes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use bytes::Bytes;
    ///
    /// let parquet_bytes = vec![/* ... */];
    /// let reader = PrunedParquetReader::from_bytes(Bytes::from(parquet_bytes));
    /// ```
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self::new(ParquetSource::Bytes(bytes))
    }

    /// Create a ParquetObjectReader with file size optimization
    /// Providing the file size ensures bounded range requests instead of suffix range requests,
    /// which is an important optimization to avoid extra calls
    ///
    /// Performance consideration: ParquetObjectReader may issue multiple GET requests per row group
    /// This is a known trade-off in the Parquet ecosystem: small targeted range requests provide
    /// better performance for selective queries but incur more API calls. For pruned reads (our use case),
    /// this is generally optimal since we only fetch needed row groups. Alternative approaches:
    /// - Pre-fetching entire file: Would waste bandwidth for selective queries
    /// - Batched range requests: Parquet format doesn't always allow predictable batching
    /// - Caching layer: Adds complexity, most benefit comes from OS page cache already
    /// Monitor S3 request costs if this becomes a bottleneck. Current approach aligns with
    /// standard Parquet reader behavior and works well for our access patterns.
    async fn create_object_reader(&self) -> Result<ParquetObjectReader, Box<dyn std::error::Error + Send + Sync>> {
        match &self.source {
            ParquetSource::Path(path) => {
                let (store, obj_path) = get_object_store(path).await?;
                let meta = store.head(&obj_path).await?;
                Ok(ParquetObjectReader::new(store, obj_path)
                    .with_file_size(meta.size))
            }
            ParquetSource::Bytes(bytes) => {
                // Create in-memory object store
                let store = Arc::new(InMemory::new());
                let path = ObjectPath::from("in_memory.parquet");
                let bytes_copy = Bytes::copy_from_slice(bytes);
                let file_size = bytes_copy.len() as u64;
                store.put(&path, bytes_copy.into()).await?;

                Ok(ParquetObjectReader::new(store, path)
                    .with_file_size(file_size))
            }
        }
    }

    /// Read only the rows that match a single keyword search.
    ///
    /// Uses the search result to identify which row groups and rows to read, efficiently skipping
    /// all non-matching data. Returns batches of up to 8192 rows each.
    ///
    /// # Arguments
    ///
    /// * `search_result` - Result from [`KeywordSearcher::search()`]. If `found` is `false`,
    ///   returns empty vector immediately without accessing the file.
    /// * `columns` - Optional column projection:
    ///   - `None` - Read all columns
    ///   - `Some(vec)` - Read only specified columns (reduces I/O and memory)
    ///   - Column names not found in schema are silently ignored
    ///
    /// # Returns
    ///
    /// `Ok(Vec<RecordBatch>)` - Vector of Arrow RecordBatches containing matching rows.
    /// Returns empty vector if keyword not found or no matches in file.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * File cannot be accessed (not found, permission denied, network failure)
    /// * Parquet file is corrupted or has invalid metadata
    /// * Parquet format is incompatible with Arrow reader
    /// * Search result has `found=true` but `data` is `None` (invalid state)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let result = searcher.search("test@example.com", None, true).await?;
    ///
    ///     let reader = PrunedParquetReader::from_path("data.parquet");
    ///
    ///     // Read all columns
    ///     let batches = reader.read_matching_rows(&result, None).await?;
    ///     println!("Found {} batches", batches.len());
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **With column projection:**
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("users.parquet", None).await?;
    ///     let result = searcher.search("alice", None, true).await?;
    ///
    ///     let reader = PrunedParquetReader::from_path("users.parquet");
    ///
    ///     // Only read specific columns
    ///     let columns = vec!["user_id".to_string(), "email".to_string()];
    ///     let batches = reader.read_matching_rows(&result, Some(columns)).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`KeywordSearcher::search()`]: crate::searching::keyword_search::KeywordSearcher::search
    pub async fn read_matching_rows(
        &self,
        search_result: &SearchResult,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if !search_result.found {
            return Ok(Vec::new());
        }

        let data = search_result.verified_matches.as_ref()
            .ok_or("Search result has no data")?;

        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut row_group_ranges: std::collections::HashMap<u16, Vec<(u32, u32)>> =
            std::collections::HashMap::new();
        collect_merged_ranges(data, &mut row_groups_to_read, &mut row_group_ranges);

        if row_groups_to_read.is_empty() {
            return Ok(Vec::new());
        }

        // Create ParquetObjectReader
        let object_reader = self.create_object_reader().await?;

        let mut all_batches = Vec::new();
        let row_groups_vec: Vec<usize> = row_groups_to_read.into_iter().collect();

        for &rg_idx in &row_groups_vec {
            // Create async stream builder
            let mut builder = ParquetRecordBatchStreamBuilder::new(object_reader.clone()).await?;

            // Apply column projection if specified
            if let Some(cols) = &columns {
                let schema = builder.schema();
                let indices: Vec<usize> = cols.iter()
                    .filter_map(|col_name| {
                        schema.fields().iter().position(|f| f.name() == col_name)
                    })
                    .collect();

                if !indices.is_empty() {
                    let mask = ProjectionMask::leaves(
                        builder.metadata().file_metadata().schema_descr(),
                        indices
                    );
                    builder = builder.with_projection(mask);
                }
            }

            // Get row group size and row ranges for this row group
            let row_group_metadata = builder.metadata().row_group(rg_idx);
            let row_group_size = row_group_metadata.num_rows() as usize;
            let ranges = row_group_ranges.get(&(rg_idx as u16))
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            // Build RowSelection for efficient row-level pruning (if needed)
            let selection = build_row_selection(ranges, row_group_size);

            // Apply row selection if we have specific ranges
            let builder_with_rg = builder.with_row_groups(vec![rg_idx]);
            let mut stream = if let Some(sel) = selection {
                builder_with_rg
                    .with_row_selection(sel)  // Only read needed rows!
                    .with_batch_size(8192)
                    .build()?
            } else {
                // No selection - read all rows in row group
                builder_with_rg
                    .with_batch_size(8192)
                    .build()?
            };

            // Stream the selected rows
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }

        Ok(all_batches)
    }


    /// Read Parquet data for unified SearchResult.
    ///
    /// Reads both verified matches and matches needing verification from the SearchResult.
    /// This is the recommended method for use with the unified search API.
    ///
    /// # Arguments
    ///
    /// * `search_result` - Result from `KeywordSearcher::search()`
    /// * `columns` - Optional column projection (None = all columns)
    ///
    /// # Returns
    ///
    /// Vector of RecordBatches containing all matching rows (verified + needs verification).
    /// If you only want verified matches, filter the search_result before passing it.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use keywords::searching::pruned_reader::PrunedParquetReader;
    /// # use keywords::searching::keyword_search::KeywordSearcher;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let searcher = KeywordSearcher::load("data.parquet", None).await?;
    /// let result = searcher.search("example.com", None, false).await?;
    ///
    /// let reader = PrunedParquetReader::from_path("data.parquet");
    /// let batches = reader.read_search_result(&result, None).await?;
    ///
    /// println!("Read {} batches", batches.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_search_result(
        &self,
        search_result: &SearchResult,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if !search_result.found {
            return Ok(Vec::new());
        }
        let plan = plan_from_search_result(search_result);
        self.read_with_plan(plan, columns).await
    }


    /// Read only the rows that match combined search results (AND/OR logic).
    ///
    /// Used after combining multiple keyword searches with [`KeywordSearcher::combine_and()`]
    /// or [`KeywordSearcher::combine_or()`]. Efficiently reads only rows matching the combined criteria.
    ///
    /// # Arguments
    ///
    /// * `combined_result` - Result from combining multiple keyword searches with AND/OR logic.
    ///   If `row_groups` is empty, returns empty vector immediately.
    /// * `columns` - Optional column projection:
    ///   - `None` - Read all columns
    ///   - `Some(vec)` - Read only specified columns
    ///
    /// # Returns
    ///
    /// `Ok(Vec<RecordBatch>)` - Vector of Arrow RecordBatches containing rows that match
    /// the combined search criteria. Returns empty vector if no matches.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * File cannot be accessed (not found, permission denied, network failure)
    /// * Parquet file is corrupted or has invalid metadata
    /// * Parquet format is incompatible with Arrow reader
    ///
    /// # Examples
    ///
    /// **AND logic (all keywords must match):**
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///
    ///     // Find rows containing ALL keywords
    ///     let search1 = searcher.search("error", None, true).await?;
    ///     let search2 = searcher.search("database", None, true).await?;
    ///     let search3 = searcher.search("connection", None, true).await?;
    ///
    ///     let r1 = search1;
    ///     let r2 = search2;
    ///     let r3 = search3;
    ///
    ///     let combined = KeywordSearcher::combine_and(&[r1, r2, r3]);
    ///
    ///     if let Some(result) = combined {
    ///         let reader = PrunedParquetReader::from_path("logs.parquet");
    ///         let batches = reader.read_combined_rows(&result, None).await?;
    ///
    ///         let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    ///         println!("Found {} rows with all three keywords", total);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **OR logic (any keyword can match):**
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///
    ///     // Find rows containing ANY keyword
    ///     let search_error = searcher.search("error", None, true).await?;
    ///     let search_warning = searcher.search("warning", None, true).await?;
    ///     let search_critical = searcher.search("critical", None, true).await?;
    ///
    ///     let error = search_error;
    ///     let warning = search_warning;
    ///     let critical = search_critical;
    ///
    ///     let combined = KeywordSearcher::combine_or(&[error, warning, critical]);
    ///
    ///     if let Some(result) = combined {
    ///         let reader = PrunedParquetReader::from_path("logs.parquet");
    ///         let batches = reader.read_combined_rows(&result, None).await?;
    ///
    ///         let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    ///         println!("Found {} high-severity entries", total);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`KeywordSearcher::combine_and()`]: crate::searching::keyword_search::KeywordSearcher::combine_and
    /// [`KeywordSearcher::combine_or()`]: crate::searching::keyword_search::KeywordSearcher::combine_or
    pub async fn read_combined_rows(
        &self,
        combined_result: &CombinedSearchResult,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if combined_result.row_groups.is_empty() || combined_result.terms.is_empty() {
            return Ok(Vec::new());
        }
        let plan = plan_from_combined(combined_result);
        self.read_with_plan(plan, columns).await
    }

    /// Read rows from a combined search result with an explicit metadata
    /// cache override. Thin wrapper over `read_combined_rows` that clones the
    /// reader with `with_metadata_cache` applied. Kept for backward API
    /// compatibility — new code should prefer
    /// `PrunedParquetReader::with_metadata_cache(...)` at construction time.
    pub async fn read_combined_rows_with_metadata(
        &self,
        combined_result: &CombinedSearchResult,
        columns: Option<Vec<String>>,
        metadata_cache: Option<(u64, u64)>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        match metadata_cache {
            Some((offset, length)) => {
                let reader = PrunedParquetReader {
                    source: self.source.clone(),
                    metadata_cache: Some((offset, length)),
                    row_selection_min_skip_per_range: self.row_selection_min_skip_per_range,
                };
                reader.read_combined_rows(combined_result, columns).await
            }
            None => self.read_combined_rows(combined_result, columns).await,
        }
    }

    /// Unified reader backing both `read_search_result` and
    /// `read_combined_rows`. Takes a `MatchPlan` describing the scan candidate
    /// set and per-term match logic; produces the filtered batches.
    ///
    /// Drives one decode task per row group concurrently. Each task
    /// independently applies the fragmentation heuristic (to decide
    /// `RowSelection` vs contiguous decode), compiles per-term pending
    /// predicates once against the post-projection batch schema, and folds
    /// per-batch term masks together via `plan.combiner`.
    async fn read_with_plan(
        &self,
        plan: MatchPlan,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if plan.candidate_ranges.is_empty() {
            return Ok(Vec::new());
        }

        let object_reader = self.create_object_reader().await?;
        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);

        let arrow_metadata = load_arrow_metadata(
            &self.source,
            self.metadata_cache,
            object_reader.clone(),
        )
        .await?;
        let num_row_groups_in_file = arrow_metadata.metadata().num_row_groups();
        let min_skip = self.row_selection_min_skip_per_range;
        let plan = Arc::new(plan);

        let tasks = plan
            .candidate_ranges
            .iter()
            .filter_map(|(rg_id, candidate)| {
                let rg_idx = *rg_id as usize;
                if rg_idx >= num_row_groups_in_file || candidate.is_empty() {
                    return None;
                }
                let object_reader = object_reader.clone();
                let arrow_metadata = arrow_metadata.clone();
                let columns = columns.clone();
                let plan = plan.clone();
                let candidate = candidate.clone();
                let rg_u16 = *rg_id;
                Some(async move {
                    Self::read_one_row_group_with_plan(
                        object_reader,
                        arrow_metadata,
                        rg_idx,
                        rg_u16,
                        candidate,
                        columns,
                        plan,
                        min_skip,
                    )
                    .await
                })
            })
            .collect::<Vec<_>>();

        use futures::TryStreamExt;
        let per_task: Vec<Vec<RecordBatch>> = futures::stream::iter(tasks)
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let total: usize = per_task.iter().map(Vec::len).sum();
        let mut all_batches = Vec::with_capacity(total);
        for mut b in per_task {
            all_batches.append(&mut b);
        }
        Ok(all_batches)
    }

    /// Decode one row group against a shared `MatchPlan`. Builds the parquet
    /// stream, compiles per-term pending predicates against the post-
    /// projection batch schema, then walks batches applying
    /// `verified OR (pending_range AND pending_predicate)` per term and
    /// combining across terms via the plan's combiner.
    #[allow(clippy::too_many_arguments)]
    async fn read_one_row_group_with_plan(
        object_reader: ParquetObjectReader,
        arrow_metadata: ArrowReaderMetadata,
        rg_idx: usize,
        rg_u16: u16,
        candidate_ranges: Vec<(u32, u32)>,
        columns: Option<Vec<String>>,
        plan: Arc<MatchPlan>,
        row_selection_min_skip_per_range: usize,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if rg_idx >= arrow_metadata.metadata().num_row_groups() {
            return Ok(Vec::new());
        }

        let file_schema = arrow_metadata.schema().clone();
        let schema_descr = arrow_metadata
            .metadata()
            .file_metadata()
            .schema_descr()
            .clone();
        let rg_size = arrow_metadata.metadata().row_group(rg_idx).num_rows() as usize;

        // Fragmentation heuristic: same logic as before, now driven by the
        // plan's candidate set for this row group.
        let total_selected: usize = candidate_ranges
            .iter()
            .map(|(s, e)| (*e as usize).saturating_sub(*s as usize) + 1)
            .sum();
        let skipped = rg_size.saturating_sub(total_selected);
        let skip_per_range = if candidate_ranges.is_empty() {
            0
        } else {
            skipped / candidate_ranges.len()
        };
        let use_row_selection =
            !candidate_ranges.is_empty() && skip_per_range >= row_selection_min_skip_per_range;

        let mut builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(object_reader, arrow_metadata);

        if let Some(ref cols) = columns {
            let indices: Vec<usize> = cols
                .iter()
                .filter_map(|n| file_schema.fields().iter().position(|f| f.name() == n))
                .collect();
            if !indices.is_empty() {
                builder = builder.with_projection(ProjectionMask::roots(&schema_descr, indices));
            }
        }

        let builder = builder.with_row_groups(vec![rg_idx]).with_batch_size(8192);
        let mut stream = if use_row_selection {
            let selection = build_row_selection(&candidate_ranges, rg_size);
            if let Some(sel) = selection {
                builder.with_row_selection(sel).build()?
            } else {
                builder.build()?
            }
        } else {
            builder.build()?
        };

        // Compile per-term pending predicates once per task against the
        // post-projection batch schema. Extract the subset of per-term
        // verified / pending ranges that apply to this row group.
        let batch_schema = stream.schema().clone();
        let term_states: Vec<TermState> = plan
            .terms
            .iter()
            .map(|term| {
                let verified_ranges = term
                    .verified
                    .get(&rg_u16)
                    .cloned()
                    .unwrap_or_default();
                let pending = term.pending.as_ref().and_then(|p| {
                    let ranges = p.ranges.get(&rg_u16).cloned().unwrap_or_default();
                    if ranges.is_empty() {
                        return None;
                    }
                    let compiled = CompiledPredicate::from_query(
                        batch_schema.as_ref(),
                        &p.parsed_query,
                        &p.check_columns,
                    );
                    Some((ranges, compiled))
                });
                TermState {
                    verified_ranges,
                    pending,
                }
            })
            .collect();

        // Original row-group positions for every decoded row, in order:
        // - RowSelection on: positions are the expanded candidate ranges,
        //   since the decoder emits only selected rows in file order.
        // - RowSelection off: positions are 0..rg_size (every row decoded).
        // Per-term masks use these original positions to check membership in
        // verified / pending ranges — checking against output indices would
        // be wrong when RowSelection shifts rows around.
        let original_positions: Vec<u32> = if use_row_selection {
            candidate_ranges
                .iter()
                .flat_map(|&(s, e)| s..=e)
                .collect()
        } else {
            (0..rg_size as u32).collect()
        };

        // When RowSelection was skipped, the decoder emits every row of the
        // row group; we must still trim to the candidate set ourselves.
        let need_candidate_range_filter = !use_row_selection;

        use arrow::compute::filter_record_batch;
        use arrow::compute::kernels::boolean::{and_kleene, or_kleene};

        let mut batches = Vec::new();
        let mut batch_start: usize = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let batch_len = batch.num_rows();
            if batch_len == 0 {
                continue;
            }
            let positions = &original_positions[batch_start..batch_start + batch_len];

            // Per-term: verified_mask OR (pending_range_mask AND predicate).
            let mut term_masks: Vec<BooleanArray> = Vec::with_capacity(term_states.len());
            for state in &term_states {
                let verified_mask =
                    build_range_mask_for_positions(positions, &state.verified_ranges);
                let term_mask = match &state.pending {
                    Some((ranges, compiled)) => {
                        let range_mask = build_range_mask_for_positions(positions, ranges);
                        let pred_mask = compute_predicate_mask(&batch, compiled)?;
                        let pending_mask = and_kleene(&range_mask, &pred_mask)?;
                        or_kleene(&verified_mask, &pending_mask)?
                    }
                    None => verified_mask,
                };
                term_masks.push(term_mask);
            }

            // Combine per term: OR-all or AND-all.
            let combined_term_mask: Option<BooleanArray> = if term_masks.is_empty() {
                None
            } else {
                let mut iter = term_masks.into_iter();
                let mut acc = iter.next().unwrap();
                for m in iter {
                    acc = match plan.combiner {
                        MatchCombiner::Or => or_kleene(&acc, &m)?,
                        MatchCombiner::And => and_kleene(&acc, &m)?,
                    };
                }
                Some(acc)
            };

            // AND the candidate-set mask in when we skipped RowSelection.
            let final_mask = if need_candidate_range_filter {
                let range_mask = build_range_mask_for_positions(positions, &candidate_ranges);
                match combined_term_mask {
                    None => Some(range_mask),
                    Some(tm) => Some(and_kleene(&range_mask, &tm)?),
                }
            } else {
                combined_term_mask
            };

            let kept = match final_mask {
                None => batch,
                Some(mask) if mask.true_count() == mask.len() => batch,
                Some(mask) => filter_record_batch(&batch, &mask)?,
            };
            if kept.num_rows() > 0 {
                batches.push(kept);
            }

            batch_start += batch_len;
        }
        Ok(batches)
    }

    /// Get statistics about how much data can be skipped by pruning.
    ///
    /// Analyzes the search result against Parquet metadata to calculate how many row groups
    /// and rows can be skipped. Useful for understanding query selectivity and deciding
    /// whether to use pruned reading vs full scan.
    ///
    /// # Arguments
    ///
    /// * `search_result` - Result from [`KeywordSearcher::search()`].
    ///
    /// # Returns
    ///
    /// `Ok(PruningStats)` - Statistics showing:
    /// * Total row groups and rows in file
    /// * Row groups and rows that must be read
    /// * Row groups and rows that can be skipped
    /// * Skip percentages for both metrics
    ///
    /// If `search_result.found` is `false`, returns stats showing 100% skip rate.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * File cannot be accessed to read metadata
    /// * Parquet metadata is corrupted or invalid
    /// * Search result has `found=true` but `data` is `None` (invalid state)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let result = searcher.search("rare_value", None, true).await?;
    ///
    ///     let reader = PrunedParquetReader::from_path("data.parquet");
    ///     let stats = reader.get_pruning_stats(&result).await?;
    ///
    ///     println!("Row Groups: {}/{} ({:.1}% skipped)",
    ///         stats.row_groups_to_read,
    ///         stats.total_row_groups,
    ///         stats.row_group_skip_percentage);
    ///     println!("Rows: {}/{} ({:.1}% skipped)",
    ///         stats.rows_to_read,
    ///         stats.total_rows,
    ///         stats.row_skip_percentage);
    ///
    ///     // Decide whether to proceed with pruned read
    ///     if stats.row_skip_percentage > 50.0 {
    ///         let batches = reader.read_matching_rows(&result, None).await?;
    ///         println!("High selectivity - read {} batches", batches.len());
    ///     } else {
    ///         println!("Low selectivity - consider full scan instead");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`KeywordSearcher::search()`]: crate::searching::keyword_search::KeywordSearcher::search
    pub async fn get_pruning_stats(
        &self,
        search_result: &SearchResult,
    ) -> Result<PruningStats, Box<dyn std::error::Error + Send + Sync>> {
        // Create ParquetObjectReader and builder to access metadata
        let object_reader = self.create_object_reader().await?;
        let builder = ParquetRecordBatchStreamBuilder::new(object_reader).await?;
        let metadata = builder.metadata();

        let total_row_groups = metadata.num_row_groups();
        let total_rows: i64 = metadata.file_metadata().num_rows();

        if !search_result.found {
            return Ok(PruningStats {
                total_row_groups,
                row_groups_to_read: 0,
                row_groups_skipped: total_row_groups,
                row_group_skip_percentage: 100.0,
                total_rows: total_rows as u64,
                rows_to_read: 0,
                rows_skipped: total_rows as u64,
                row_skip_percentage: 100.0,
            });
        }

        let data = search_result.verified_matches.as_ref().unwrap();

        // Count row groups to read
        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut rows_to_read = 0u64;

        for col_detail in &data.column_details {
            for rg in &col_detail.row_groups {
                row_groups_to_read.insert(rg.row_group_id);

                for range in &rg.row_ranges {
                    rows_to_read += (range.end_row - range.start_row + 1) as u64;
                }
            }
        }

        let row_groups_read = row_groups_to_read.len();
        let row_groups_skipped = total_row_groups - row_groups_read;
        let rows_skipped = (total_rows as u64).saturating_sub(rows_to_read);

        Ok(PruningStats {
            total_row_groups,
            row_groups_to_read: row_groups_read,
            row_groups_skipped,
            row_group_skip_percentage: (row_groups_skipped as f64 / total_row_groups as f64) * 100.0,
            total_rows: total_rows as u64,
            rows_to_read,
            rows_skipped,
            row_skip_percentage: (rows_skipped as f64 / total_rows as f64) * 100.0,
        })
    }
}

/// Statistics about how much data can be skipped during pruned reading.
///
/// Provides detailed metrics about pruning efficiency at both the row group and row level,
/// helping you understand query selectivity and potential performance benefits.
///
/// # Fields
///
/// * `total_row_groups` - Total number of row groups in the Parquet file
/// * `row_groups_to_read` - Number of row groups that must be read (contain matches)
/// * `row_groups_skipped` - Number of row groups that can be skipped entirely
/// * `row_group_skip_percentage` - Percentage of row groups skipped (0.0 to 100.0)
/// * `total_rows` - Total number of rows in the Parquet file
/// * `rows_to_read` - Number of rows that must be read (match search criteria)
/// * `rows_skipped` - Number of rows that can be skipped
/// * `row_skip_percentage` - Percentage of rows skipped (0.0 to 100.0)
///
/// # Interpreting Results
///
/// **Row skip percentage guidelines:**
/// # Examples
///
/// ```no_run
/// use keywords::searching::pruned_reader::PrunedParquetReader;
/// use keywords::searching::keyword_search::KeywordSearcher;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
///     let result = searcher.search("keyword", None, true).await?;
///
///     let reader = PrunedParquetReader::from_path("data.parquet");
///     let stats = reader.get_pruning_stats(&result).await?;
///
///     // Check row group pruning efficiency
///     if stats.row_group_skip_percentage > 80.0 {
///         println!("Excellent row group pruning: {:.1}%", stats.row_group_skip_percentage);
///     }
///
///     // Check row-level pruning efficiency
///     if stats.row_skip_percentage > 90.0 {
///         println!("Excellent row-level pruning: {:.1}%", stats.row_skip_percentage);
///     }
///
///     // Estimate I/O savings
///     let io_reduction = stats.row_skip_percentage / 100.0;
///     println!("Estimated I/O reduction: {:.1}x", 1.0 / (1.0 - io_reduction));
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PruningStats {
    pub total_row_groups: usize,
    pub row_groups_to_read: usize,
    pub row_groups_skipped: usize,
    pub row_group_skip_percentage: f64,
    pub total_rows: u64,
    pub rows_to_read: u64,
    pub rows_skipped: u64,
    pub row_skip_percentage: f64,
}

#[cfg(test)]
mod tests {
    use tokio::sync::OnceCell;
    use std::sync::Arc;
    use arrow::array::{StringArray, Int32Array, Int64Array, Float64Array, BooleanArray};
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use rand::{Rng, SeedableRng};
    use crate::{build_index_in_memory, ParquetSource};
    use super::*;
    use crate::searching::keyword_search::KeywordSearcher;

    static TEST_SEARCHER: OnceCell<KeywordSearcher> = OnceCell::const_new();
    static TEST_PARQUET: OnceCell<Bytes> = OnceCell::const_new();

    /// Generate a small test parquet file with 500 distinct values, 1000 rows
    fn create_test_parquet() -> Result<Bytes, Box<dyn std::error::Error>> {
        const DISTINCT_VALUES: usize = 500;
        const TOTAL_ROWS: usize = 1000;

        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);

        // Create schema with mixed types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        // Generate pool of distinct values
        let names: Vec<String> = (0..DISTINCT_VALUES)
            .map(|i| format!("user_{}", i))
            .collect();
        let emails: Vec<String> = (0..DISTINCT_VALUES)
            .map(|i| format!("user_{}@test{}.com", i, i % 10))
            .collect();
        let statuses = vec!["active", "inactive", "pending", "suspended"];

        // Generate row data
        let mut id_data = Vec::with_capacity(TOTAL_ROWS);
        let mut name_data = Vec::with_capacity(TOTAL_ROWS);
        let mut email_data = Vec::with_capacity(TOTAL_ROWS);
        let mut status_data = Vec::with_capacity(TOTAL_ROWS);
        let mut age_data = Vec::with_capacity(TOTAL_ROWS);
        let mut score_data = Vec::with_capacity(TOTAL_ROWS);
        let mut active_data = Vec::with_capacity(TOTAL_ROWS);

        for i in 0..TOTAL_ROWS {
            id_data.push(i as i32);
            name_data.push(names[rng.random_range(0..DISTINCT_VALUES)].clone());
            email_data.push(emails[rng.random_range(0..DISTINCT_VALUES)].clone());
            status_data.push(statuses[rng.random_range(0..statuses.len())].to_string());
            age_data.push(rng.random_range(18..80) as i64);
            score_data.push(rng.random_range(0.0..100.0));
            active_data.push(rng.random_bool(0.7));
        }

        // Create arrays
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(id_data)),
                Arc::new(StringArray::from(name_data)),
                Arc::new(StringArray::from(email_data)),
                Arc::new(StringArray::from(status_data)),
                Arc::new(Int64Array::from(age_data)),
                Arc::new(Float64Array::from(score_data)),
                Arc::new(BooleanArray::from(active_data)),
            ],
        )?;

        // Write to buffer
        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Verify file is < 1MB
        assert!(buffer.len() < 1024 * 1024,
                "Small parquet should be < 1MB, got {} bytes ({:.2} MB)",
                buffer.len(), buffer.len() as f64 / (1024.0 * 1024.0));

        Ok(Bytes::from(buffer))
    }

    async fn get_test_parquet() -> &'static Bytes {
        TEST_PARQUET.get_or_init(|| async {
            create_test_parquet().expect("Failed to create test parquet")
        }).await
    }

    async fn get_searcher() -> &'static KeywordSearcher {
        TEST_SEARCHER.get_or_init(|| async {
            println!("Building test index in memory...");
            let parquet_bytes = get_test_parquet().await;
            build_index_in_memory(ParquetSource::Bytes(parquet_bytes.clone()), None, None, None, None, None, None, None, None, None)
                .await
                .expect("Build Index Failed")
        }).await
    }

    #[tokio::test]
    async fn test_pruned_read() {
        let searcher = get_searcher().await;
        let parquet_bytes = get_test_parquet().await;

        // Search for a keyword that exists in the generated data
        let search_result = searcher.search("user_0", None, true).await.unwrap();

        if !search_result.found {
            println!("Keyword not found - skipping test");
            return;
        }

        // Convert to SearchResult for compatibility with get_pruning_stats and read_matching_rows
        let result = SearchResult {
            query: search_result.query.clone(),
            found: search_result.found,
            tokens: search_result.tokens.clone(),
            verified_matches: search_result.verified_matches.clone(),
            needs_verification: search_result.needs_verification.clone(),
        };

        // Create pruned reader from bytes
        let reader = PrunedParquetReader::new(ParquetSource::Bytes(parquet_bytes.clone()));

        // Get pruning stats
        let stats = reader.get_pruning_stats(&result).await.unwrap();
        println!("Pruning stats:");
        println!("  Row groups: {}/{} ({:.1}% skipped)",
                 stats.row_groups_to_read, stats.total_row_groups, stats.row_group_skip_percentage);
        println!("  Rows: {}/{} ({:.1}% skipped)",
                 stats.rows_to_read, stats.total_rows, stats.row_skip_percentage);

        // Read only matching rows
        let batches = reader.read_matching_rows(&result, None).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Read {} rows in {} batches", total_rows, batches.len());
    }

    #[tokio::test]
    async fn test_combined_pruned_read() {
        let searcher = get_searcher().await;
        let parquet_bytes = get_test_parquet().await;

        // Search with AND logic for keywords that exist in generated data
        let search_result1 = searcher.search("user_0", None, true).await.unwrap();
        let search_result2 = searcher.search("active", None, true).await.unwrap();

        // Convert to SearchResult for combine_and
        let result1 = SearchResult {
            query: search_result1.query.clone(),
            found: search_result1.found,
            tokens: search_result1.tokens.clone(),
            verified_matches: search_result1.verified_matches.clone(),
            needs_verification: search_result1.needs_verification.clone(),
        };
        let result2 = SearchResult {
            query: search_result2.query.clone(),
            found: search_result2.found,
            tokens: search_result2.tokens.clone(),
            verified_matches: search_result2.verified_matches.clone(),
            needs_verification: search_result2.needs_verification.clone(),
        };

        let combined = KeywordSearcher::combine_and(&[result1, result2]);

        if let Some(combined_result) = combined {
            let reader = PrunedParquetReader::new(ParquetSource::Bytes(parquet_bytes.clone()));

            // Read only rows matching both conditions
            let batches = reader.read_combined_rows(&combined_result, None).await.unwrap();

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("Combined AND read {} rows in {} batches", total_rows, batches.len());
        }
    }

    #[tokio::test]
    async fn test_column_projection() {
        let searcher = get_searcher().await;
        let parquet_bytes = get_test_parquet().await;

        let search_result = searcher.search("active", None, true).await.unwrap();

        if !search_result.found {
            return;
        }

        // Convert to SearchResult for read_matching_rows
        let result = SearchResult {
            query: search_result.query.clone(),
            found: search_result.found,
            tokens: search_result.tokens.clone(),
            verified_matches: search_result.verified_matches.clone(),
            needs_verification: search_result.needs_verification.clone(),
        };

        let reader = PrunedParquetReader::new(ParquetSource::Bytes(parquet_bytes.clone()));

        // Read only specific columns from generated data
        let columns = vec!["name".to_string(), "email".to_string(), "status".to_string()];
        let batches = reader.read_matching_rows(&result, Some(columns)).await.unwrap();

        if !batches.is_empty() {
            println!("Columns read: {:?}",
                     batches[0].schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        }
    }

}