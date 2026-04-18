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
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter,
    RowSelection, RowSelector,
};
use parquet::file::reader::FileReader;
use futures::StreamExt;
use crate::column_parquet_reader::array_to_string_smart;
use crate::searching::search_results::{SearchResult, CombinedSearchResult};
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

/// Build a positional BooleanArray for a batch whose rows start at absolute
/// row-group position `batch_row_offset`. Bit `i` is set iff `(batch_row_offset + i)`
/// falls inside one of the index ranges. Used when `RowSelection` has been
/// skipped for fragmentation reasons — this recovers the same positional
/// filter the decoder would have applied, just one stage later.
fn build_range_mask_for_batch(
    batch_row_offset: u32,
    batch_len: usize,
    ranges: &[(u32, u32)],
) -> BooleanArray {
    let batch_end = batch_row_offset as usize + batch_len;
    let mut bits = vec![false; batch_len];
    for &(start, end) in ranges {
        let start = start as usize;
        let end_exclusive = end as usize + 1;
        if end_exclusive <= batch_row_offset as usize || start >= batch_end {
            continue;
        }
        let local_start = start.saturating_sub(batch_row_offset as usize);
        let local_end = (end_exclusive - batch_row_offset as usize).min(batch_len);
        for bit in &mut bits[local_start..local_end] {
            *bit = true;
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

/// Build a parquet `ArrowPredicate` that evaluates the same equality predicate
/// as `filter_batch_natively`, but during decode so the reader can skip the
/// non-predicate output columns for rows that fail. Returns `None` when none
/// of `check_columns` exist in the schema (no rows can match).
///
/// Used selectively: only when `check_columns` are a small share of the output
/// projection, so the double-decode of predicate columns (once to evaluate,
/// once for output) is outweighed by saved decode on the other columns.
fn build_row_filter_predicate(
    file_schema: &arrow::datatypes::Schema,
    schema_descr: &parquet::schema::types::SchemaDescriptor,
    check_columns: &[String],
    parsed_query: &ParsedQuery,
) -> Option<Box<dyn ArrowPredicate>> {
    // Resolve check_column names to file indices. The parquet reader emits
    // predicate columns in file-index order regardless of what we pass, so
    // sort+dedupe here and build `CompiledPredicate` against that order.
    let mut file_indices: Vec<usize> = check_columns
        .iter()
        .filter_map(|name| file_schema.fields().iter().position(|f| f.name() == name))
        .collect();
    file_indices.sort_unstable();
    file_indices.dedup();
    if file_indices.is_empty() {
        return None;
    }

    let mut entries = Vec::new();
    for (batch_pos, &file_idx) in file_indices.iter().enumerate() {
        let dtype = file_schema.field(file_idx).data_type();
        if let Some(value) = CompiledValue::from_type_and_query(dtype, parsed_query) {
            entries.push(CompiledEntry { batch_col_idx: batch_pos, value });
        }
    }
    let compiled = CompiledPredicate {
        entries,
        raw_query: parsed_query.raw.clone(),
    };

    let mask = ProjectionMask::roots(schema_descr, file_indices);

    let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
        let mut acc: Option<BooleanArray> = None;
        for entry in &compiled.entries {
            let column = batch.column(entry.batch_col_idx);
            let m = eq_compiled(column, &entry.value, compiled.raw_query.as_ref())?;
            acc = Some(match acc.take() {
                None => m,
                Some(prev) => or_kleene(&prev, &m)?,
            });
            if acc.as_ref().map_or(false, |a| a.true_count() == a.len()) {
                break;
            }
        }
        Ok(acc.unwrap_or_else(|| BooleanArray::from(vec![false; batch.num_rows()])))
    });
    Some(Box::new(predicate))
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

        let mut batches = Vec::new();

        // Read verified matches — confirmed correct, return as-is.
        if let Some(verified) = &search_result.verified_matches {
            let mut rg_set = std::collections::HashSet::new();
            let mut rg_ranges = std::collections::HashMap::new();
            collect_merged_ranges(verified, &mut rg_set, &mut rg_ranges);
            if !rg_set.is_empty() {
                let rgs: Vec<usize> = rg_set.into_iter().collect();
                let mut verified_batches = self
                    .read_row_groups_filtered(&rgs, &rg_ranges, columns.clone(), None)
                    .await?;
                batches.append(&mut verified_batches);
            }
        }

        // Read needs_verification matches and filter them inline as each batch
        // streams off the decoder. Split-elimination fires only on frequent
        // keywords, so the predicate's match rate is high — a parquet
        // `RowFilter` would double-decode the check columns with little pruning
        // benefit. Inline native filtering per batch avoids that double decode
        // and lets decode/filter pipeline via the stream's await points.
        if let Some(needs_check) = &search_result.needs_verification {
            let mut rg_set = std::collections::HashSet::new();
            let mut rg_ranges = std::collections::HashMap::new();
            collect_merged_ranges(needs_check, &mut rg_set, &mut rg_ranges);
            if !rg_set.is_empty() {
                let rgs: Vec<usize> = rg_set.into_iter().collect();
                let verification =
                    Some((ParsedQuery::new(&search_result.query), needs_check.columns.clone()));
                let mut filtered_batches = self
                    .read_row_groups_filtered(&rgs, &rg_ranges, columns, verification)
                    .await?;
                batches.append(&mut filtered_batches);
            }
        }

        Ok(batches)
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
        self.read_combined_rows_with_metadata(combined_result, columns, None).await
    }

    /// Read rows from a combined search result with optional metadata caching.
    ///
    /// This is the internal implementation that supports metadata caching for performance.
    /// When metadata offset/length are provided, the Parquet metadata is read once
    /// and reused for all row groups, avoiding redundant metadata reads.
    ///
    /// # Arguments
    ///
    /// * `combined_result` - Combined search result from `combine_and()` or `combine_or()`
    /// * `columns` - Optional column projection (None = all columns)
    /// * `metadata_cache` - Optional tuple of (metadata_offset, metadata_length) for caching
    pub async fn read_combined_rows_with_metadata(
        &self,
        combined_result: &CombinedSearchResult,
        columns: Option<Vec<String>>,
        metadata_cache: Option<(u64, u64)>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if combined_result.row_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Collect row groups and ranges
        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut row_group_ranges: std::collections::HashMap<u16, Vec<(u32, u32)>> =
            std::collections::HashMap::new();

        for rg in &combined_result.row_groups {
            row_groups_to_read.insert(rg.row_group_id as usize);

            let ranges = row_group_ranges.entry(rg.row_group_id).or_insert_with(Vec::new);
            for range in &rg.row_ranges {
                ranges.push((range.start_row, range.end_row));
            }
        }

        // Create ParquetObjectReader
        let object_reader = self.create_object_reader().await?;

        // Pre-allocate vector for better performance
        let row_groups_vec: Vec<usize> = row_groups_to_read.into_iter().collect();
        let mut all_batches = Vec::with_capacity(row_groups_vec.len());

        // If metadata cache is provided, read metadata once and reuse for all row groups
        if let Some((metadata_offset, metadata_length)) = metadata_cache {
            use parquet::arrow::arrow_reader::ArrowReaderMetadata;

            // Read metadata once using stored offset/length
            let (store, path) = match &self.source {
                ParquetSource::Path(p) => {
                    use crate::utils::file_interaction_local_and_cloud::get_object_store;
                    get_object_store(p).await?
                },
                ParquetSource::Bytes(_) => {
                    // For in-memory bytes, metadata is already in memory, no benefit to caching
                    // Fall through to non-cached path
                    return self.read_combined_rows_non_cached(
                        &row_groups_vec,
                        &row_group_ranges,
                        &columns,
                        object_reader
                    ).await;
                }
            };

            let range = metadata_offset..(metadata_offset + metadata_length);
            let metadata_bytes = store.get_range(&path, range).await?;

            // Parse Parquet metadata from bytes using SerializedFileReader
            // The metadata_bytes contain: [FileMetaData][4-byte footer length][4-byte "PAR1"]
            use parquet::file::reader::SerializedFileReader;
            use parquet::arrow::arrow_reader::ArrowReaderOptions;

            let reader = SerializedFileReader::new(metadata_bytes)?;
            let parquet_metadata = reader.metadata().clone();

            // Create ArrowReaderMetadata for reuse
            let arrow_metadata = ArrowReaderMetadata::try_new(
                Arc::new(parquet_metadata),
                ArrowReaderOptions::new()
            )?;

            // Now iterate through row groups using cached metadata
            for &rg_idx in &row_groups_vec {
                let row_group_size = arrow_metadata.metadata()
                    .row_group(rg_idx)
                    .num_rows() as usize;
                let batch_size = row_group_size.min(100_000);

                // Create builder with cached metadata (no redundant metadata reads!)
                let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    object_reader.clone(),
                    arrow_metadata.clone()
                );

                // Apply column projection if specified
                if let Some(ref cols) = columns {
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

                // Get row ranges and build RowSelection
                let ranges = row_group_ranges.get(&(rg_idx as u16))
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let selection = build_row_selection(ranges, row_group_size);

                // Apply row selection if we have specific ranges
                let builder_with_rg = builder.with_row_groups(vec![rg_idx]);
                let mut stream = if let Some(sel) = selection {
                    builder_with_rg
                        .with_row_selection(sel)  // Only read needed rows!
                        .with_batch_size(batch_size)
                        .build()?
                } else {
                    // No selection - read all rows in row group
                    builder_with_rg
                        .with_batch_size(batch_size)
                        .build()?
                };

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result?;
                    if batch.num_rows() > 0 {
                        all_batches.push(batch);
                    }
                }
            }
        } else {
            // No metadata cache - use original approach (one builder per row group)
            all_batches = self.read_combined_rows_non_cached(
                &row_groups_vec,
                &row_group_ranges,
                &columns,
                object_reader
            ).await?;
        }

        Ok(all_batches)
    }

    /// Non-cached path for reading combined rows (original implementation)
    async fn read_combined_rows_non_cached(
        &self,
        row_groups_vec: &[usize],
        row_group_ranges: &std::collections::HashMap<u16, Vec<(u32, u32)>>,
        columns: &Option<Vec<String>>,
        object_reader: ParquetObjectReader,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let mut all_batches = Vec::with_capacity(row_groups_vec.len());

        for rg_idx in row_groups_vec {
            let rg_idx = *rg_idx; // Dereference since we're iterating over &[usize]
            // Create builder for this row group
            // Note: object_reader caches metadata, so subsequent creations are faster
            let mut builder = ParquetRecordBatchStreamBuilder::new(object_reader.clone()).await?;

            // Get row group size for adaptive batch sizing
            let row_group_metadata = builder.metadata().row_group(rg_idx);
            let row_group_size = row_group_metadata.num_rows() as usize;

            // Adaptive batch size: use row group size but cap at 100K to prevent memory issues
            // This ensures small row groups are read in one batch (minimizing overhead)
            // while large row groups are chunked to reasonable sizes
            let batch_size = row_group_size.min(100_000);

            // Apply column projection if specified
            if let Some(cols) = columns {
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

            // Get row ranges and build RowSelection
            let ranges = row_group_ranges.get(&(rg_idx as u16))
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let selection = build_row_selection(ranges, row_group_size);

            // Apply row selection if we have specific ranges
            let builder_with_rg = builder.with_row_groups(vec![rg_idx]);
            let mut stream = if let Some(sel) = selection {
                builder_with_rg
                    .with_row_selection(sel)  // Only read needed rows!
                    .with_batch_size(batch_size)
                    .build()?
            } else {
                // No selection - read all rows in row group
                builder_with_rg
                    .with_batch_size(batch_size)
                    .build()?
            };

            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }

        Ok(all_batches)
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

    /// Read specific row groups restricted to the given per-row-group indexed
    /// ranges. Uses parquet `RowSelection` so only the ranges are decoded.
    ///
    /// Row groups are decoded concurrently: parquet column decode and
    /// decompression are independent across row groups, and DataFusion gets
    /// most of its wall-time advantage from exploiting that. We match it by
    /// driving up to `available_parallelism()` row groups in flight via
    /// `buffer_unordered`.
    /// Read specific row groups restricted to the given per-row-group indexed
    /// ranges. Uses parquet `RowSelection` for row-level pruning.
    ///
    /// Decodes one stream per row group concurrently via `buffer_unordered`
    /// (bounded by `available_parallelism`). Matches DataFusion's partitioning
    /// model: row groups are the unit of parallelism, never split. `ArrowReaderMetadata`
    /// is loaded once and shared across tasks so no task pays for a footer parse.
    ///
    /// When `verification` is provided, each batch is passed through the native
    /// equality filter inline with decode — this keeps memory bounded and lets
    /// decode of batch N+1 overlap with filter of batch N on multi-core runtimes.
    async fn read_row_groups_filtered(
        &self,
        row_groups_vec: &[usize],
        row_group_ranges: &std::collections::HashMap<u16, Vec<(u32, u32)>>,
        columns: Option<Vec<String>>,
        verification: Option<(ParsedQuery, Vec<String>)>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if row_groups_vec.is_empty() {
            return Ok(Vec::new());
        }

        let object_reader = self.create_object_reader().await?;
        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);

        // Load metadata once — shared across every task via new_with_metadata
        // so no task pays for a footer read/parse.
        let arrow_metadata =
            load_arrow_metadata(&self.source, self.metadata_cache, object_reader.clone()).await?;
        let num_row_groups_in_file = arrow_metadata.metadata().num_row_groups();

        let min_skip = self.row_selection_min_skip_per_range;

        let futures_iter = row_groups_vec.iter().copied().filter_map(|rg_idx| {
            if rg_idx >= num_row_groups_in_file {
                return None;
            }
            let ranges: Vec<(u32, u32)> = row_group_ranges
                .get(&(rg_idx as u16))
                .cloned()
                .unwrap_or_default();
            if ranges.is_empty() {
                return None;
            }
            let object_reader = object_reader.clone();
            let arrow_metadata = arrow_metadata.clone();
            let columns = columns.clone();
            let verification = verification.clone();
            Some(async move {
                Self::read_one_row_group(
                    object_reader,
                    arrow_metadata,
                    rg_idx,
                    ranges,
                    columns,
                    verification,
                    min_skip,
                )
                .await
            })
        });

        use futures::TryStreamExt;
        let per_task: Vec<Vec<RecordBatch>> = futures::stream::iter(futures_iter)
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

    /// Decode one row group against a shared, pre-loaded `ArrowReaderMetadata`.
    ///
    /// Chooses between three strategies based on fragmentation and selectivity:
    /// 1. **RowSelection + RowFilter** (predicate push-down): index ranges
    ///    drive the decoder's row selection; the query predicate is evaluated
    ///    during decode so non-predicate columns are only decoded for rows
    ///    that pass.
    /// 2. **RowSelection + post-filter predicate**: index ranges drive decode;
    ///    query predicate (when present) is applied per batch after decode.
    /// 3. **No RowSelection + post-filter range (+ predicate)**: ranges are
    ///    too fragmented for `RowSelection` to pay off — the row group is
    ///    decoded contiguously and a positional mask built from the index
    ///    ranges is applied per batch (AND'd with the predicate mask if any).
    ///
    /// The split between 1/2 and 3 is controlled by `row_selection_min_skip_per_range`.
    async fn read_one_row_group(
        object_reader: ParquetObjectReader,
        arrow_metadata: ArrowReaderMetadata,
        rg_idx: usize,
        ranges: Vec<(u32, u32)>,
        columns: Option<Vec<String>>,
        verification: Option<(ParsedQuery, Vec<String>)>,
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

        // --- Fragmentation decision ----------------------------------------
        // RowSelection pays its per-selector bookkeeping cost only when each
        // range causes enough decode to be skipped. `skip_per_range` guards
        // against division-by-zero (no ranges → post-decode path handles the
        // empty case trivially).
        let total_selected: usize = ranges
            .iter()
            .map(|(s, e)| (*e as usize).saturating_sub(*s as usize) + 1)
            .sum();
        let skipped = rg_size.saturating_sub(total_selected);
        let skip_per_range = if ranges.is_empty() {
            0
        } else {
            skipped / ranges.len()
        };
        let use_row_selection =
            !ranges.is_empty() && skip_per_range >= row_selection_min_skip_per_range;

        // --- Predicate path decision ---------------------------------------
        // RowFilter (decode fusion) only makes sense when RowSelection is
        // also in play — otherwise the predicate would see rows outside our
        // index ranges and potentially admit them. When RowSelection is
        // dropped, the post-decode path applies both range and predicate
        // masks together.
        let output_col_count = columns
            .as_ref()
            .map(|c| c.len())
            .unwrap_or_else(|| file_schema.fields().len());
        let use_row_filter = use_row_selection
            && verification
                .as_ref()
                .map(|(_, cols)| cols.len() * 3 <= output_col_count.max(1))
                .unwrap_or(false);

        // --- Builder setup -------------------------------------------------
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

        if use_row_filter {
            if let Some((parsed, check_cols)) = verification.as_ref() {
                if let Some(predicate) =
                    build_row_filter_predicate(&file_schema, &schema_descr, check_cols, parsed)
                {
                    builder = builder.with_row_filter(RowFilter::new(vec![predicate]));
                }
            }
        }

        let builder = builder.with_row_groups(vec![rg_idx]).with_batch_size(8192);
        let mut stream = if use_row_selection {
            let selection = build_row_selection(&ranges, rg_size);
            if let Some(sel) = selection {
                builder.with_row_selection(sel).build()?
            } else {
                // Ranges yielded no selection (e.g. all out of bounds) — fall
                // back to reading everything; the range filter below will
                // drop the non-matching rows.
                builder.build()?
            }
        } else {
            builder.build()?
        };

        // --- Post-decode filter setup --------------------------------------
        // Predicate: compile once against the post-projection batch schema.
        // Skipped when RowFilter already evaluated the predicate during decode.
        let compiled_post = if use_row_filter {
            None
        } else {
            verification.as_ref().map(|(parsed, cols)| {
                CompiledPredicate::from_query(stream.schema().as_ref(), parsed, cols)
            })
        };
        // Range: when RowSelection was skipped, the decoder emits every row
        // in the row group; we apply the index ranges as a positional mask.
        let need_range_filter = !use_row_selection;

        // --- Stream and filter ---------------------------------------------
        use arrow::compute::filter_record_batch;
        use arrow::compute::kernels::boolean::and_kleene;

        let mut batches = Vec::new();
        let mut batch_row_offset: u32 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let batch_len = batch.num_rows();
            if batch_len == 0 {
                continue;
            }

            let range_mask = if need_range_filter {
                Some(build_range_mask_for_batch(batch_row_offset, batch_len, &ranges))
            } else {
                None
            };
            let predicate_mask = compiled_post
                .as_ref()
                .map(|p| compute_predicate_mask(&batch, p))
                .transpose()?;

            let combined = match (range_mask, predicate_mask) {
                (None, None) => None,
                (Some(r), None) => Some(r),
                (None, Some(p)) => Some(p),
                (Some(r), Some(p)) => Some(and_kleene(&r, &p)?),
            };

            let kept = match combined {
                None => batch,
                Some(mask) if mask.true_count() == mask.len() => batch,
                Some(mask) => filter_record_batch(&batch, &mask)?,
            };
            if kept.num_rows() > 0 {
                batches.push(kept);
            }

            batch_row_offset += batch_len as u32;
        }
        Ok(batches)
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