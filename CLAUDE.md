# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

**keywords** is a Rust library and CLI tool that builds keyword indexes on Parquet files and performs fast keyword searches. It achieves large speedups over Apache DataFusion by using hierarchical keyword extraction, bloom filter pre-checks, zero-copy deserialization (rkyv), and chunked on-demand loading.
Understand that it is designed to work with object storage, to be cost effective, speedy and efficient. It must scale to millions of parquet files when considering speed and efficiency.

## Build & Test Commands

All test invocations should enable the `timing` feature so fine-grained
stderr breakdowns are produced for any test that exercises the hot search
path. The feature compiles out completely when disabled, so there is no
runtime cost — keeping it on by default just makes perf regressions
visible when they happen.

```bash
cargo build                        # debug build
cargo build --release              # release build (uses target-cpu=native via .cargo/config.toml)
cargo check --features timing      # fast type-check without building
cargo fmt                          # format code
cargo clippy --features timing     # lint
cargo test --features timing                                    # all tests
cargo test --features timing <test_name>                        # single test by name
cargo test --features timing <test_name> -- --nocapture         # with stdout
cargo test --release --features timing performance_test -- --nocapture  # performance benchmarks (must use --release)
```
3
`.cargo/config.toml` sets `RUST_TEST_THREADS=16` and `rustflags = ["-C", "target-cpu=native"]` automatically.

## CLI Commands

```bash
cargo run --release -- index <file.parquet>       # build index
cargo run --release -- search <file.parquet> <keyword>  # search
cargo run --release -- index_info <file.parquet>  # inspect index metadata
```

## Code Architecture

### Index Structure

An index is stored as a `{parquet_path}.index/` directory containing:
- `filters.rkyv` — bloom filters, metadata, column pool, chunk binary-search table (always loaded into memory)
- `data.bin` — compressed chunked keyword-to-row data (loaded on demand per chunk)

### Core Data Flow

1. **Indexing**: `column_parquet_reader.rs` reads all string columns → `keyword_shred.rs` extracts keywords hierarchically → `index_data.rs` builds bloom filters and serializes chunks
2. **Searching**: Load filters → global bloom filter check → binary search chunk index → load matching chunk → verify parent keywords → optionally read Parquet rows for result data

### Hierarchical Keyword Splitting (4 levels)

Keywords are split at multiple delimiter levels so substrings are independently searchable:
- Level 0: whitespace and structural chars (space, `"`, `(`, `,`, etc.)
- Level 1: path/network chars (`/`, `@`, `=`, `:`, `\`, `?`, `&`)
- Level 2: dot notation (`.`, `$`, `#`, etc.)
- Level 3: word separators (`-`, `_`)

Each split produces both the full token and its sub-tokens, with parent tracking so phrase-like verification is possible without re-reading the Parquet file.

### Column ID 0 Optimization

Column ID `0` is a synthetic aggregate across all columns. Searches without a column filter check only ID 0's bloom filter rather than each column individually.

### Bloom Filter Selection (`index_structure/column_filter.rs`)

- `< 100` unique keywords per column → `RkyvHashSet` (exact, no false positives)
- `>= 100` unique keywords → `BloomFilter` (~1% FPR, space-efficient)

### Key Modules

| File/Module | Role |
|---|---|
| `lib.rs` | Public API: `build_and_save_index`, `search`, etc. |
| `main.rs` | CLI entry point (`index`, `search`, `index_info` subcommands) |
| `keyword_shred.rs` | Hierarchical splitting; `Row` with run-length encoding; parent keyword tracking |
| `column_parquet_reader.rs` | Parquet + Arrow I/O; smart caching (full load <2MB, range requests otherwise); cloud storage via `object_store` |
| `index_data.rs` | Bloom filter construction, rkyv serialization, chunk management |
| `searching/keyword_search.rs` | Main search engine: filter checks, binary search, chunk loading |
| `searching/pruned_reader.rs` | Reads only needed Parquet rows for result verification |
| `searching/search_results.rs` | `SearchResult`, `VerifiedMatches`, result formatting |
| `index_structure/column_filter.rs` | `ColumnFilter` enum (BloomFilter vs RkyvHashSet) |
| `utils/` | Storage abstraction (local + cloud), column name deduplication pool |

### `Row` Type (run-length encoding)

```rust
pub struct Row {
    pub row: u32,
    pub additional_rows: u16,  // consecutive rows with same split pattern
    pub splits_matched: Option<NonZeroU16>,
    pub parent_keyword: Option<Rc<str>>,
}
```

### Test Organization

- `src/unit_tests/` — integration, performance, and real-world tests (NYC Taxi dataset)
- `src/keyword_shred/tests/` — splitting behavior, parent tracking, edge cases
- `src/searching/tests/` — search correctness

Performance comparison tests require `--release` and use DataFusion as the baseline.

### In-memory testing pattern

Tests that build an index from `ParquetSource::Bytes` must save it to the global `MEMORY_STORE` and load from there — **not** use `KeywordSearcher::from_serialized` with a fake path. `from_serialized` only deserialises the filters; `data.bin` is read lazily on the first search call, so the `index_dir` path must resolve at search time.

```rust
// Correct pattern (mirrors build_index_in_memory internally)
let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
let memory_path = format!("memory://test-{}.parquet", ts);
save_distributed_index(&index_files, &memory_path, None).await.unwrap();
let searcher = KeywordSearcher::load(&memory_path, None).await.unwrap();
```

The in-memory store is a global singleton (`MEMORY_STORE`); use unique path prefixes per test to avoid parallel-test interference.

## Non-obvious Design Constraints

### Split elimination requires parent tracking to be set

`check_and_reconsolidate_if_needed` is gated behind `if let Some(parent_threshold) = parent_threshold_count`. **If `parent_tracking_threshold` is `None`, split elimination never runs**, even when `split_elimination_threshold` is `Some`. Always pass both together:

```rust
process_parquet_file(source, ..., Some(threshold), Some(threshold)).await
// or equivalently via build_index_in_memory:
build_index_in_memory(source, ..., Some(threshold), Some(threshold)).await
```

### Two distinct `splits_matched` fields

- **`KeywordOneFile.splits_matched`** (keyword-level): records which split level first derived this keyword. Never nullified — always `Some` for split keywords. Surfaced as `verified_matches.splits_matched` in search results.
- **`Row.splits_matched`** (row-level): set to `None` by split elimination when a keyword's Row object count exceeds the threshold. Surfaced as `range.splits_matched` in `RowRange`.

### Threshold comparison is against Row object count, not row count

After RLE consolidation, N consecutive identical rows become 1 Row object. The split/parent threshold is compared against the Row object count, not the raw row count. A keyword that appears in 1000 consecutive identical rows has a Row object count of 1 and will not trigger elimination unless the threshold is `< 1` (which truncates to 0 as `usize`).

Note that many of the .rs code files also contain tests.

### Row ordering in the aggregate column

When multiple columns are processed sequentially, rows for the same keyword in the aggregate column (column 0) may be appended out of order. For example, col_a contributes rows 0-4, 6-8, then col_b contributes row 5 — row 5 ends up at the end of the Vec, after row 6-8. Both `reconsolidate_column_rows` and `reconsolidate_column_rows_eliminate_splits` sort rows by row number before merging to handle this. Without the sort, the merge silently loses rows that appear out of order.

### SearchMode: Contains vs Equals

The search API supports two modes via the `exact_match` bool parameter on `search()` and `search_and_read()`:
- **Contains** (`exact_match: false`, default): finds the keyword wherever it appears in the index, including as a sub-token from hierarchical splitting. Searching for "1" matches rows where value is "1" AND rows where value is "1.5" (because "1" is a sub-token).
- **Equals** (`exact_match: true`): only matches rows where the column value exactly equals the keyword. Uses `splits_matched` bit 0 on each `RowRange` to distinguish root values from sub-tokens. Rows with `splits_matched = None` (split-eliminated) go to `needs_verification`.

Internally these map to `SearchMode::Contains` and `SearchMode::Equals` in `search_results.rs`.

### `needs_verification` and the pruned reader

`SearchResult` has two result fields: `verified_matches` (confirmed from the index) and `needs_verification` (index can't confirm, requires Parquet read). `PrunedParquetReader::read_search_result` reads both, but for `needs_verification` rows it verifies them against actual Parquet column values using `array_to_string_smart` (the same string conversion used during indexing) and filters out non-matches. This is critical for Equals mode when split elimination has set `splits_matched = None` on the aggregate column.

### Phrase search: the phrase token must be excluded from multi-token search

When `split_phrase("user-name")` produces tokens `["name", "user", "user-name"]`, the token `"user-name"` IS the search phrase itself. This token must be excluded from the multi-token search path for two reasons:
1. It has `parent = None` (root value) while sub-tokens "user" and "name" have `parent = "user-name"`, failing the "all same parent" check
2. Keyword `"user-name"` only exists for rows where the value IS "user-name", so requiring all tokens to exist at the same row excludes containment matches like "user-name-extra"

For Equals mode, a fast path searches for the phrase as a direct keyword before falling through to multi-token search.

### `keyword_only` does NOT mean "exact match"

`keyword_only: true` means "search for this single keyword token in the index" — it finds all rows where the keyword exists, including as a sub-token. It does NOT filter by whether the keyword equals the column value. For exact-value matching, use `exact_match: true`.

NEVER NEVER NEVER write a test that asserts for known incorrect behaviour, the purpose of tests is to ensure code/logic is correct, never to say it is what it is until we fix the code! You are NEVER permitted to break this rule!
