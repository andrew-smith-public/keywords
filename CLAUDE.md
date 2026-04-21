# CLAUDE.md

Guidance for Claude Code working in this repository.

## Rules

1. **Never write a test that asserts incorrect behaviour.** Tests exist to
   catch bugs, not document them. If a test fails because the code is
   wrong, fix the code — don't pin the test to the wrong output.
2. **Enable `--features timing` when running tests.** It compiles out when
   disabled; keeping it on makes hot-path perf regressions visible.
3. **Heavy tests are gated behind `#[cfg_attr(feature = "ci", ignore)]`.**
   CI runs `--features ci` to skip them. Don't add `--features ci` locally.

## What keywords does

Rust library + CLI that builds a keyword index over a Parquet file and
serves fast keyword lookups. Large speedups over Apache DataFusion via
hierarchical keyword extraction, bloom-filter prechecks, rkyv zero-copy
deserialization, and chunked on-demand loading. Designed for cloud object
storage (S3) and to scale to millions of Parquet files cost-effectively.

## Build & test

```bash
cargo build                                   # debug
cargo build --release                         # release (target-cpu=native)
cargo check   --features timing               # fast type-check
cargo fmt
cargo clippy  --features timing
cargo test    --features timing               # all tests
cargo test    --features timing <name>        # single test
cargo test    --features timing <name> -- --nocapture
cargo test    --release --features timing performance_test -- --nocapture
cargo test    --features ci                   # what CI runs
```

`.cargo/config.toml` pins `RUST_TEST_THREADS=16` and `-C target-cpu=native`.

## CLI

```bash
cargo run --release -- index      <file.parquet>
cargo run --release -- search     <file.parquet> <keyword>
cargo run --release -- index_info <file.parquet>
```

## Architecture

### Index layout

An index lives at `{parquet_path}.index/`:

- `filters.rkyv` — bloom filters, metadata, column pool, chunk
  binary-search table. Always memory-resident.
- `data.bin` — compressed chunked keyword→row data. Loaded lazily per
  chunk on first search.

### Core flow

- **Index:** `column_parquet_reader.rs` reads *every* column (non-strings
  are stringified via `array_to_string_smart`: whole-number floats get
  `"1"` instead of `"1.0"`, everything else casts to Utf8) →
  `keyword_shred.rs` splits hierarchically → `index_data.rs` builds
  bloom filters and serialises chunks.
- **Search:** load filters → global bloom filter → binary-search chunk
  index → load matching chunk → verify parent keywords → (optional)
  read matching Parquet rows.

### Hierarchical splitting (4 levels)

Each value is split at progressively weaker delimiters. Every sub-token
is searchable, and parent tracking lets phrase-like verification happen
without re-reading Parquet.

| Level | Delimiters                        | Purpose               |
|-------|-----------------------------------|-----------------------|
| 0     | space, `"`, `(`, `,`, …           | whitespace/structural |
| 1     | `/`, `@`, `=`, `:`, `\`, `?`, `&` | path / network        |
| 2     | `.`, `$`, `#`, …                  | dot notation          |
| 3     | `-`, `_`                          | word separators       |

### Column-ID 0 (global aggregate)

Column 0 is a synthetic aggregate across all columns. Searches without
a column filter hit only column 0's bloom filter instead of every
column's.

### Bloom filter vs HashSet (`index_structure/column_filter.rs`)

- `< 100` unique keywords per column → `RkyvHashSet` (exact).
- `≥ 100` → `BloomFilter` (~1% FPR).

### Key modules

| Module | Role |
|---|---|
| `lib.rs` | Public API: `build_and_save_index`, `search`, `search_and_read` |
| `main.rs` | CLI (`index`, `search`, `index_info`) |
| `keyword_shred.rs` | Hierarchical split, `Row` RLE, parent tracking |
| `column_parquet_reader.rs` | Parquet + Arrow I/O; smart caching (<2 MB full-load, else range requests); cloud via `object_store` |
| `index_data.rs` | Bloom filter build, rkyv serialisation, chunking |
| `searching/keyword_search.rs` | Filter checks, binary search, chunk loading |
| `searching/pruned_reader.rs` | Reads only the Parquet rows needed for verification |
| `searching/search_results.rs` | `SearchResult`, `VerifiedMatches` |
| `index_structure/column_filter.rs` | `ColumnFilter` enum |
| `utils/` | Local+cloud storage abstraction, column-name pool |

### `Row` (run-length encoded)

```rust
pub struct Row {
    pub row: u32,
    pub additional_rows: u16,              // consecutive same-pattern rows
    pub splits_matched: Option<NonZeroU16>,
    pub parent_keyword: Option<Rc<str>>,
}
```

## Testing

### Organisation

- `src/unit_tests/` — integration, perf, real-world (NYC Taxi) tests.
- `src/keyword_shred/tests/` — splitting, parent tracking, edge cases.
- `src/searching/tests/` — search correctness.
- Many source files also carry `#[cfg(test)]` modules.

Performance comparisons use DataFusion as the baseline; run them
`--release` to get meaningful numbers.

### In-memory tests

Tests that build an index from `ParquetSource::Bytes` must save it to
the global `MEMORY_STORE` and load from there. Do **not** call
`KeywordSearcher::from_serialized` with a fake path: that only
deserialises the filters; `data.bin` is read lazily on first search,
so the `index_dir` path has to resolve at search time.

```rust
let ts = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
let memory_path = format!("memory://test-{}.parquet", ts);
save_distributed_index(&index_files, &memory_path, None).await.unwrap();
let searcher = KeywordSearcher::load(&memory_path, None).await.unwrap();
```

`MEMORY_STORE` is a global singleton — use unique path prefixes per
test to avoid parallel-test interference.

## Gotchas

### Split elimination is gated on parent tracking

`check_and_reconsolidate_if_needed` only runs when
`parent_tracking_threshold` is `Some`. Passing
`split_elimination_threshold` alone does nothing. Always pass both:

```rust
process_parquet_file(source, …, Some(t), Some(t)).await
build_index_in_memory(source, …, Some(t), Some(t)).await
```

### Two different `splits_matched` fields

| Field | Level | Nullifiable? | Surfaced as |
|---|---|---|---|
| `KeywordOneFile.splits_matched` | keyword | no — always `Some` for split keywords | `verified_matches.splits_matched` |
| `Row.splits_matched`            | row     | yes — nulled by split elimination     | `range.splits_matched` on `RowRange` |

### Thresholds compare against *Row-object* count, not row count

After RLE, N consecutive identical rows collapse to one `Row`. The
split/parent threshold fires on `Row` count, not raw row count. A
keyword in 1 000 consecutive identical rows has Row-object count = 1
and won't trigger elimination unless the threshold is `< 1` (which
truncates to `0 as usize`).

### Rows in the aggregate column can arrive out of order

When columns are processed sequentially, rows for the same keyword in
column 0 may be appended out of order: col_a adds rows 0–4 then 6–8,
then col_b adds row 5 — row 5 lands after 6–8 in the Vec. Both
`reconsolidate_column_rows` and
`reconsolidate_column_rows_eliminate_splits` sort by row number before
merging. Without the sort, out-of-order rows are silently lost.

## Search API semantics

`search()` and `search_and_read()` take three flags; two are subtle.

| Flag                       | Effect                                       |
|----------------------------|----------------------------------------------|
| `in_columns: Option<&str>` | Restrict to one column.                      |
| `keyword_only: bool`       | Search one token as-is; skip phrase/multi-token decomposition. Still matches sub-tokens — does **not** imply column-value equality. |
| `exact_match: bool`        | `SearchMode::Equals` (`true`) vs `Contains` (`false`). |

**Contains vs Equals:**

- **Contains** (`exact_match: false`, default): matches wherever the
  keyword appears, including as a sub-token. `"1"` matches rows where
  the value is `"1"` *and* rows where the value is `"1.5"`.
- **Equals** (`exact_match: true`): only matches rows where the column
  value equals the keyword. Uses `splits_matched` bit 0 on each
  `RowRange`; rows with `splits_matched = None` (split-eliminated) go
  to `needs_verification`.

For exact-value matching, use `exact_match: true`, not
`keyword_only: true`.

### `needs_verification` and the pruned reader

`SearchResult` has two result bags:

- `verified_matches` — confirmed from the index alone.
- `needs_verification` — the index can't confirm; must read Parquet.

`PrunedParquetReader::read_search_result` reads both. For
`needs_verification`, it re-verifies each row against the actual column
value via `array_to_string_smart` (the same conversion used at index
time) and drops non-matches. Critical for Equals mode when split
elimination has nullified `splits_matched` on column 0.

### Phrase search excludes the phrase token itself

`split_phrase("user-name")` produces `["name", "user", "user-name"]`.
The token `"user-name"` *is* the phrase and must be dropped from the
multi-token path, because:

1. It has `parent = None` (root) while the sub-tokens have
   `parent = "user-name"` — fails the "all same parent" check.
2. Keyword `"user-name"` only exists on rows where the value *is*
   `"user-name"`, so requiring it excludes containment matches like
   `"user-name-extra"`.

In Equals mode, a fast path looks up the phrase as a direct keyword
before falling through to multi-token search.
