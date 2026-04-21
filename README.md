# Keywords - High-Performance Keyword Search for Parquet Files

**Fast keyword index and search for Parquet files achieving up to 129x speedups over Apache DataFusion, with 4.7x improvement for selective queries on moderate-cardinality data.**

## Executive Summary

High-performance Rust library and CLI tool for building keyword indexes on Parquet files and performing fast searches. Enables searching large files without loading data into memory, with performance exceeding Apache DataFusion where traditional row group statistics provide limited pruning.

## Key Features & Technical Achievements

**Core Implementation**
- **Hierarchical keyword extraction** with 4 levels of delimiter splitting (whitespace → paths → dots → word boundaries)
- **Zero-copy deserialization** using rkyv for memory-efficient operations
- **Run-length encoding** for consecutive rows to reduce memory footprint
- **Adaptive storage strategies**: Automatic selection between bloom filters (large sets) and HashSets (small sets)
- **Parent keyword tracking** enables phrase search verification without reading Parquet data
- **Index 0 optimization**: Aggregates keywords across all columns for efficient unfiltered searches on wide tables

**Production-Oriented Patterns**
- **Comprehensive testing**: 200+ tests across 20+ modules covering edge cases, integration, and performance scenarios
- **Distributed index structure with dynamic chunk sizing** for efficient partial loading

**Tested Capabilities**
- Successfully indexes and searches individual Parquet files
- 4.7x faster than DataFusion for selective queries on moderate-cardinality data
- 7.4x faster when keywords exist separately but not together
- 129x faster when keywords don't exist (bloom filter rejection)
- Indexes approximately 15 million keywords in 4GB RAM (high-cardinality data)

## Project Context

This proof-of-concept was originally developed over 2-3 weeks "out of hours" to explore the foundational architecture for a fast, low-cost multi-petabyte query system capable of running purely from cloud object storage (e.g., AWS S3) without requiring significant expensive local NVMe storage.

Built as a first Rust project using hands-on implementation with advice from AI combined with AI-assisted coding, and most recently using Claude Code.

### Current Status

**Completed:**
- Index and search individual Parquet files efficiently
- Optimized indexing with tested performance characteristics
- Demonstrated concept feasibility for scaling to larger systems

**Future Enhancements** (detailed in Roadmap section):
- **Multi-file index consolidation** - Largest performance improvement potential, pruning thousands of files in 1 index lookup
- Limit detail size for a keyword to control index size
- Case-insensitive search options
- Wildcard and regex support
- Additional statistics for analytical queries
- Integration with query engines (Spark, Trino, Presto)

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd keywords

# Build the project
cargo build --release
```

### Basic Usage

```bash
# Create an index for a Parquet file
keywords index data.parquet

# Search for a keyword
keywords search data.parquet "user@example.com"

# Show help
keywords --help
```

### Library Usage

```rust
use keywords::{build_and_save_index, search, search_and_read};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build an index (all optional parameters set to None for defaults)
    build_and_save_index("data.parquet", None, None, None, None, None, None, None, None, None, None).await?;

    // Search for a keyword (contains mode — finds "example" even in "user@example.com")
    let result = search("data.parquet", "example", None, true, false).await?;

    if let Some(data) = result.verified_matches {
        println!("Found in columns: {:?}", data.columns);
        println!("Total occurrences: {}", data.total_occurrences);
    }

    // Search with exact match (equals mode — only finds rows where value IS "example")
    let result = search("data.parquet", "example", None, true, true).await?;

    // Search and read matching rows from the Parquet file in one call
    let (result, batches) = search_and_read("data.parquet", "example", None, true, false).await?;
    println!("Read {} matching rows", batches.iter().map(|b| b.num_rows()).sum::<usize>());

    Ok(())
}
```

---

## Architecture

### Index Structure

The index is stored as a `.index` directory adjacent to the Parquet file:

```
data.parquet
data.parquet.index/
├── filters.rkyv          # Bloom filters, metadata, column pool, and chunk index
└── data.bin              # Compressed chunked keyword lists and occurrence data
```

### Core Components

#### 1. **Keyword Extraction** (`keyword_shred.rs`)
- Hierarchical splitting with 4 delimiter levels
- Parent keyword tracking for phrase search
- Run-length encoding for consecutive rows
- Efficient memory usage with `SmallVec` and `Rc<str>`

#### 2. **Index Building** (`index_data.rs`, `column_parquet_reader.rs`)
- Processes all string columns from Parquet files
- Creates bloom filters for fast existence checks
- Builds distributed index structure
- Designed for both local and cloud (S3) storage

#### 3. **Search Engine** (`searching/keyword_search.rs`)
- Global bloom filter for quick rejection
- Binary search for chunk location
- Exact match within chunks
- Parent keyword verification for phrases
- Two search modes via `SearchMode`: Contains (default, matches sub-tokens) and Equals (matches only exact column values using `splits_matched` bit 0 filtering)

#### 3b. **Pruned Reader** (`searching/pruned_reader.rs`)
- Reads only the Parquet rows identified by the search
- Deduplicates overlapping row ranges across columns
- Verifies `needs_verification` rows against actual Parquet column values using the same string conversion as indexing

#### 4. **Column Filters** (`index_structure/column_filter.rs`)
- Automatic selection between Bloom filter and HashSet
- Configurable false positive rate (default 1%)
- Space-efficient for large keyword sets

### Index 0 Optimization (Global Aggregate)

The index includes a special column_id 0 that aggregates keyword occurrences across **ALL** columns. This enables efficient searching when you don't know which column contains a keyword.

**Benefits:**
- Eliminates need to check bloom filters for every column
- Single lookup determines if keyword exists anywhere in file
- Particularly valuable for files with many columns

### Hierarchical Keyword Splitting

Keywords are split across 4 delimiter levels in sequence:

1. **Level 0**: Whitespace & structural: ` `, `\r`, `\n`, `\t`, `'`, `"`, `<`, `>`, `(`, `)`, `|`, `,`, `!`, `;`, `{`, `}`, `*`
2. **Level 1**: Path/network: `/`, `@`, `=`, `:`, `\`, `?`, `&`
3. **Level 2**: Dot notation: `.`, `$`, `#`, `` ` ``, `~`, `^`, `+`
4. **Level 3**: Word separators: `-`, `_`

**Example: "user@example.com"**
```
Level 0: No split → continue
Level 1: Split on @ → ["user@example.com", "user", "example.com"]
Level 2: "example.com" splits on . → ["example.com", "example", "com"]
Level 3: No further splits
Final keywords: ["user@example.com", "user", "example.com", "example", "com"]
```

This hierarchical approach enables:
- Searching for complete 'words': `"user@example.com"`
- Searching for components: `"example"` or `"com"`
- Efficient phrase matching using parent tracking
- Does not bloat by storing entire sentences, full json objects or full pieces of xml

### Performance Characteristics

**Index Building:**
- Memory: Proportional to unique keyword count (~15 million keywords in 4GB RAM for high-cardinality data)
- Single-threaded but I/O optimized

**Searching:**
- Time complexity: O(1) for bloom filter check + O(log n) for binary search
- Search time independent of Parquet file size
- Performance exceeds Apache DataFusion where row group statistics provide limited pruning
- Verified matches require no Parquet file access (uses parent tracking)

**Memory Usage:**
- Indexing: Proportional to unique keyword count
- Searching: Only index loaded, not Parquet data


### Example Performance Results

Testing on representative hardware with a 500,000-row Parquet file containing 5,000 random values across 10 columns (see [`test_performance_with_debug`](src/unit_tests/performance_test.rs) for full test implementation):

```
┌─────────────────────────────────┬──────────────┬──────────────┐
│ Approach                        │ Time         │ Speedup      │
├─────────────────────────────────┼──────────────┼──────────────┤
│ Keyword Index (this project)    │      10.13ms │ baseline     │
│ DataFusion (pushdown + pruning) │      47.81ms │ 4.72x faster │
│ Naive (read all, filter)        │      68.34ms │ 6.75x faster │
└─────────────────────────────────┴──────────────┴──────────────┘
```

**Key Observations:**
- Keyword index: **4.7x faster** than Apache DataFusion for this workload
- DataFusion and naive approaches show similar performance (19ms vs 21ms), demonstrating that row group statistics provide minimal pruning benefit for higher-cardinality random data
- Pre-computed bloom filters eliminate the runtime cost of statistics evaluation
- Absolute times vary based on hardware, file size, and data characteristics
- Entire file pruning at the bloom filter stage or through row combination (filtering values in multiple columns where values exist, but not in the same row) is much faster

**Test Details:**
- Comparison includes Apache DataFusion 45.0 (industry-standard query engine with automatic row group pruning)
- Three-way comparison validates that traditional optimization techniques (row group statistics) are ineffective for this data pattern
- Full test code available in [`src/unit_tests/performance_test.rs`](src/unit_tests/performance_test.rs)

**When These Results Apply:**
- High-cardinality data (many unique values per column)
- Random or unstructured data distributions (logs, JSON fields, user-generated content)
- Selective keyword searches (finding specific values, not aggregations)
- Scenarios where row group statistics provide limited pruning opportunities

**Note:** These are representative results from development testing. The keyword index advantage is most pronounced for selective queries on higher-cardinality, unstructured data where traditional statistics-based pruning is ineffective. Sorted or clustered data may favor traditional query engines.

### Other Performance Results

Full test code available in [performance_comparison_test.rs](src/unit_tests/performance_comparison_test.rs)

**By Parquet Compression Algorithm**

| Compression | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |
|-----------|-----------|-------------|---------------|------------|---------|-------|------|
| GZIP-9 | 10.88 MB | 3.4199817s | 9.8619ms | 47.1435ms | 4.78x | 81.2882ms | 1 |
| ZSTD-18 | 10.74 MB | 3.9432419s | 7.897ms | 41.3682ms | 5.24x | 77.215ms | 1 |
| SNAPPY | 12.06 MB | 3.623418s | 6.1355ms | 29.0629ms | 4.74x | 64.9799ms | 1 |
| LZ4 | 12.26 MB | 3.4841745s | 5.8859ms | 28.047ms | 4.77x | 64.6821ms | 1 |
| BROTLI-9 | 10.75 MB | 3.5646943s | 12.0096ms | 89.1047ms | 7.42x | 127.2695ms | 1 |
| UNCOMPRESSED | 12.32 MB | 3.3711396s | 5.3076ms | 26.863ms | 5.06x | 65.1426ms | 1 |

**By Number of Row Groups**

| Row Groups | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |
|----------|-----------|-------------|---------------|------------|---------|-------|------|
| 1 RG | 8.63 MB | 3.0620407s | 8.1927ms | 37.7718ms | 4.61x | 61.7811ms | 1 |
| 2 RG | 9.48 MB | 3.1185151s | 6.6753ms | 35.5222ms | 5.32x | 62.5651ms | 1 |
| 3 RG | 10.32 MB | 3.0907206s | 5.7233ms | 47.5411ms | 8.31x | 65.2361ms | 1 |
| 4 RG | 11.20 MB | 3.1808725s | 5.4951ms | 28.5979ms | 5.20x | 65.4227ms | 1 |
| 5 RG | 12.03 MB | 3.2585228s | 5.9572ms | 29.8952ms | 5.02x | 65.9698ms | 1 |
| 6 RG | 12.87 MB | 3.4324421s | 5.8792ms | 31.4059ms | 5.34x | 66.474ms | 1 |
| 7 RG | 13.76 MB | 3.5064593s | 5.8601ms | 30.2749ms | 5.17x | 83.5822ms | 1 |
| 8 RG | 14.61 MB | 3.5183013s | 6.1231ms | 33.9602ms | 5.55x | 68.6671ms | 1 |
| 9 RG | 15.42 MB | 3.560135s | 6.1496ms | 32.465ms | 5.28x | 69.6454ms | 1 |
| 10 RG | 16.30 MB | 3.6291479s | 6.2515ms | 33.1341ms | 5.30x | 70.7447ms | 1 |
| 20 RG | 24.72 MB | 4.114057s | 6.8005ms | 39.8315ms | 5.86x | 79.6355ms | 1 |
| 30 RG | 32.54 MB | 4.4996789s | 7.1707ms | 47.0762ms | 6.57x | 88.1797ms | 1 |
| 40 RG | 39.09 MB | 4.8473155s | 7.1574ms | 54.5048ms | 7.62x | 96.4199ms | 1 |
| 50 RG | 44.54 MB | 5.1684349s | 6.53ms | 60.141ms | 9.21x | 102.7322ms | 1 |

**By Cardinality (number of random strings in the pool used to build the parquet file)**

| Pool Size | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |
|---------|-----------|-------------|---------------|------------|---------|-------|------|
| 50 | 3.66 MB | 1.9747809s | 15.5858ms | 26.1259ms | 1.68x | 62.7014ms | 4 |
| 500 | 5.82 MB | 2.2467061s | 4.088ms | 23.4609ms | 5.74x | 55.2885ms | 1 |
| 5000 | 12.05 MB | 3.2037869s | 6.2443ms | 27.7583ms | 4.45x | 63.4311ms | 1 |
| 50000 | 46.11 MB | 7.0693962s | 16.4559ms | 57.0207ms | 3.47x | 114.8148ms | 1 |
| 500000 | 87.67 MB | 14.356577s | 28.0444ms | 100.3602ms | 3.58x | 160.536ms | 1 |

**Where all keywords exist within columns, but not together in any row**

| Method | Time | Rows Found |
|--------|------|------------|
| Keyword Index | 4.0108ms | 0 |
| DataFusion | 29.7844ms | 0 |
| Naive | 66.1238ms | 0 |

An **7.4x** speedup

**Where keywords do not exist in the data at all (bloom filter rejection)**

| Method | Time | Rows Found |
|--------|------|------------|
| Keyword Index | 396.3µs | 0 |
| DataFusion | 51.254ms | 0 |
| Naive | 81.5495ms | 0 |

A **129x** speedup

---

## Features

### Fast Indexing
- Processes all string columns from Parquet files
- Optimized I/O with minimal memory allocations
- Smart file reading: complete caching for files <2MB
- Efficient metadata extraction with range requests for large files

### Fast Searches
- Bloom filter-based existence checks (configurable false positive rate)
- Binary search to locate data chunks
- Parent keyword verification without Parquet access
- Configurable column filtering
- **Two search modes**: Contains (find keyword anywhere, including as a sub-token) and Equals (find only rows where the column value exactly matches the keyword)
- **End-to-end search+read**: `search_and_read` queries the index and reads matching Parquet rows in one call, with automatic verification of `needs_verification` rows against actual column values

### Phrase Search
- Multi-token phrase matching using parent relationships
- Can verify some multi-token matches without reading the Parquet file
- When the search phrase itself is stored as a keyword, the search uses a direct keyword lookup (fast path) rather than multi-token parent verification

### Cloud Storage Support
- Architecture designed for cloud storage (S3, Azure, GCP via `object_store` crate)
- Automatic retry logic for transient failures
- Efficient range requests for metadata
- Basic S3 read functions tested; full cloud indexing and searching designed but not extensively tested

### Validation
- Index validation checks file size, ETag, and last modified time
- Prevents stale index usage
- Clear error messages for missing or outdated indexes

---

## Dependencies

Key dependencies used in this project:

- **arrow** (57.0.0): Arrow data format integration
- **bytes** (1.11.0): Efficient byte buffer management
- **dashmap** (6.1.0): Concurrent hash map
- **futures** (0.3.31): Async programming primitives
- **hashbrown** (0.16.0): High-performance hash maps
- **indexmap** (2.12.0): Order-preserving hash maps
- **object_store** (0.12.4): Cloud storage abstraction (S3, Azure, GCP)
- **once_cell** (1.21.3): Single assignment cells for lazy initialization
- **parquet** (57.0.0): Parquet file format support
- **rand** (0.9.2): Random number generation for testing
- **rkyv** (0.8.12): Zero-copy deserialization
- **smallvec** (1.15.1): Stack-allocated vectors for common small cases
- **tokio** (1.48.0): Async runtime

See `Cargo.toml` for complete dependency list with versions.

---

## Project Structure

```
keywords/
├── src/
│   ├── main.rs                    # CLI entry point
│   ├── lib.rs                     # Public library API
│   ├── keyword_shred.rs           # Hierarchical keyword extraction
│   ├── column_parquet_reader.rs   # Parquet file processing
│   ├── index_data.rs              # Index building and serialization
│   ├── index_structure/
│   │   ├── column_filter.rs       # Bloom filter implementation
│   │   ├── index_files.rs         # Index file path management
│   │   └── mod.rs
│   ├── searching/
│   │   ├── keyword_search.rs      # Search implementation
│   │   ├── search_results.rs      # Search result types
│   │   ├── pruned_reader.rs       # Optimized Parquet reading
│   │   └── tests/                 # Search-specific tests
│   ├── utils/
│   │   ├── column_pool.rs         # Column name deduplication
│   │   ├── file_interaction_local_and_cloud.rs  # Storage abstraction
│   │   └── mod.rs
│   ├── unit_tests/                # Integration and performance tests
│   └── keyword_shred/tests/       # Hierarchical keyword extraction tests
├── Cargo.toml
├── COPYRIGHT.txt
└── README.md
```

---

## Limitations

Current implementation has the following constraints (appropriate for POC phase):

- **Case-sensitive**: "Email" and "email" are treated as different keywords
- **Exact token match**: No wildcard support; searches match tokens as split during indexing
- **Memory bound during indexing**: Entire index must fit in memory during construction. No disk spooling during index build.
- **Memory bound during searching**: Entire index must fit in memory
- **Single-threaded indexing**: Indexing is not parallelized (but I/O is optimized)
- **No incremental updates**: Index must be rebuilt if Parquet file changes
- **Numeric precision**: Non-string columns are stringified during indexing (whole-number floats lose their trailing `.0`); use exact-match searches with care for floats

---

## Index Size Management

### Challenge

High-cardinality columns with many rows per keyword can create excessively large indexes that provide minimal performance benefit over direct Parquet filtering. When a keyword appears in many rows, storing detailed row-level information for each occurrence may cause the index to grow disproportionately to the benefit provided.

### What's Implemented

A threshold-based approach that gracefully degrades behavior for high-frequency keywords:

**1. Row Information Thresholds** (complete)
- Keywords below threshold: Store full row details with parent tracking and split-level information
- Keywords above threshold: Parent tracking and split-level information eliminated, rows merged aggressively via RLE
- Configurable via `parent_tracking_threshold` and `split_elimination_threshold` parameters

**2. Search Behavior Adaptation** (complete)
- **Selective keywords** (below threshold): Use indexed row details for instant verified results
- **Common keywords** (above threshold): Row-level `splits_matched` set to `None`, search classifies these as `needs_verification`
- **Automatic verification**: `search_and_read` reads `needs_verification` rows from the Parquet file and verifies them against actual column values
- **Clear result indication**: `SearchResult` distinguishes between `verified_matches` (confirmed from index) and `needs_verification` (requires Parquet read)

**3. Contains vs Equals search modes** (complete)
- Contains mode (default): Finds keywords wherever they appear, including as sub-tokens of split values
- Equals mode: Only matches rows where the column value exactly equals the search term, using `splits_matched` bit 0 filtering

### What's Still Needed

**4. Per-column data reduction for eliminated keywords** (future)
- When split elimination fires on the aggregate column (column 0), per-column row data is still stored in full in `data.bin`, leading to large chunk sizes for high-frequency keywords (e.g. 6MB for keyword "1" in NYC Taxi data)
- Stripping per-column row data for keywords where the aggregate has been split-eliminated would dramatically reduce `data.bin` size

**5. Index size vs Parquet size ratio** (future)
- For numeric-heavy data where most values become searchable keywords, the index can exceed the Parquet file size
- Further work is needed on selective indexing strategies and storage trade-offs to keep the index proportional

### Implementation Status

**Current State**: Threshold-based parent tracking and split elimination are fully implemented via the `parent_tracking_threshold` and `split_elimination_threshold` parameters on `build_and_save_index` and `build_index_in_memory`.

**How It Works**:
- `parent_tracking_threshold`: A fraction (0.0–1.0) of total rows. Keywords whose Row object count exceeds `threshold × total_rows` have their per-row parent references (`Row.splits_matched`) nullified, causing phrase searches to fall back to a `needs_verification` path rather than being resolved entirely from the index.
- `split_elimination_threshold`: Same fraction; keywords exceeding this count lose their row-level split detail but are still found by exact-token searches.
- Both thresholds must be set together — setting only one has no effect (split elimination is gated on the parent threshold check).

**When to Enable**:
- High-frequency keywords (e.g. common words like "the", "active") cause large Row lists that dominate index size.
- Set both thresholds to e.g. `Some(0.01)` (1% of rows) to cap row detail for very common keywords.

```rust
build_and_save_index(
    "data.parquet",
    None, None, None, None, None, None, None, None,
    Some(0.01),  // parent_tracking_threshold
    Some(0.01),  // split_elimination_threshold
).await?;
```

---

## Advanced Features

### Parent Keyword Tracking

Keywords maintain references to their parent keywords in the split hierarchy. This enables efficient phrase search without reading the Parquet file.

### Bloom Filter Configuration

Customize the false positive rate during index creation:

```rust
use keywords::build_and_save_index;
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build index with 0.1% false positive rate (lower = more memory)
    let error_rate = Some(0.001);
    let exclude_columns = None;

    build_and_save_index("data.parquet", exclude_columns, error_rate, None, None, None, None, None, None, None, None).await?;

    Ok(())
}
```

### Excluding Columns

Exclude specific columns from indexing to reduce index size and improve performance. Consider excluding:

- **Unique identifiers**: Columns like `id`, `uuid`, `transaction_id` that are never searched by keyword
- **High cardinality, low-value columns**: Data that's unique per row but not useful for search (e.g., raw JSON blobs, internal codes)
- **Time fields**: Timestamps and dates are typically queried by range, not exact match, making keyword indexing inefficient
- **Binary or encoded data**: Base64 strings, hashes, or other encoded values

```rust
use keywords::build_and_save_index;
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut exclude = HashSet::new();
    exclude.insert("id".to_string());               // Unique identifier
    exclude.insert("user_uuid".to_string());        // High cardinality, never searched
    exclude.insert("created_at".to_string());       // Time field (use range queries)
    exclude.insert("updated_at".to_string());       // Time field
    exclude.insert("internal_metadata".to_string()); // Raw JSON, not useful for keyword search

    build_and_save_index("data.parquet", Some(exclude), None, None, None, None, None, None, None, None, None).await?;

    Ok(())
}
```

**Why exclude these columns?**
- Reduces index size (less memory needed)
- Faster indexing (fewer keywords to process)
- Better search performance (smaller bloom filters)
- Focuses index on columns that benefit from keyword search

---

## Parallelization Strategy

Single-threaded was chosen for the POC phase to reduce complexity while establishing baseline performance and only dealing with single files.

---

## Roadmap / Future Improvements

**Near-term Enhancements:**
- Case-insensitive search option (store keywords normalized)
- Wildcard support (trailing wildcards initially)
- Metadata caching within index (eliminate additional GET requests)
- Improved delimiter configuration
- Row information threshold for high-frequency keywords (see Index Size Management section)

**Index Scaling:**
- Multi-file index consolidation at partition level (e.g., daily aggregates)
- Hierarchical index structure with range metadata
- Compaction process for late-arriving data
- Spill-to-disk strategy for large files (temporary partitioned files during indexing)

**Query Capabilities:**
- Leading wildcard support via reverse index
- SQL-like query interface (investigate sqlparser-rs)
- Complex boolean queries (AND, OR, NOT operations)
- Pattern extraction (IP addresses, email domains, etc.)
- Abstract entity searching (hostname/domain matching, CIDR IP matching)

**Performance & Scale:**
- Parallel indexing (multi-threaded column processing)
- Parallel searching
- Additional statistics storage for analytical queries
- Integration with distributed query engines (Spark, Trino, Presto)
- Iceberg table format integration

**Additional Formats:**
- Line-separated JSON
- CSV with schema detection
- Arbitrary text with regex field extraction
- Plain text with line-based processing

**Testing & Validation:**
- ~~Test on large public datasets (e.g., NYC Taxi Trip Data)~~ Completed: NYC Yellow Taxi April 2020 data tested with DataFusion comparison
- Comprehensive benchmarking suite
- Memory profiling and optimization
- Cost analysis (storage vs compute trade-offs)

---

## FAQ

**Q: How much memory does searching use?**  
A: The entire index is loaded into memory during search. The actual Parquet data is not loaded unless verification is needed.

**Q: Can I update an existing index?**  
A: No. You must rebuild the entire index if the Parquet file changes.

**Q: Is the search thread-safe?**  
A: `KeywordSearcher` is not `Sync` due to internal buffer management. Create one instance per thread or use appropriate synchronization primitives.

**Q: What about transient network failures when accessing cloud storage?**  
A: The `object_store` library includes built-in retry logic with exponential backoff for handling transient failures automatically.

**Q: How do I validate my index is up-to-date?**  
A: Use the `validate_index()` function which checks file size, ETag, and last modified time.

---

Copyright (c) 2025 Andrew Smith. All rights reserved. See [COPYRIGHT.txt](COPYRIGHT.txt) for full terms.