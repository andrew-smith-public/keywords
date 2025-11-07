# Keywords - High-Performance Keyword Search for Parquet Files

**A keyword index system POC, currently achieving 2.5x faster searches than Apache DataFusion for selective queries on high-cardinality data.**

Copyright (c) 2025 Andrew Smith. All rights reserved. See [COPYRIGHT.txt](COPYRIGHT.txt) for full terms.

## Executive Summary

High-performance Rust library and CLI tool for building keyword indexes on Parquet files and performing fast keyword searches. The system enables searching multi-gigabyte files without loading data into memory, with performance exceeding industry-standard query engines like Apache DataFusion for high-cardinality data where traditional row group statistics provide limited pruning opportunities.

## Key Features & Technical Achievements

**Core Implementation**
- **Hierarchical keyword extraction** with 4 levels of delimiter splitting (whitespace → paths → dots → word boundaries)
- **Zero-copy deserialization** using rkyv for memory-efficient operations
- **Run-length encoding** for consecutive rows to reduce memory footprint
- **Adaptive storage strategies**: Automatic selection between bloom filters (large sets) and HashSets (small sets)
- **Parent keyword tracking** enables phrase search verification without reading Parquet data
- **Index 0 optimization**: Aggregates keywords across all columns for efficient unfiltered searches on wide tables

**Production-Oriented Patterns**
- **Cloud and local support**: Seamless operation with S3 and local filesystems via unified interface
- **Comprehensive testing**: 13 test modules covering edge cases, integration, and performance scenarios
- **Distributed index structure with configurable chunk size** for efficient partial loading
- **Centralized configuration**: All tunable parameters in dedicated config module

**Proven Capabilities**
- Successfully indexes and searches individual Parquet files efficiently
- Search performance competitive with Apache DataFusion for selective queries on high-cardinality data
- Can index approximately 15 million keywords in 4GB RAM for high cardinality data
- Optimized indexing with multiple performance enhancements

## Project Context

This proof-of-concept was developed over approximately 2 weeks to explore the foundational architecture for a fast, low-cost multi-petabyte query system capable of running purely from cloud object storage (e.g., AWS S3) without requiring expensive local NVME storage.

**Development approach**: Hands-on implementation with AI-assisted learning for Rust-specific patterns (first Rust project)

### Current Status

**Completed:**
- Index and search individual Parquet files efficiently
- Optimized indexing with tested performance characteristics
- Demonstrated concept feasibility for scaling to larger systems
- Comprehensive configuration management system

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
use keywords::{build_and_save_index, search};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build an index
    build_and_save_index("data.parquet", None, None, None).await?;

    // Search for a keyword
    let result = search("data.parquet", "example", None, true).await?;

    if let Some(data) = result.verified_matches {
        println!("Found in columns: {:?}", data.columns);
        println!("Total occurrences: {}", data.total_occurrences);
    }

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
├── filters.rkyv          # Bloom filters, metadata, column pool
├── metadata.rkyv         # Keyword metadata chunks
├── data.bin              # Serialized keyword data
└── column_keywords.rkyv  # Column-to-keyword mappings
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
- Supports both local and cloud (S3) storage

#### 3. **Search Engine** (`searching/keyword_search.rs`)
- Global bloom filter for quick rejection
- Binary search for chunk location
- Exact match within chunks
- Parent keyword verification for phrases

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
- Memory: ~15 million keywords in 4GB RAM (high cardinality data, normal length keywords)
- Lower cardinality data can fit more keywords in same memory
- Single-threaded but I/O optimized

**Searching:**
- Time complexity: O(1) for bloom filter check + O(log n) for binary search
- Search time independent of Parquet file size
- Performance competitive with Apache DataFusion, with advantages for high-cardinality keyword lookups where row group statistics provide limited pruning
- Verified matches require no Parquet file access (uses parent tracking)

**Memory Usage:**
- Indexing: Proportional to unique keyword count
- Searching: Only index loaded, not Parquet data


### Example Performance Results

Testing on representative hardware with a 100,000-row Parquet file containing random high-cardinality data across 10 columns (see [`test_performance_comparison`](src/unit_tests/performance_test.rs) for full test implementation):

```
┌─────────────────────────────────┬──────────────┬──────────┐
│ Approach                        │ Time         │ Speedup  │
├─────────────────────────────────┼──────────────┼──────────┤
│ Keyword Index (this project)    │       7.83ms │ baseline │
│ DataFusion (pushdown + pruning) │      19.36ms │ 2.47x    │
│ Naive (read all, filter)        │      20.75ms │ 2.65x    │
└─────────────────────────────────┴──────────────┴──────────┘
```

**Key Observations:**
- Keyword index: **2.5x faster** than Apache DataFusion for this workload
- DataFusion and naive approaches show similar performance (19ms vs 21ms), demonstrating that row group statistics provide minimal pruning benefit for high-cardinality random data
- Pre-computed bloom filters eliminate the runtime cost of statistics evaluation
- Absolute times vary based on hardware, file size, and data characteristics
- Entire file pruning at the bloom filter stage is much faster

**Test Details:**
- Comparison includes Apache DataFusion 45.0 (industry-standard query engine with automatic row group pruning)
- Three-way comparison validates that traditional optimization techniques (row group statistics) are ineffective for this data pattern
- Full test code available in [`src/unit_tests/performance_test.rs`](src/unit_tests/performance_test.rs)

**When These Results Apply:**
- High-cardinality data (many unique values per column)
- Random or unstructured data distributions (logs, JSON fields, user-generated content)
- Selective keyword searches (finding specific values, not aggregations)
- Scenarios where row group statistics provide limited pruning opportunities

**Note:** These are representative results from development testing. The keyword index advantage is most pronounced for selective queries on high-cardinality, unstructured data where traditional statistics-based pruning is ineffective. Sorted or clustered data may favor traditional query engines.

---

## Features

### Fast Indexing
- Processes all string columns from Parquet files
- Optimized I/O with minimal memory allocations
- Smart file reading: complete caching for files <2MB
- Efficient metadata extraction with range requests for large files

### Fast Searches
- Bloom filter-based existence checks (configurable false positive rate)
- Binary search within metadata chunks
- Parent keyword verification without Parquet access
- Configurable column filtering

### Phrase Search
- Multi-token phrase matching using parent relationships
- Can verify some multi-token matches without reading the Parquet file

### Cloud Storage Support
- Works seamlessly with S3 (via `object_store` crate)
- Automatic retry logic for transient failures
- Efficient range requests for metadata

### Validation
- Index validation checks file size, ETag, and last modified time
- Prevents stale index usage
- Clear error messages for missing or outdated indexes

---

## Dependencies

Key dependencies used in this project:

- **arrow** (57.0.0): Arrow data format integration
- **bytes** (1.10.1): Efficient byte buffer management
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
│   └── keyword_shred/test/        # Hierarchical keyword extraction tests
├── Cargo.toml
├── COPYRIGHT.txt
└── README.md
```

---

## Limitations

Current implementation has the following constraints (appropriate for POC phase):

- **Case-sensitive**: "Email" and "email" are treated as different keywords
- **Exact token match**: No wildcard support; searches match tokens as split during indexing
- **Memory bound during indexing**: Entire index must fit in memory during construction (~15 million keywords in 4GB RAM for high cardinality data). No disk spooling during index build.
- **Memory bound during searching**: Entire index must fit in memory
- **Single-threaded indexing**: Indexing is not parallelized (but I/O is optimized)
- **No incremental updates**: Index must be rebuilt if Parquet file changes
- **String columns only**: Currently focuses on string data types

---

## Index Size Management

### Challenge

High-cardinality columns with many rows per keyword can create excessively large indexes that provide minimal performance benefit over direct Parquet filtering. When a keyword appears in many rows, storing detailed row-level information for each occurrence may cause the index to grow disproportionately to the benefit provided.

### Current Behavior

The index currently stores complete row-level detail for every keyword occurrence, including:
- Row numbers with run-length encoding
- Row group mappings
- Parent keyword relationships for phrase search
- Split-level metadata

For keywords appearing in a small to moderate number of rows, this provides excellent search performance with minimal overhead. However, for very common keywords appearing across many rows, the index size can grow substantially while providing diminishing returns.

### Planned Solution

A threshold-based approach that gracefully degrades behavior for high-frequency keywords:

**1. Row Information Threshold** (configurable parameter requiring profiling)
- Keywords below threshold: Store full row details (current behavior)
- Keywords above threshold: Switch to summary-only storage

The optimal threshold value depends on the specific dataset characteristics and would need to be determined through profiling and measurement.

**2. Degraded Storage Mode** (for keywords exceeding threshold)
Instead of storing every row occurrence, store only:
- Keyword existence in column (bloom filter - already present)
- Approximate occurrence count
- Row group statistics (which row groups contain the keyword, not individual rows)
- Indication that this keyword is in "high-frequency mode"

**3. Search Behavior Adaptation**
- **Selective keywords** (below threshold): Use indexed row details for instant results
- **Common keywords** (above threshold): Return indication that Parquet-level filtering is needed
- **Clear result indication**: `SearchResult` distinguishes between verified matches (from index) and high-frequency keywords requiring verification

**4. Trade-off Analysis**

This approach provides several benefits:

| Aspect | Benefit |
|--------|---------|
| **Index Size** | Prevents index from growing larger than the Parquet file itself |
| **Memory** | Keeps memory requirements predictable and bounded |
| **Search Speed** | Maintains fast searches for selective queries while being honest about limitations for common terms |
| **Practicality** | Very common keywords typically indicate the term isn't selective enough for index optimization anyway |

The system would provide clear user feedback when searches encounter high-frequency keywords, suggesting either Parquet-level filtering or more specific search terms.

### Implementation Status

**Current State**: All keywords store full row detail regardless of frequency. The system works well for datasets with moderate keyword frequencies.

**Threshold Approach**: Designed but not yet implemented. The architecture already supports different storage strategies (bloom filter vs HashSet based on size), making this enhancement a natural extension.

**When to Consider Implementing**:
- If monitoring shows indexes growing disproportionately large relative to Parquet files
- If common queries frequently return very large result sets
- If memory constraints during indexing become problematic for production workloads

For the current POC phase, the full-detail approach provides valuable insights into real-world cardinality distributions and helps validate whether the threshold-based approach would provide meaningful benefits for production use cases.

**With Additional Development Time**: Profiling on representative production datasets would inform specific threshold values and validate the memory/performance trade-offs. Additionally, benchmark testing across different data distributions (uniform vs. power-law keyword frequencies) would help quantify when the threshold approach provides meaningful benefits versus the current full-detail implementation.

---

## Advanced Features

### Parent Keyword Tracking

Keywords maintain references to their parent keywords in the split hierarchy. This enables efficient phrase search without reading the Parquet file:

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

    build_and_save_index("data.parquet", exclude_columns, error_rate, None).await?;

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

    build_and_save_index("data.parquet", Some(exclude), None, None).await?;

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

While the current implementation is single-threaded, the architecture is designed to support parallelization.
The strategy includes:

### 1. Column Parallelization
- For each column in each row groups concurrently build the index (up to a thread limit)
- Use `Arc<DashMap>` for thread-safe keyword insertion
- Requires changing `Rc<str>` to `Arc<str>`

### 2. Memory Management
- Monitor memory usage across threads
- Implement backpressure mechanisms

### 3. Expected Performance
- Estimated 3-4x improvement on 4-core systems
- Sub-linear scaling due to merge overhead
- Memory bandwidth may become limiting factor, however S3 download rate will also be a limiting factor

### 4. Why Not Implemented
For the POC phase, single-threaded was chosen to:
- Reduce complexity for first Rust project
- Establish baseline performance
- Keep codebase accessible
- Prove concept before optimization

---

## Roadmap / Future Improvements

**Near-term Enhancements:**
- Case-insensitive search option (store keywords normalized)
- Wildcard support (trailing wildcards initially)
- Metadata caching within index (eliminate additional GET requests)
- Block compression in data.bin
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
- Test on large public datasets (e.g., NYC Taxi Trip Data)
- Comprehensive benchmarking suite
- Memory profiling and optimization
- Cost analysis (storage vs compute trade-offs)

---

## FAQ

**Q: Can I search for partial matches or wildcards?**  
A: Currently, the system only supports exact token matches as they were split during indexing. Wildcard support is planned for future versions.

**Q: How much memory does searching use?**  
A: The entire index is loaded into memory. The actual Parquet data is not loaded unless verification is needed.

**Q: Can I update an existing index?**  
A: Not currently. You must rebuild the entire index if the Parquet file changes. Incremental updates are planned for future versions.

**Q: Does it work with nested/complex Parquet schemas?**  
A: Currently focuses on string columns. Complex nested structures may need flattening.

**Q: Can I use this with other cloud providers (Azure, GCP)?**  
A: The code uses the `object_store` crate which supports multiple backends. AWS S3 is configured by default, but Azure and GCP can be configured with minimal changes.

**Q: Is the search thread-safe?**  
A: `KeywordSearcher` is not `Sync` due to internal buffer management. Create one instance per thread or use appropriate synchronization primitives.

**Q: What about transient network failures when accessing S3?**  
A: The `object_store` library includes built-in retry logic with exponential backoff for handling transient S3 failures automatically.

**Q: How do I validate my index is up-to-date?**  
A: Use the `validate_index()` function which checks file size, ETag, and last modified time:

---
