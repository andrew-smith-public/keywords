use hashbrown::HashMap;
use hashbrown::hash_map::RawEntryMut;
use indexmap::IndexSet;
use smallvec::{SmallVec, smallvec};
use std::rc::Rc;
use crate::utils::column_pool::{ColumnPool};

// Original constants
pub const SPLIT_CHARS_INCLUSIVE: &[&[char]] = &[
    &['\r', '\n', '\t', '\'', '"', '<', '>', '(', ')', '|', ',', '!', ';', '{', '}', '*', ' '],
    &['/', '@', '=', ':', '\\', '?', '&'],
    &['.', '$', '#', '`', '~', '^', '+'],
    &['-', '_'],
];
pub const SPLIT_CHARS_COUNT: usize = SPLIT_CHARS_INCLUSIVE.len();

pub const ADDITIONAL_ROWS_CAP: u16 = u16::MAX - 1;

// Build lookup table once as a static
static SPLIT_LOOKUP: [[bool; 128]; 4] = {
    let mut lookup = [[false; 128]; 4];

    let mut level = 0;
    while level < 4 {
        let mut i = 0;
        while i < SPLIT_CHARS_INCLUSIVE[level].len() {
            let ch = SPLIT_CHARS_INCLUSIVE[level][i] as u32;
            if ch < 128 {
                lookup[level][ch as usize] = true;
            }
            i += 1;
        }
        level += 1;
    }
    lookup
};

/// Checks if a character is a delimiter at the specified split-level.
///
/// This function uses a pre-computed lookup table for fast delimiter detection.
/// It only works for ASCII characters; non-ASCII characters always return false.
///
/// # Arguments
///
/// * `c` - The character to check
/// * `level` - The split-level (0-3) corresponding to different delimiter sets
///
/// # Returns
///
/// Returns `true` if the character is an ASCII delimiter at the given level, `false` otherwise.
///
/// # Safety
///
/// Uses `get_unchecked` for performance after checking that the character is ASCII.
/// The ASCII check ensures the character value is always < 128, making the unchecked
/// access safe.
#[inline(always)]
pub fn is_delimiter(c: char, level: usize) -> bool {
    c.is_ascii() && unsafe { *SPLIT_LOOKUP[level].get_unchecked(c as usize) }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Row {
    pub(crate) row: u32,
    pub(crate) additional_rows: u16,
    pub(crate) splits_matched: u16,

    // Parent keyword tracking for phrase search optimization
    // Stores immediate parent (one level up in split hierarchy)
    // Uses Rc<str> for cheap cloning - only increments reference count
    // None indicates this is a root token from the original parquet string
    //
    // Considered but deferred optimizations (only consider once there is more control over index size):
    //
    // 1. token_position (u16/u32): Would enable early pruning of non-adjacent tokens during
    //    phrase search at cost of 2-4 bytes per row. Current approach favors memory efficiency.
    //    Position tracking would be most beneficial for very long phrases (5+ tokens), which
    //    are rare in typical keyword search workloads.
    //
    // 2. top_ancestor: Storing root keyword in addition to immediate parent would eliminate
    //    recursive parent lookups during phrase verification, trading storage (~8 bytes per row)
    //    for faster phrase matching. Current single-parent approach already handles most queries
    //    efficiently. Consider if phrase search profiling shows parent chain traversal is a bottleneck.
    pub(crate) parent_keyword: Option<Rc<str>>,
}

impl std::fmt::Display for Row {
    /// Formats the Row for display, showing its row number, additional rows count,
    /// splits matched as a binary representation, and parent keyword.
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    ///
    /// # Returns
    ///
    /// Returns `std::fmt::Result` indicating success or failure of the formatting operation.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Row {{ row: {}, additional_rows: {}, splits_matched: {:05b}, parent_keyword: ",
            self.row,
            self.additional_rows,
            self.splits_matched
        )?;

        match &self.parent_keyword {
            Some(parent) => write!(f, "Some(\"{}\")", parent)?,
            None => write!(f, "None")?,
        }

        write!(f, " }}")
    }
}

impl Row {
    /// Adds a new row occurrence or merges with the existing row if conditions allow.
    ///
    /// This method implements run-length encoding for consecutive rows with the same
    /// split pattern and parent keyword. It will merge rows if:
    /// - The row number is the same as the current row and parent matches (OR the splits)
    /// - The row number is consecutive, split patterns match, and we haven't hit the cap
    ///
    /// # Arguments
    ///
    /// * `row_number` - The row number to add
    /// * `split_match_bit` - Bitmask indicating which split levels matched
    /// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
    ///
    /// # Returns
    ///
    /// Returns `Some(Row)` if a new row needs to be created (couldn't be merged),
    /// or `None` if the row was successfully merged into the existing row.
    pub(crate) fn add(
        &mut self,
        row_number: u32,
        split_match_bit: u16,
        parent_keyword: &Option<Rc<str>>
    ) -> Option<Row> {
        let previous_row_seen_number = self.row + self.additional_rows as u32;
        if previous_row_seen_number == row_number && self.parent_keyword == *parent_keyword {
            // Same row, same parent - just merge splits_matched
            self.splits_matched |= split_match_bit;
            None
        } else if (previous_row_seen_number + 1 == row_number)
            && (self.splits_matched == split_match_bit)
            && self.additional_rows < ADDITIONAL_ROWS_CAP
            && self.parent_keyword == *parent_keyword {
            self.additional_rows += 1;
            None
        } else {
            Some(Row {
                row: row_number,
                additional_rows: 0,
                splits_matched: split_match_bit,
                parent_keyword: parent_keyword.clone(),
            })
        }
    }
}

#[derive(Debug)]
pub struct KeywordOneFile {
    pub(crate) splits_matched: u16,
    pub(crate) column_references: SmallVec<[u32; 2]>,  // Stack-allocated for ≤2 columns
    pub(crate) row_groups: Vec<SmallVec<[u16; 4]>>,   // Adaptive storage based on file's row group count
    pub(crate) row_group_to_rows: Vec<Vec<Vec<Row>>>
}

impl std::fmt::Display for KeywordOneFile {
    /// Formats the KeywordOneFile for display, showing splits matched, column count,
    /// and total row group count.
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    ///
    /// # Returns
    ///
    /// Returns `std::fmt::Result` indicating success or failure of the formatting operation.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeywordOneFile {{ splits_matched: {}, columns: {}, row_groups: {} }}",
            self.splits_matched,
            self.column_references.len(),
            self.row_groups.iter().map(|rg| rg.len()).sum::<usize>()
        )
    }
}

impl KeywordOneFile {

    /// Adds a row to a specific column and row group index.
    ///
    /// This method attempts to merge the row with the last row in the vector if possible,
    /// or creates a new row entry if merging isn't possible.
    ///
    /// # Arguments
    ///
    /// * `column_idx` - Index of the column in the internal structure
    /// * `row_idx` - Index of the row group in the internal structure
    /// * `row_number` - The actual row number in the file
    /// * `split_match_bit` - Bitmask indicating which split levels matched
    /// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
    fn add_row(
        &mut self,
        column_idx: usize,
        row_idx: usize,
        row_number: u32,
        split_match_bit: u16,
        parent_keyword: &Option<Rc<str>>,
    ) {
        let rows_len = self.row_group_to_rows[column_idx][row_idx].len();
        if rows_len > 0 {
            let previous_row: &mut Row = &mut self.row_group_to_rows[column_idx][row_idx][rows_len - 1];
            let new_row: Option<Row> = previous_row.add(row_number, split_match_bit, parent_keyword);
            if let Some(new_row_value) = new_row {
                self.row_group_to_rows[column_idx][row_idx].push(new_row_value);
            }
        } else {
            self.row_group_to_rows[column_idx][row_idx].push(Row {
                row: row_number,
                additional_rows: 0,
                splits_matched: split_match_bit,
                parent_keyword: parent_keyword.clone(),
            });
        }
    }

    /// Adds a row group to the keyword structure for a specific column.
    ///
    /// This method ensures the row group exists for both the global bucket (index 0)
    /// and the specific column. If the row group doesn't exist, it creates the necessary
    /// structures before adding the row.
    ///
    /// # Arguments
    ///
    /// * `column_idx` - Index of the column in the internal structure
    /// * `row_group` - The row group number to add
    /// * `row_number` - The actual row number in the file
    /// * `split_match_bit` - Bitmask indicating which split levels matched
    /// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
    fn add_group(
        &mut self,
        column_idx: usize,
        row_group: u16,
        row_number: u32,
        split_match_bit: u16,
        parent_keyword: &Option<Rc<str>>,
    ) {
        for column_idx_u in [0, column_idx] {
            match self.row_groups[column_idx_u].iter().position(|&c| c == row_group) {
                Some(idx) => {
                    self.add_row(column_idx_u, idx, row_number, split_match_bit, parent_keyword);
                }
                None => {
                    {
                        self.row_groups[column_idx_u].push(row_group);
                        self.row_group_to_rows[column_idx_u].push(Vec::new());
                        let new_idx = self.row_groups[column_idx_u].len() - 1;
                        self.add_row(column_idx_u, new_idx, row_number, split_match_bit, parent_keyword);
                    }
                }
            }
        }
    }

    /// Adds keyword details to the structure, creating the column reference if needed.
    ///
    /// This method updates the splits_matched bitmask and ensures the column reference
    /// exists in the structure before delegating to add_group.
    ///
    /// # Arguments
    ///
    /// * `column_reference` - The column identifier from the column pool
    /// * `row_group` - The row group number
    /// * `row_number` - The actual row number in the file
    /// * `split_match_bit` - Bitmask indicating which split levels matched
    /// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
    fn add_keyword_details(
        &mut self,
        column_reference: u32,
        row_group: u16,
        row_number: u32,
        split_match_bit: u16,
        parent_keyword: &Option<Rc<str>>,
    ) {
        self.splits_matched |= split_match_bit;
        match self.column_references.iter().position(|&c| c == column_reference) {
            Some(idx) => {
                self.add_group(idx, row_group, row_number, split_match_bit, parent_keyword);
            }
            None => {
                self.column_references.push(column_reference);
                self.row_groups.push(SmallVec::new());
                self.row_group_to_rows.push(Vec::new());
                let new_idx = self.column_references.len() - 1;
                self.add_group(new_idx, row_group, row_number, split_match_bit, parent_keyword);
            }
        }
    }
}

/// Creates a new KeywordOneFile instance with initial data.
///
/// This function initializes a new keyword structure with both a global bucket (index 0)
/// and a specific column reference, each containing the same initial row data.
///
/// # Arguments
///
/// * `column_reference` - The column identifier from the column pool
/// * `row_group` - The initial row group number
/// * `row_number` - The initial row number
/// * `split_match_bit` - Bitmask indicating which split levels matched
/// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
///
/// # Returns
///
/// Returns a newly created `KeywordOneFile` instance with the provided initial data
/// in both the global bucket and the specific column.
#[inline(always)]
fn create_new_keyword_one_file(
    column_reference: u32,
    row_group: u16,
    row_number: u32,
    split_match_bit: u16,
    parent_keyword: &Option<Rc<str>>
) -> KeywordOneFile {
    KeywordOneFile {
        splits_matched: split_match_bit,
        column_references: smallvec![0, column_reference],  // Global bucket + this column
        row_groups: vec![
            smallvec![row_group],
            smallvec![row_group]
        ],
        row_group_to_rows: vec![
            vec![vec![Row {
                row: row_number,
                additional_rows: 0,
                splits_matched: split_match_bit,
                parent_keyword: parent_keyword.clone(),
            }]],
            vec![vec![Row {
                row: row_number,
                additional_rows: 0,
                splits_matched: split_match_bit,
                parent_keyword: parent_keyword.clone(),
            }]]
        ]
    }
}

// Inspection incorrectly marks an as incorrect, even thought it is correct as the rule for acronyms
// or similar is based on the sound made, not the actual letter
//noinspection GrazieInspection
/// Merges keyword data into an existing entry or creates a new one, returning an Rc to the key.
///
/// This function uses raw entry API to avoid double hashing and cloning. If the keyword
/// already exists in the map, it updates the existing entry. Otherwise, it creates a new
/// entry. The function returns an Rc<str> reference to the keyword, which is useful for
/// establishing parent-child relationships in the keyword hierarchy.
///
/// # Arguments
///
/// * `keyword_string` - The keyword string to merge or add
/// * `column_reference` - The column identifier from the column pool
/// * `row_group` - The row group number
/// * `row_number` - The row number
/// * `split_match_bit` - Bitmask indicating which split levels matched
/// * `keyword_map` - Mutable reference to the keyword map
/// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
///
/// # Returns
///
/// Returns an `Rc<str>` reference to the keyword, whether it was newly created or already existed.
fn merge_or_add_keyword_return_rc(
    keyword_string: &str,
    column_reference: u32,
    row_group: u16,
    row_number: u32,
    split_match_bit: u16,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>,
    parent_keyword: &Option<Rc<str>>
) -> Rc<str> {

    match keyword_map.raw_entry_mut().from_key(keyword_string) {
        RawEntryMut::Occupied(mut keyword_one_file_entry) => {
            // Update existing entry
            keyword_one_file_entry.get_mut().add_keyword_details(
                column_reference,
                row_group,
                row_number,
                split_match_bit,
                parent_keyword
            );
            keyword_one_file_entry.key().clone()
        }
        RawEntryMut::Vacant(keyword_one_file_entry) => {
            let key_rc: Rc<str> = Rc::from(keyword_string);
            keyword_one_file_entry.insert(
                key_rc.clone(),
                create_new_keyword_one_file(
                    column_reference,
                    row_group,
                    row_number,
                    split_match_bit,
                    parent_keyword,
                )
            );
            key_rc
        }
    }
}

/// Merges keyword data into an existing entry or creates a new one without returning a reference.
///
/// This function is similar to `merge_or_add_keyword_return_rc` but doesn't return the Rc<str>
/// reference. This is more efficient when the caller doesn't need to use the keyword reference
/// for parent-child relationships. Uses raw entry API to avoid double hashing.
///
/// # Arguments
///
/// * `keyword_string` - The keyword string to merge or add
/// * `column_reference` - The column identifier from the column pool
/// * `row_group` - The row group number
/// * `row_number` - The row number
/// * `split_match_bit` - Bitmask indicating which split levels matched
/// * `keyword_map` - Mutable reference to the keyword map
/// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
fn merge_or_add_keyword_no_return(
    keyword_string: &str,
    column_reference: u32,
    row_group: u16,
    row_number: u32,
    split_match_bit: u16,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>,
    parent_keyword: &Option<Rc<str>>
) {

    match keyword_map.raw_entry_mut().from_key(keyword_string) {
        RawEntryMut::Occupied(mut keyword_one_file_entry) => {
            // Update existing entry
            keyword_one_file_entry.get_mut().add_keyword_details(
                column_reference,
                row_group,
                row_number,
                split_match_bit,
                parent_keyword
            );
        }
        RawEntryMut::Vacant(keyword_one_file_entry) => {
            keyword_one_file_entry.insert(
                Rc::from(keyword_string),
                create_new_keyword_one_file(
                    column_reference,
                    row_group,
                    row_number,
                    split_match_bit,
                    parent_keyword,
                )
            );
        }
    }
}


/// Internal recursive function that performs hierarchical keyword splitting.
///
/// This function implements a multi-level splitting strategy where keywords are split
/// using different delimiter sets at each level. It recursively processes splits and
/// maintains parent-child relationships in the keyword hierarchy. The function handles
/// three cases:
/// 1. No split occurred - recurse to next level with updated match bit
/// 2. Split occurred - add parent keyword and process each child with parent reference
/// 3. String ends with delimiters - ensure parent is still added to the map
///
/// # Arguments
///
/// * `keyword_string` - The string to split at this level
/// * `column_reference` - The column identifier from the column pool
/// * `row_group` - The row group number
/// * `row_number` - The row number
/// * `keyword_map` - Mutable reference to the keyword map
/// * `split_level` - Current split-level (0-3)
/// * `incomplete_split_match_bit_in` - Accumulated bitmask of splits matched so far
/// * `parent_keyword` - Reference to the parent keyword in the split hierarchy
// Optimized version: Use lookup table and optimized character iteration
#[inline]
fn perform_split_inner(
    keyword_string: &str,
    column_reference: u32,
    row_group: u16,
    row_number: u32,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>,
    split_level: usize,
    incomplete_split_match_bit_in: u16,
    parent_keyword: &Option<Rc<str>>
) {
    let mut new_parent_keyword: Option<Rc<str>> = None;
    let mut output_parent_decision_complete = false;
    let current_split_level = 1 << (split_level + 1);

    for split in keyword_string.split(|c| is_delimiter(c, split_level)).filter(|s| !s.is_empty()) {
        if split.len() == keyword_string.len() {
            let combined_match_bit: u16 = incomplete_split_match_bit_in | current_split_level;
            output_parent_decision_complete = true;
            if split_level + 1 == SPLIT_CHARS_COUNT {
                merge_or_add_keyword_no_return(
                    keyword_string,
                    column_reference,
                    row_group,
                    row_number,
                    combined_match_bit,
                    keyword_map,
                    parent_keyword
                );
            }
            else {
                perform_split_inner(
                    keyword_string,
                    column_reference,
                    row_group,
                    row_number,
                    keyword_map,
                    split_level + 1,
                    combined_match_bit,
                    parent_keyword
                );
            }
            break;
        }
        else {
            if !output_parent_decision_complete && (incomplete_split_match_bit_in != 1) {
                let keyword_rc_str = merge_or_add_keyword_return_rc(
                    keyword_string,
                    column_reference,
                    row_group,
                    row_number,
                    incomplete_split_match_bit_in,
                    keyword_map,
                    parent_keyword
                );
                new_parent_keyword = Some(keyword_rc_str);
            }
            output_parent_decision_complete = true;

            let parent_to_use = if new_parent_keyword.is_some() {
                &new_parent_keyword
            } else {
                parent_keyword
            };

            if split_level + 1 == SPLIT_CHARS_COUNT {
                merge_or_add_keyword_no_return(
                    split,
                    column_reference,
                    row_group,
                    row_number,
                    current_split_level,
                    keyword_map,
                    parent_to_use
                );
            } else {
                perform_split_inner(
                    split,
                    column_reference,
                    row_group,
                    row_number,
                    keyword_map,
                    split_level + 1,
                    current_split_level,
                    parent_to_use
                );
            }
        }
    }

    // If the final characters were only splitting characters the loop will hit nothing so we need
    // to just output here
    if !output_parent_decision_complete && incomplete_split_match_bit_in != 1 {
        merge_or_add_keyword_no_return(
            keyword_string,
            column_reference,
            row_group,
            row_number,
            incomplete_split_match_bit_in,
            keyword_map,
            parent_keyword
        );
    }
}

/// Performs hierarchical keyword splitting on a string from a Parquet file.
///
/// This is the entry point for the keyword splitting process. It initiates a recursive
/// multi-level splitting strategy that breaks down the input string using different
/// delimiter sets at each level. The function creates a hierarchy of keywords in the
/// keyword_map, with parent-child relationships tracked for phrase search optimization.
///
/// # Arguments
///
/// * `keyword_string` - The original string from the Parquet file to split
/// * `column_reference` - The column identifier from the column pool
/// * `row_group` - The row group number in the Parquet file
/// * `row_number` - The row number in the Parquet file
/// * `keyword_map` - Mutable reference to the keyword map where results are stored
///
/// # Examples
///
/// ```
/// # use keywords::keyword_shred::perform_split;
/// use hashbrown::HashMap;
/// let mut keyword_map = HashMap::new();
/// perform_split("hello-world", 1, 0, 42, &mut keyword_map);
/// // Creates entries for "hello-world", "hello", and "world" with parent-child relationships
/// ```
pub fn perform_split(
    keyword_string: &str,
    column_reference: u32,  // From a column pool
    row_group: u16,
    row_number: u32,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>
) {
    perform_split_inner(
        keyword_string,
        column_reference,
        row_group,
        row_number,
        keyword_map,
        0,
        1,
        &None,  // No parent - this is the root/original string from parquet
    );
}

/// Builds a mapping of column names to the set of keywords found in each column.
///
/// This function should be called once after all keyword splitting is complete. It
/// iterates through the keyword map and creates a reverse index that maps column names
/// to the set of keywords that appear in each column. The global bucket (index 0) is
/// skipped as it's not a real column.
///
/// # Arguments
///
/// * `keyword_map` - Reference to the complete keyword map after splitting
/// * `column_pool` - Reference to the column pool for resolving column names
///
/// # Returns
///
/// Returns a `HashMap` mapping column names (as `Rc<str>`) to `IndexSet` of keywords
/// that appear in each column. Uses `Rc<str>` for efficient memory usage through
/// reference counting.
///
/// # Examples
///
/// ```
/// use hashbrown::HashMap;
/// use indexmap::IndexSet;
/// use keywords::utils::column_pool::ColumnPool;
/// use keywords::keyword_shred::KeywordOneFile;
/// use std::rc::Rc;
/// # use keywords::keyword_shred::build_column_keywords_map;
/// # let keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
/// # let column_pool: ColumnPool = ColumnPool::new();
/// // let keyword_map = // ... populated keyword map
/// // let column_pool = // ... column pool
/// let column_keywords = build_column_keywords_map(&keyword_map, &column_pool);
/// // Access keywords for a specific column:
/// if let Some(keywords) = column_keywords.get("email") {
///     // Process keywords found in email column
/// }
/// ```
// NEW: Build column_keywords_map ONCE after all splitting is complete
pub fn build_column_keywords_map(
    keyword_map: &HashMap<Rc<str>, KeywordOneFile>,
    column_pool: &ColumnPool,
) -> HashMap<Rc<str>, IndexSet<Rc<str>>> {
    let mut column_keywords_map: HashMap<Rc<str>, IndexSet<Rc<str>>> = HashMap::new();

    for (keyword, keyword_file) in keyword_map {
        for &column_idx in keyword_file.column_references.iter() {
            if column_idx == 0 {
                continue; // Skip global bucket
            }

            if let Some(column_name) = column_pool.get(column_idx) {
                column_keywords_map
                    .entry(column_name.into())
                    .or_insert_with(IndexSet::new)
                    .insert(keyword.clone());  // Cheap Rc clone
            }
        }
    }

    column_keywords_map
}

// Link to test module (only compiled during tests)
#[cfg(test)]
#[path = "keyword_shred/tests/mod.rs"]
mod tests;