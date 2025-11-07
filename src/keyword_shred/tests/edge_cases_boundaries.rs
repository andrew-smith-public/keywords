use super::*;

#[test]
fn test_row_overflow_additional_rows() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("Hello", col_ref, 0, 0, &mut keyword_map);

    let near_max = ADDITIONAL_ROWS_CAP - 1;
    for i in 1..=near_max {
        perform_split("Hello", col_ref, 0, i as u32, &mut keyword_map);
    }

    {
        let kw = keyword_map.get("Hello").unwrap();
        assert_eq!(kw.row_group_to_rows[0][0].len(), 1, "Should still be 1 Row entry");
        assert_eq!(kw.row_group_to_rows[0][0][0].row, 0);
        assert_eq!(kw.row_group_to_rows[0][0][0].additional_rows, near_max);
    }

    perform_split("Hello", col_ref, 0, (near_max + 1) as u32, &mut keyword_map);
    {
        let kw = keyword_map.get("Hello").unwrap();
        assert_eq!(kw.row_group_to_rows[0][0].len(), 1);
        assert_eq!(kw.row_group_to_rows[0][0][0].additional_rows, ADDITIONAL_ROWS_CAP);
    }

    perform_split("Hello", col_ref, 0, (near_max + 2) as u32, &mut keyword_map);
    let kw = keyword_map.get("Hello").unwrap();
    assert_eq!(kw.row_group_to_rows[0][0].len(), 2, "Should create new Row entry at overflow");
    assert_eq!(kw.row_group_to_rows[0][0][1].row, (near_max + 2) as u32);
    assert_eq!(kw.row_group_to_rows[0][0][1].additional_rows, 0);
}

#[test]
fn test_very_long_string_many_splits() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Create a string with 100 tokens
    let mut tokens = Vec::new();
    for i in 0..100 {
        tokens.push(format!("token{}", i));
    }
    let long_string = tokens.join(" ");

    perform_split(&long_string, col_ref, 0, 0, &mut keyword_map);

    println!("\nVery long string produced {} keywords", keyword_map.len());

    // Should have extracted all 100 tokens
    assert_eq!(keyword_map.len(), 100, "Should extract 100 keywords");

    // Verify a few tokens
    assert!(keyword_map.contains_key("token0"));
    assert!(keyword_map.contains_key("token50"));
    assert!(keyword_map.contains_key("token99"));

    println!("Very long string test passed!");
}

#[test]
fn test_very_long_keyword() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Create a 10,000 character keyword
    let long_keyword = "a".repeat(10_000);
    perform_split(&long_keyword, col_ref, 0, 0, &mut keyword_map);

    assert_eq!(keyword_map.len(), 1, "Should handle very long keyword");
    assert!(keyword_map.contains_key(long_keyword.as_str()));

    let kw = keyword_map.get(long_keyword.as_str()).unwrap();
    assert_eq!(kw.splits_matched, 31); // All levels match (no splits)
}

#[test]
fn test_very_long_keyword_50k_chars() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Create a 50,000 character keyword
    let long_keyword = "x".repeat(50_000);
    perform_split(&long_keyword, col_ref, 0, 0, &mut keyword_map);

    assert_eq!(keyword_map.len(), 1, "Should handle 50k character keyword");
    assert!(keyword_map.contains_key(long_keyword.as_str()));
}

#[test]
fn test_max_row_number() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Test with u32::MAX
    perform_split("keyword", col_ref, 0, u32::MAX, &mut keyword_map);

    let kw = keyword_map.get("keyword").unwrap();
    assert_eq!(
        kw.row_group_to_rows[0][0][0].row,
        u32::MAX,
        "Should handle u32::MAX row number"
    );
}

#[test]
fn test_max_row_group() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Test with u16::MAX
    perform_split("keyword", col_ref, u16::MAX, 0, &mut keyword_map);

    let kw = keyword_map.get("keyword").unwrap();
    assert!(
        kw.row_groups[0].contains(&u16::MAX),
        "Should handle u16::MAX row group"
    );
}

#[test]
fn test_max_column_reference() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let _column_pool = ColumnPool::new();

    // Test with u32::MAX as column reference
    perform_split("keyword", u32::MAX, 0, 0, &mut keyword_map);

    let kw = keyword_map.get("keyword").unwrap();
    assert!(
        kw.column_references.contains(&u32::MAX),
        "Should handle u32::MAX column reference"
    );
}

#[test]
fn test_boundary_additional_rows_near_cap() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // This test is similar to the existing one but focuses on the boundary
    use crate::keyword_shred::ADDITIONAL_ROWS_CAP;

    perform_split("test", col_ref, 0, 0, &mut keyword_map);

    // Add rows up to exactly ADDITIONAL_ROWS_CAP
    for i in 1..=ADDITIONAL_ROWS_CAP {
        perform_split("test", col_ref, 0, i as u32, &mut keyword_map);
    }

    let kw = keyword_map.get("test").unwrap();
    assert_eq!(kw.row_group_to_rows[0][0].len(), 1);
    assert_eq!(kw.row_group_to_rows[0][0][0].additional_rows, ADDITIONAL_ROWS_CAP);

    // One more should create a new Row
    perform_split("test", col_ref, 0, (ADDITIONAL_ROWS_CAP + 1) as u32, &mut keyword_map);

    let kw = keyword_map.get("test").unwrap();
    assert_eq!(
        kw.row_group_to_rows[0][0].len(),
        2,
        "Should create new Row when exceeding cap"
    );
}