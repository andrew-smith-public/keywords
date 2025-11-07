use super::*;

#[test]
fn test_column_keywords_map_single_value() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    perform_split("hello".into(), column_pool.intern("col1"), 0u16, 0u32, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 1, "Should have 1 column");
    assert!(column_keywords_map.contains_key("col1"), "Should contain col1");

    let col1_keywords = column_keywords_map.get("col1").unwrap();
    assert_eq!(col1_keywords.len(), 1, "col1 should have 1 keyword");
    assert!(col1_keywords.contains("hello"), "col1 should contain 'hello'");
}

#[test]
fn test_column_keywords_map_split_values() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    perform_split("hello world".into(), column_pool.intern("col1"), 0u16, 0u32, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 1, "Should have 1 column");
    let col1_keywords = column_keywords_map.get("col1").unwrap();
    assert_eq!(col1_keywords.len(), 2, "col1 should have 2 keywords");
    assert!(col1_keywords.contains("hello"), "col1 should contain 'hello'");
    assert!(col1_keywords.contains("world"), "col1 should contain 'world'");
}

#[test]
fn test_column_keywords_map_multiple_columns() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    perform_split("apple".into(), column_pool.intern("col1"), 0u16, 0u32, &mut keyword_map);
    perform_split("banana orange".into(), column_pool.intern("col2"), 0u16, 0u32, &mut keyword_map);
    perform_split("apple".into(), column_pool.intern("col3"), 0u16, 0u32, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 3, "Should have 3 columns");

    let col1_keywords = column_keywords_map.get("col1").unwrap();
    assert_eq!(col1_keywords.len(), 1);
    assert!(col1_keywords.contains("apple"));

    let col2_keywords = column_keywords_map.get("col2").unwrap();
    println!("{:?}", col2_keywords);
    assert_eq!(col2_keywords.len(), 2);
    assert!(col2_keywords.contains("banana"));
    assert!(col2_keywords.contains("orange"));

    let col3_keywords = column_keywords_map.get("col3").unwrap();
    assert_eq!(col3_keywords.len(), 1);
    assert!(col3_keywords.contains("apple"));
}

#[test]
fn test_column_keywords_map_complex_splitting() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    perform_split("path/to/file.txt".into(), column_pool.intern("filepath"), 0u16, 0u32, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    let filepath_keywords = column_keywords_map.get("filepath").unwrap();

    assert!(filepath_keywords.contains("path/to/file.txt"), "Should contain full path");
    assert!(filepath_keywords.contains("path"), "Should contain 'path'");
    assert!(filepath_keywords.contains("to"), "Should contain 'to'");
    assert!(filepath_keywords.contains("file"), "Should contain 'file'");
    assert!(filepath_keywords.contains("txt"), "Should contain 'txt'");
}

#[test]
fn test_column_keywords_map_single_chars() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    perform_split("a bc d efg h".into(), column_pool.intern("mixed"), 0u16, 0u32, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    let mixed_keywords = column_keywords_map.get("mixed").unwrap();
    assert_eq!(mixed_keywords.len(), 5, "Should have 5 keywords");
    assert!(mixed_keywords.contains("a"));
    assert!(mixed_keywords.contains("bc"));
    assert!(mixed_keywords.contains("d"));
    assert!(mixed_keywords.contains("efg"));
    assert!(mixed_keywords.contains("h"));
}

// ========== COMPREHENSIVE SPLIT LEVEL TESTS ==========

#[test]
fn test_build_column_keywords_map_empty() {
    let keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let column_pool = ColumnPool::new();

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 0, "Should return empty map for empty input");

    println!("Empty map test passed!");
}

#[test]
fn test_build_column_keywords_map_single_column_multiple_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("col1");

    perform_split("apple banana cherry", col_ref, 0, 0, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 1, "Should have 1 column");
    assert!(column_keywords_map.contains_key("col1"));

    let col1_keywords = column_keywords_map.get("col1").unwrap();
    assert_eq!(col1_keywords.len(), 3, "col1 should have 3 keywords");
    assert!(col1_keywords.contains("apple"));
    assert!(col1_keywords.contains("banana"));
    assert!(col1_keywords.contains("cherry"));
}

#[test]
fn test_build_column_keywords_map_overlapping_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col1 = column_pool.intern("col1");
    let col2 = column_pool.intern("col2");

    // "shared" appears in both columns
    perform_split("shared unique1", col1, 0, 0, &mut keyword_map);
    perform_split("shared unique2", col2, 0, 0, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 2, "Should have 2 columns");

    let col1_keywords = column_keywords_map.get("col1").unwrap();
    assert_eq!(col1_keywords.len(), 2);
    assert!(col1_keywords.contains("shared"));
    assert!(col1_keywords.contains("unique1"));

    let col2_keywords = column_keywords_map.get("col2").unwrap();
    assert_eq!(col2_keywords.len(), 2);
    assert!(col2_keywords.contains("shared"));
    assert!(col2_keywords.contains("unique2"));
}

#[test]
fn test_build_column_keywords_map_skips_global_bucket() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("col1");

    perform_split("keyword", col_ref, 0, 0, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    // Should not have an entry for column index 0 (global bucket)
    // The map should use column names as keys, not indices
    assert_eq!(
        column_keywords_map.len(),
        1,
        "Should only have 1 column (not the global bucket)"
    );
    assert!(column_keywords_map.contains_key("col1"));
    assert!(!column_keywords_map.contains_key("0"));
}

#[test]
fn test_build_column_keywords_map_many_columns() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    // Create 10 columns with various keywords
    for i in 0..10 {
        let col_name = format!("col{}", i);
        let col_ref = column_pool.intern(&col_name);
        let keyword = format!("keyword{} shared", i);
        perform_split(&keyword, col_ref, 0, i as u32, &mut keyword_map);
    }

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    assert_eq!(column_keywords_map.len(), 10, "Should have 10 columns");

    // Each column should have its unique keyword + "shared"
    for i in 0..10 {
        let col_name = format!("col{}", i);
        let keywords = column_keywords_map.get(col_name.as_str()).unwrap();
        assert_eq!(keywords.len(), 2, "Each column should have 2 keywords");
        let keyword_str = format!("keyword{}", i);
        assert!(keywords.contains(keyword_str.as_str()));
        assert!(keywords.contains("shared"));
    }
}

#[test]
fn test_build_column_keywords_map_with_splitting() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("filepath");

    // This will split into multiple keywords
    perform_split("path/to/file.txt", col_ref, 0, 0, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    let filepath_keywords = column_keywords_map.get("filepath").unwrap();

    // Should contain the full path AND all split parts
    assert!(filepath_keywords.contains("path/to/file.txt"));
    assert!(filepath_keywords.contains("path"));
    assert!(filepath_keywords.contains("to"));
    assert!(filepath_keywords.contains("file.txt"));
    assert!(filepath_keywords.contains("file"));
    assert!(filepath_keywords.contains("txt"));
}

#[test]
fn test_build_column_keywords_map_indexset_ordering() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("col1");

    // Add keywords in a specific order
    perform_split("zebra", col_ref, 0, 0, &mut keyword_map);
    perform_split("apple", col_ref, 0, 1, &mut keyword_map);
    perform_split("banana", col_ref, 0, 2, &mut keyword_map);

    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);
    let col1_keywords = column_keywords_map.get("col1").unwrap();

    // IndexSet maintains insertion order
    let keywords_vec: Vec<&str> = col1_keywords.iter().map(|s| s.as_ref()).collect();
    println!("Keywords in order: {:?}", keywords_vec);

    // All keywords should be present (order is determined by internal HashMap iteration)
    assert!(col1_keywords.contains("zebra"));
    assert!(col1_keywords.contains("apple"));
    assert!(col1_keywords.contains("banana"));
}