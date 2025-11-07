use super::*;

#[test]
fn test_row_add_same_row_same_parent() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 1);
    let kw = keyword_map.get("Hello").unwrap();
    assert_eq!(kw.column_references.len(), 2);
    assert!(kw.column_references.len() >= 2);
    assert_eq!(kw.row_groups.len(), 2);
    assert_eq!(kw.row_groups[0].len(), 1);
    assert_eq!(kw.row_groups[0].as_slice()[0], 0);
    assert_eq!(kw.row_group_to_rows.len(), 2);
    assert_eq!(kw.row_group_to_rows[0].len(), 1);
    assert_eq!(kw.row_group_to_rows[0][0].len(), 1);
    assert_eq!(kw.row_group_to_rows[0][0][0], Row {
        row: 0,
        additional_rows: 0,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_parent_keyword_tracking() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("parent/child", col_ref, 0, 0, &mut keyword_map);

    let child = keyword_map.get("child").unwrap();

    println!("Child parent_keyword: {:?}", child.row_group_to_rows[0][0][0].parent_keyword);

    assert!(
        child.row_group_to_rows[0][0][0].parent_keyword.is_some(),
        "Child should have 1 parent keyword"
    );

    assert_eq!(
        child.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
        "parent/child",
        "Parent should be 'parent/child'"
    );
}

#[test]
fn test_same_row_different_parents() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Create two different parent strings that contain "test"
    perform_split("parent1/test", col_ref, 0, 10, &mut keyword_map);
    perform_split("parent2/test", col_ref, 0, 10, &mut keyword_map);  // Same row!

    let kw = keyword_map.get("test").unwrap();

    println!("\nRows for 'test' keyword:");
    for (i, row) in kw.row_group_to_rows[0][0].iter().enumerate() {
        println!("  Row {}: row={}, parent={:?}", i, row.row, row.parent_keyword);
    }

    // Should have 2 separate Row entries because parents differ
    assert_eq!(kw.row_group_to_rows[0][0].len(), 2, "Should have 2 Row entries for different parents");
    assert_eq!(kw.row_group_to_rows[0][0][0].row, 10);
    assert_eq!(kw.row_group_to_rows[0][0][1].row, 10);

    // Verify different parents
    assert_eq!(kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(), "parent1/test");
    assert_eq!(kw.row_group_to_rows[0][0][1].parent_keyword.as_ref().unwrap().as_ref(), "parent2/test");

    println!("Different parents test passed!");
}

#[test]
fn test_parent_keyword_hierarchy_all_levels() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Complex string that demonstrates hierarchical splitting through all 4 levels:
    // Need a string that doesn't split at Level 1, but does at later levels
    // Level 0: spaces, tabs, etc - use something without these
    // Level 1: /, @, =, :, \, ?, & - use something without these
    // Level 2: ., $, #, `, ~, ^, + - split here
    // Level 3: -, _ - split here
    //
    // Example: "file.txt$data#tag-item_name"
    // Level 0: no splits → continues
    // Level 1: no splits → continues
    // Level 2: splits by ., $, # → ["file", "txt", "data", "tag-item_name"]
    //   Before splitting, "file.txt$data#tag-item_name" is ADDED with no parent
    // Each part continues to Level 3:
    //   "file" → no splits → ADDED with parent "file.txt$data#tag-item_name"
    //   "txt" → no splits → ADDED with parent "file.txt$data#tag-item_name"
    //   "data" → no splits → ADDED with parent "file.txt$data#tag-item_name"
    //   "tag-item_name" → splits by -, _ → ["tag", "item", "name"]
    //     Before splitting, "tag-item_name" is ADDED with parent "file.txt$data#tag-item_name"
    //     "tag", "item", "name" are ADDED with parent "tag-item_name"

    let complex_string = "file.txt$data#tag-item_name";

    perform_split(complex_string, col_ref, 0, 0, &mut keyword_map);

    println!("\n=== Parent Keyword Hierarchy Test ===");
    println!("Original string: {}", complex_string);
    println!("\nAll keywords found:");
    for (keyword, _) in keyword_map.iter() {
        println!("  '{}'", keyword);
    }

    // Root keyword should have no parents
    let root_kw = keyword_map.get(complex_string).unwrap();
    assert!(
        root_kw.row_group_to_rows[0][0][0].parent_keyword.is_none(),
        "Root keyword should have no parents"
    );

    // Level 2 split parts should have root as parent
    if let Some(kw) = keyword_map.get("file") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "file should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            complex_string,
            "file's parent should be the root string"
        );
    }

    if let Some(kw) = keyword_map.get("txt") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "txt should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            complex_string
        );
    }

    if let Some(kw) = keyword_map.get("data") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "data should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            complex_string
        );
    }

    // "tag-item_name" should have root as parent
    if let Some(kw) = keyword_map.get("tag-item_name") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "tag-item_name should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            complex_string,
            "tag-item_name's parent should be the root string"
        );
    }

    // Level 3 splits should have "tag-item_name" as parent
    if let Some(kw) = keyword_map.get("tag") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "tag should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            "tag-item_name",
            "tag's parent should be tag-item_name"
        );
    }

    if let Some(kw) = keyword_map.get("item") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "item should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            "tag-item_name",
            "item's parent should be tag-item_name"
        );
    }

    if let Some(kw) = keyword_map.get("name") {
        assert!(
            kw.row_group_to_rows[0][0][0].parent_keyword.is_some(),
            "name should have 1 parent"
        );
        assert_eq!(
            kw.row_group_to_rows[0][0][0].parent_keyword.as_ref().unwrap().as_ref(),
            "tag-item_name",
            "name's parent should be tag-item_name"
        );
    }

    println!("\n=== Parent hierarchy test passed! ===");
}

#[test]
fn test_parent_keywords_empty_for_unsplit_root() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Simple keyword with no delimiters - should have no parent
    perform_split("simple", col_ref, 0, 0, &mut keyword_map);

    let kw = keyword_map.get("simple").unwrap();
    assert!(
        kw.row_group_to_rows[0][0][0].parent_keyword.is_none(),
        "Root keyword without splits should have empty parent_keywords"
    );
}

#[test]
fn test_parent_keywords_multiple_occurrences() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Create scenarios where "test" appears with different parents
    perform_split("parent1/test", col_ref, 0, 0, &mut keyword_map);
    perform_split("parent2/test", col_ref, 0, 1, &mut keyword_map);
    perform_split("parent3-test", col_ref, 0, 2, &mut keyword_map);

    let kw = keyword_map.get("test").unwrap();

    println!("\n=== Multiple parent occurrences ===");
    for (i, row) in kw.row_group_to_rows[0][0].iter().enumerate() {
        println!("  Row {}: row={}, parent={:?}", i, row.row, row.parent_keyword);
    }

    // Should have 3 separate Row entries with different parents
    assert!(
        kw.row_group_to_rows[0][0].len() >= 3,
        "Should have at least 3 Row entries"
    );

    // Verify we have all three different parent strings
    let parents_found: Vec<String> = kw.row_group_to_rows[0][0]
        .iter()
        .filter_map(|row| {
            row.parent_keyword.as_ref().map(|p| p.to_string())
        })
        .collect();

    assert!(
        parents_found.contains(&"parent1/test".to_string()),
        "Should have parent1/test"
    );
    assert!(
        parents_found.contains(&"parent2/test".to_string()),
        "Should have parent2/test"
    );
    assert!(
        parents_found.contains(&"parent3-test".to_string()),
        "Should have parent3-test"
    );

    println!("Multiple parent occurrences test passed!");
}