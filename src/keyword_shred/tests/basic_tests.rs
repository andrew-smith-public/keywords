use super::*;

#[test]
fn test_split_chars_count() {
    assert_eq!(SPLIT_CHARS_COUNT, 4);
    assert_eq!(SPLIT_CHARS_INCLUSIVE.len(), 4);
}

#[test]
fn test_row_creation() {
    let row = Row {
        row: 42,
        additional_rows: 2,
        splits_matched: 0b0001,
        parent_keyword: None,
    };
    assert_eq!(row.row, 42);
    assert_eq!(row.additional_rows, 2);
    assert_eq!(row.splits_matched, 1);
}

#[test]
fn test_row_equality() {
    let row1 = Row {
        row: 1,
        additional_rows: 0,
        splits_matched: 0b0001,
        parent_keyword: None,
    };
    let row2 = Row {
        row: 1,
        additional_rows: 0,
        splits_matched: 0b0001,
        parent_keyword: None,
    };
    assert_eq!(row1, row2);
}

#[test]
fn test_simple_keyword_no_splits() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("simple").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    assert_eq!(keyword_map.len(), 1);
    assert!(keyword_map.contains_key("simple"));
    let kw = keyword_map.get("simple").unwrap();
    assert_eq!(kw.splits_matched, 31);
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
fn test_keyword_with_space_split() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("hello world").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 2);
    assert!(keyword_map.contains_key("hello"));
    assert!(keyword_map.contains_key("world"));
    let kw = keyword_map.get("hello").unwrap();
    assert_eq!(kw.splits_matched, 30);
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
        splits_matched: 30,
        parent_keyword: None,
    });
    let kw2 = keyword_map.get("world").unwrap();
    assert_eq!(kw2.splits_matched, 30);
    assert_eq!(kw2.column_references.len(), 2);
    assert!(kw2.column_references.len() >= 2);
    assert_eq!(kw2.row_groups.len(), 2);
    assert_eq!(kw2.row_groups[0].len(), 1);
    assert_eq!(kw2.row_groups[0].as_slice()[0], 0);
    assert_eq!(kw2.row_group_to_rows.len(), 2);
    assert_eq!(kw2.row_group_to_rows[0].len(), 1);
    assert_eq!(kw2.row_group_to_rows[0][0].len(), 1);
    assert_eq!(kw2.row_group_to_rows[0][0][0], Row {
        row: 0,
        additional_rows: 0,
        splits_matched: 30,
        parent_keyword: None,
    });
}

#[test]
fn test_keyword_with_space_start() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from(" hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 1);
    assert!(keyword_map.contains_key("hello"));
    let kw = keyword_map.get("hello").unwrap();
    assert_eq!(kw.splits_matched, 30);
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
        splits_matched: 30,
        parent_keyword: None,
    });
}

#[test]
fn test_keyword_with_space_end() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("hello ").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 1);
    assert!(keyword_map.contains_key("hello"));
    let kw = keyword_map.get("hello").unwrap();
    assert_eq!(kw.splits_matched, 30);
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
        splits_matched: 30,
        parent_keyword: None,
    });
}

#[test]
fn test_keyword_with_space_middle() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from(" hello ").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 1);
    assert!(keyword_map.contains_key("hello"));
    let kw = keyword_map.get("hello").unwrap();
    assert_eq!(kw.splits_matched, 30);
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
        splits_matched: 30,
        parent_keyword: None,
    });
}

#[test]
fn test_row_add_consecutive_rows() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 1u32, &mut keyword_map);
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
        additional_rows: 1,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_row_add_non_consecutive_rows() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    perform_split(String::from("Goodbye").as_str(), col_ref, 0u16, 1u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 2u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 2);
    let kw = keyword_map.get("Hello").unwrap();
    assert_eq!(kw.column_references.len(), 2);
    assert!(kw.column_references.len() >= 2);
    assert_eq!(kw.row_groups.len(), 2);
    assert_eq!(kw.row_groups[0].len(), 1);
    assert_eq!(kw.row_groups[0].as_slice()[0], 0);
    assert_eq!(kw.row_group_to_rows.len(), 2);
    assert_eq!(kw.row_group_to_rows[0].len(), 1);
    assert_eq!(kw.row_group_to_rows[0][0].len(), 2);
    assert_eq!(kw.row_group_to_rows[0][0][0], Row {
        row: 0,
        additional_rows: 0,
        splits_matched: 31,
        parent_keyword: None,
    });
    assert_eq!(kw.row_group_to_rows[0][0][1], Row {
        row: 2,
        additional_rows: 0,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_row_add_multiple_consecutive_rows() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 1u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 2u32, &mut keyword_map);
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
        additional_rows: 2,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_row_add_interspersed_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    perform_split(String::from("Goodbye").as_str(), col_ref, 0u16, 1u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 2u32, &mut keyword_map);
    perform_split(String::from("Goodbye").as_str(), col_ref, 0u16, 3u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 2);
    let kw = keyword_map.get("Hello").unwrap();
    assert_eq!(kw.column_references.len(), 2);
    assert!(kw.column_references.len() >= 2);
    assert_eq!(kw.row_groups.len(), 2);
    assert_eq!(kw.row_groups[0].len(), 1);
    assert_eq!(kw.row_groups[0].as_slice()[0], 0);
    assert_eq!(kw.row_group_to_rows.len(), 2);
    assert_eq!(kw.row_group_to_rows[0].len(), 1);
    assert_eq!(kw.row_group_to_rows[0][0].len(), 2);
    assert_eq!(kw.row_group_to_rows[0][0][0], Row {
        row: 0,
        additional_rows: 0,
        splits_matched: 31,
        parent_keyword: None,
    });
    assert_eq!(kw.row_group_to_rows[0][0][1], Row {
        row: 2,
        additional_rows: 0,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_row_add_consecutive_interspersed_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);
    perform_split(String::from("Hello").as_str(), col_ref, 0u16, 1u32, &mut keyword_map);
    perform_split(String::from("Goodbye").as_str(), col_ref, 0u16, 2u32, &mut keyword_map);
    perform_split(String::from("Goodbye").as_str(), col_ref, 0u16, 3u32, &mut keyword_map);
    println!("{:?}", keyword_map);
    assert_eq!(keyword_map.len(), 2);
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
        additional_rows: 1,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_single_char_keywords_extracted() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("a bc d efg h").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);

    println!("{:?}", keyword_map);

    assert_eq!(keyword_map.len(), 5, "Should extract exactly 5 keywords");

    assert!(keyword_map.contains_key("a"), "Should contain 'a'");
    assert!(keyword_map.contains_key("bc"), "Should contain 'bc'");
    assert!(keyword_map.contains_key("d"), "Should contain 'd'");
    assert!(keyword_map.contains_key("efg"), "Should contain 'efg'");
    assert!(keyword_map.contains_key("h"), "Should contain 'h'");

    let kw_a = keyword_map.get("a").unwrap();
    assert_eq!(kw_a.column_references.len(), 2);
    assert_eq!(kw_a.row_groups.len(), 2);
    assert_eq!(kw_a.row_group_to_rows[0][0][0], Row {
        row: 0,
        additional_rows: 0,
        splits_matched: 30,
        parent_keyword: None,
    });
}

#[test]
fn test_single_digit_extracted() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split(String::from("1").as_str(), col_ref, 0u16, 0u32, &mut keyword_map);

    println!("{:?}", keyword_map);

    assert_eq!(keyword_map.len(), 1, "Should extract exactly 1 keyword");
    assert!(keyword_map.contains_key("1"), "Should contain '1'");

    let kw = keyword_map.get("1").unwrap();
    assert_eq!(kw.column_references.len(), 2);
    assert_eq!(kw.row_groups.len(), 2);
    assert_eq!(kw.row_group_to_rows[0][0][0], Row {
        row: 0,
        additional_rows: 0,
        splits_matched: 31,
        parent_keyword: None,
    });
}

#[test]
fn test_split_all_levels_comprehensive() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    let test_string = "hello world/path:to@email.com$price#tag-dash_underscore";

    perform_split(test_string, col_ref, 0, 0, &mut keyword_map);

    println!("\n=== All Keywords Found ===");
    for (keyword, data) in keyword_map.iter() {
        println!("Keyword: '{}', splits_matched: {:04b}", keyword, data.splits_matched);
    }

    assert!(keyword_map.contains_key("hello"), "Should find 'hello'");
    assert!(keyword_map.contains_key("world"), "Should find 'world'");
    assert!(keyword_map.contains_key("path"), "Should find 'path'");
    assert!(keyword_map.contains_key("to"), "Should find 'to'");
    assert!(keyword_map.contains_key("email"), "Should find 'email'");
    assert!(keyword_map.contains_key("com"), "Should find 'com'");
    assert!(keyword_map.contains_key("price"), "Should find 'price'");
    assert!(keyword_map.contains_key("tag"), "Should find 'tag'");
    assert!(keyword_map.contains_key("dash"), "Should find 'dash'");
    assert!(keyword_map.contains_key("underscore"), "Should find 'underscore'");

    println!("\n=== Test passed! ===");
}

#[test]
fn test_all_level_0_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    let test_cases = vec![
        ("word1 word2", vec!["word1", "word2"]),
        ("word1\tword2", vec!["word1", "word2"]),
        ("word1\nword2", vec!["word1", "word2"]),
        ("word1\rword2", vec!["word1", "word2"]),
        ("word1'word2", vec!["word1", "word2"]),
        ("word1\"word2", vec!["word1", "word2"]),
        ("word1<word2", vec!["word1", "word2"]),
        ("word1>word2", vec!["word1", "word2"]),
        ("word1(word2", vec!["word1", "word2"]),
        ("word1)word2", vec!["word1", "word2"]),
        ("word1|word2", vec!["word1", "word2"]),
        ("word1,word2", vec!["word1", "word2"]),
        ("word1!word2", vec!["word1", "word2"]),
        ("word1;word2", vec!["word1", "word2"]),
        ("word1{word2", vec!["word1", "word2"]),
        ("word1}word2", vec!["word1", "word2"]),
        ("word1*word2", vec!["word1", "word2"]),
    ];

    for (input, expected_words) in test_cases {
        keyword_map.clear();
        perform_split(input, col_ref, 0, 0, &mut keyword_map);

        for word in expected_words {
            assert!(
                keyword_map.contains_key(word),
                "Input '{}' should contain keyword '{}'",
                input.escape_default(),
                word
            );
        }
    }

    println!("All level 0 delimiters tested successfully!");
}

#[test]
fn test_all_level_1_delimiters() {
    let test_cases = vec![
        ("word1/word2", vec!["word1", "word2"]),
        ("word1@word2", vec!["word1", "word2"]),
        ("word1=word2", vec!["word1", "word2"]),
        ("word1:word2", vec!["word1", "word2"]),
        ("word1\\word2", vec!["word1", "word2"]),
        ("word1?word2", vec!["word1", "word2"]),
        ("word1&word2", vec!["word1", "word2"]),
    ];

    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    for (input, expected_words) in test_cases {
        keyword_map.clear();
        perform_split(input, col_ref, 0, 0, &mut keyword_map);

        for word in expected_words {
            assert!(
                keyword_map.contains_key(word),
                "Input '{}' should contain keyword '{}'",
                input.escape_default(),
                word
            );
        }
    }

    println!("All level 1 delimiters tested successfully!");
}

#[test]
fn test_all_level_2_delimiters() {
    let test_cases = vec![
        ("word1.word2", vec!["word1", "word2"]),
        ("word1$word2", vec!["word1", "word2"]),
        ("word1#word2", vec!["word1", "word2"]),
        ("word1`word2", vec!["word1", "word2"]),
        ("word1~word2", vec!["word1", "word2"]),
        ("word1^word2", vec!["word1", "word2"]),
        ("word1+word2", vec!["word1", "word2"]),
    ];

    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    for (input, expected_words) in test_cases {
        keyword_map.clear();
        perform_split(input, col_ref, 0, 0, &mut keyword_map);

        for word in expected_words {
            assert!(
                keyword_map.contains_key(word),
                "Input '{}' should contain keyword '{}'",
                input.escape_default(),
                word
            );
        }
    }

    println!("All level 2 delimiters tested successfully!");
}

#[test]
fn test_all_level_3_delimiters() {
    let test_cases = vec![
        ("word1-word2", vec!["word1", "word2"]),
        ("word1_word2", vec!["word1", "word2"]),
    ];

    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    for (input, expected_words) in test_cases {
        keyword_map.clear();
        perform_split(input, col_ref, 0, 0, &mut keyword_map);

        for word in expected_words {
            assert!(
                keyword_map.contains_key(word),
                "Input '{}' should contain keyword '{}'",
                input.escape_default(),
                word
            );
        }
    }

    println!("All level 3 delimiters tested successfully!");
}

#[test]
fn test_hierarchical_splitting() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("hello-world/test.file", col_ref, 0, 0, &mut keyword_map);

    println!("\nHierarchical split results:");
    for (keyword, data) in keyword_map.iter() {
        println!("  '{}' - splits_matched: {:05b}", keyword, data.splits_matched);
    }

    assert!(keyword_map.contains_key("hello"));
    assert!(keyword_map.contains_key("world"));
    assert!(keyword_map.contains_key("test"));
    assert!(keyword_map.contains_key("file"));

    assert!(keyword_map.contains_key("hello-world"), "Should find 'hello-world' as intermediate");
    assert!(keyword_map.contains_key("test.file"), "Should find 'test.file' as intermediate");

    assert!(keyword_map.contains_key("hello-world/test.file"), "Should find full string");

    let hello_world = keyword_map.get("hello-world").unwrap();
    println!("'hello-world' splits_matched: {:05b}", hello_world.splits_matched);

    let test_file = keyword_map.get("test.file").unwrap();
    println!("'test.file' splits_matched: {:05b}", test_file.splits_matched);
}

#[test]
fn test_empty_segments_ignored() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("word1//word2", col_ref, 0, 0, &mut keyword_map);

    assert!(keyword_map.contains_key("word1"));
    assert!(keyword_map.contains_key("word2"));

    assert!(!keyword_map.contains_key(""), "Should not contain empty string");

    println!("Keywords found: {:?}", keyword_map.keys().collect::<Vec<_>>());
}

#[test]
fn test_complex_real_world_example() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    let url = "https://example.com/api/v1/users?id=123&name=john_doe";
    perform_split(url, col_ref, 0, 0, &mut keyword_map);

    println!("\nComplex example keywords:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    assert!(keyword_map.contains_key("https"));
    assert!(keyword_map.contains_key("example"));
    assert!(keyword_map.contains_key("com"));
    assert!(keyword_map.contains_key("api"));
    assert!(keyword_map.contains_key("v1"));
    assert!(keyword_map.contains_key("users"));
    assert!(keyword_map.contains_key("id"));
    assert!(keyword_map.contains_key("123"));
    assert!(keyword_map.contains_key("name"));
    assert!(keyword_map.contains_key("john"));
    assert!(keyword_map.contains_key("doe"));

    println!("Total keywords extracted: {}", keyword_map.len());
}

// ========== NEW EDGE CASE TESTS ==========

#[test]
fn test_multiple_row_groups() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Add same keyword across different row groups
    perform_split("common", col_ref, 0, 10, &mut keyword_map);  // row_group 0
    perform_split("common", col_ref, 1, 20, &mut keyword_map);  // row_group 1
    perform_split("common", col_ref, 2, 30, &mut keyword_map);  // row_group 2
    perform_split("common", col_ref, 5, 40, &mut keyword_map);  // row_group 5

    let kw = keyword_map.get("common").unwrap();

    // Should have 4 different row groups for column 0 (global)
    assert_eq!(kw.row_groups[0].len(), 4, "Should have 4 row groups");
    assert!(kw.row_groups[0].contains(&0));
    assert!(kw.row_groups[0].contains(&1));
    assert!(kw.row_groups[0].contains(&2));
    assert!(kw.row_groups[0].contains(&5));

    // Each row group should have its corresponding row
    for (i, &rg) in kw.row_groups[0].iter().enumerate() {
        assert_eq!(kw.row_group_to_rows[0][i].len(), 1);
        let expected_row = match rg {
            0 => 10,
            1 => 20,
            2 => 30,
            5 => 40,
            _ => panic!("Unexpected row group"),
        };
        assert_eq!(kw.row_group_to_rows[0][i][0].row, expected_row);
    }

    println!("Multiple row groups test passed!");
}

#[test]
fn test_same_keyword_many_columns() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    // Add "apple" to 6 different columns
    let col1 = column_pool.intern("col1");
    let col2 = column_pool.intern("col2");
    let col3 = column_pool.intern("col3");
    let col4 = column_pool.intern("col4");
    let col5 = column_pool.intern("col5");
    let col6 = column_pool.intern("col6");

    perform_split("apple", col1, 0, 0, &mut keyword_map);
    perform_split("apple", col2, 0, 1, &mut keyword_map);
    perform_split("apple", col3, 0, 2, &mut keyword_map);
    perform_split("apple", col4, 0, 3, &mut keyword_map);
    perform_split("apple", col5, 0, 4, &mut keyword_map);
    perform_split("apple", col6, 0, 5, &mut keyword_map);

    let kw = keyword_map.get("apple").unwrap();

    // Should have 7 column references: global (0) + 6 specific columns
    assert_eq!(kw.column_references.len(), 7, "Should have 7 column references (0 + 6)");
    assert_eq!(kw.column_references[0], 0, "First should be global");

    // Verify all columns are present
    assert!(kw.column_references.contains(&col1));
    assert!(kw.column_references.contains(&col2));
    assert!(kw.column_references.contains(&col3));
    assert!(kw.column_references.contains(&col4));
    assert!(kw.column_references.contains(&col5));
    assert!(kw.column_references.contains(&col6));

    // Should have 7 row_groups vectors
    assert_eq!(kw.row_groups.len(), 7);

    // Should have 7 row_group_to_rows vectors
    assert_eq!(kw.row_group_to_rows.len(), 7);

    println!("Many columns test passed!");
}

#[test]
fn test_non_ascii_characters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Test various non-ASCII strings
    perform_split("hello世界", col_ref, 0, 0, &mut keyword_map);
    perform_split("café", col_ref, 0, 1, &mut keyword_map);
    perform_split("привет", col_ref, 0, 2, &mut keyword_map);
    perform_split("emoji😀test", col_ref, 0, 3, &mut keyword_map);

    println!("\nNon-ASCII keywords found:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    // Non-ASCII characters should not be treated as delimiters
    // They should be part of the keyword
    assert!(keyword_map.contains_key("hello世界"), "Should contain 'hello世界' as one keyword");
    assert!(keyword_map.contains_key("café"), "Should contain 'café'");
    assert!(keyword_map.contains_key("привет"), "Should contain 'привет'");
    assert!(keyword_map.contains_key("emoji😀test"), "Should contain 'emoji😀test'");

    println!("Non-ASCII test passed!");
}

#[test]
fn test_consecutive_rows_different_split_bits() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Add "test" with one split pattern
    perform_split("test", col_ref, 0, 0, &mut keyword_map);

    // Add "test-variant" where "test" has different splits_matched
    perform_split("test-variant", col_ref, 0, 1, &mut keyword_map);

    let kw = keyword_map.get("test").unwrap();

    println!("\nRows for 'test' with different split bits:");
    for (i, row) in kw.row_group_to_rows[0][0].iter().enumerate() {
        println!("  Row {}: row={}, splits_matched={:05b}", i, row.row, row.splits_matched);
    }

    // Should have 2 separate Row entries because splits_matched differs
    assert!(kw.row_group_to_rows[0][0].len() >= 2, "Should have at least 2 Row entries for different split patterns");

    // Verify different splits_matched values
    let splits_0 = kw.row_group_to_rows[0][0][0].splits_matched;
    let splits_1 = kw.row_group_to_rows[0][0][1].splits_matched;
    assert_ne!(splits_0, splits_1, "Split bits should be different");

    println!("Different split bits test passed!");
}

#[test]
fn test_display_keyword_one_file() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("hello world", col_ref, 0, 0, &mut keyword_map);

    let kw = keyword_map.get("hello").unwrap();

    // Test Display trait
    let display_string = format!("{}", kw);

    println!("\nDisplay output: {}", display_string);

    // Should contain key information
    assert!(display_string.contains("splits_matched"));
    assert!(display_string.contains("columns"));
    assert!(display_string.contains("row_groups"));

    println!("Display test passed!");
}

#[test]
fn test_mixed_row_groups_and_columns() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();

    let col1 = column_pool.intern("col1");
    let col2 = column_pool.intern("col2");

    // Add "data" to multiple columns and row groups
    perform_split("data", col1, 0, 10, &mut keyword_map);
    perform_split("data", col1, 1, 20, &mut keyword_map);
    perform_split("data", col2, 0, 30, &mut keyword_map);
    perform_split("data", col2, 2, 40, &mut keyword_map);

    let kw = keyword_map.get("data").unwrap();

    // Should have 3 columns: global (0) + col1 + col2
    assert_eq!(kw.column_references.len(), 3);

    // Column 0 (global) should have all 3 unique row groups
    assert!(kw.row_groups[0].len() >= 3, "Global should have at least 3 row groups");

    println!("Mixed row groups and columns test passed!");
}

#[test]
fn test_deeply_nested_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // String with delimiters at all 4 levels nested
    let nested = "a b/c:d.e-f";
    perform_split(nested, col_ref, 0, 0, &mut keyword_map);

    println!("\nDeeply nested keywords:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    // Should extract all leaf tokens
    assert!(keyword_map.contains_key("a"));
    assert!(keyword_map.contains_key("b"));
    assert!(keyword_map.contains_key("c"));
    assert!(keyword_map.contains_key("d"));
    assert!(keyword_map.contains_key("e"));
    assert!(keyword_map.contains_key("f"));

    println!("Deeply nested test passed!");
}