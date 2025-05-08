use std::sync::Arc;
use std::mem::forget;
use tempfile::NamedTempFile;
use anyhow::Result;
use bayundb::common::types::Rid;
use bayundb::index::btree::BTreeIndex;

mod common;
use common::create_test_buffer_pool;

#[test]
fn test_btree_create() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create a B+Tree index
    let _btree = BTreeIndex::<i32>::new(buffer_pool)?;
    
    // Successfully creating the B+Tree is enough for this test
    Ok(())
}

#[test]
fn test_btree_insert_find() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(100)?;
    let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    
    // Insert some key-value pairs
    let test_data = [
        (5, 1001),
        (3, 1002),
        (8, 1003),
        (2, 1004),
        (7, 1005),
    ];
    
    for &(key, rid) in &test_data {
        btree.insert(key, rid as Rid)?;
    }
    
    // Find the values
    for &(key, expected_rid) in &test_data {
        let results = btree.find(&key)?;
        assert!(!results.is_empty());
        assert!(results.contains(&(expected_rid as Rid)));
    }
    
    // Try to find a non-existent key
    let non_existent_key = 100;
    let results = btree.find(&non_existent_key)?;
    assert!(results.is_empty());
    
    Ok(())
}

#[test]
fn test_btree_range_scan() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(100)?;
    let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    
    // Insert some key-value pairs in order
    for i in 1..20 {
        btree.insert(i, (1000 + i) as Rid)?;
    }
    
    // Perform range scan
    let start_key = 5;
    let end_key = 10;
    let results = btree.range_scan(&start_key, &end_key)?;
    
    // Should contain values for keys 5, 6, 7, 8, 9, 10
    assert_eq!(results.len(), 6);
    
    // Check if specific rids exist in results
    for i in 5..=10 {
        assert!(results.contains(&((1000 + i) as Rid)));
    }
    
    // Try empty range
    let start_empty = 100;
    let end_empty = 200;
    let results = btree.range_scan(&start_empty, &end_empty)?;
    assert!(results.is_empty());
    
    Ok(())
}

#[test]
fn test_btree_remove() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(100)?;
    let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    
    // Insert some key-value pairs
    for i in 1..10 {
        btree.insert(i, (1000 + i) as Rid)?;
    }
    
    // Find a key
    let key = 5;
    let results_before = btree.find(&key)?;
    assert!(!results_before.is_empty());
    
    // Remove the key
    btree.remove(&key)?;
    
    // Try to find the key again
    let results_after = btree.find(&key)?;
    assert!(results_after.is_empty());
    
    // Other keys should still be findable
    for i in 1..10 {
        if i != key {
            let results = btree.find(&i)?;
            assert!(!results.is_empty());
            assert!(results.contains(&((1000 + i) as Rid)));
        }
    }
    
    Ok(())
} 