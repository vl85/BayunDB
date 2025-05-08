use std::sync::Arc;
use anyhow::Result;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::index::btree::BTreeIndex;
use bayundb::common::types::Rid;

// Import test utilities
#[path = "../common/mod.rs"]
mod common;
use common::{create_test_buffer_pool, generate_test_data};

#[test]
fn test_database_end_to_end() -> Result<()> {
    // Create buffer pool with temp file
    let (buffer_pool, _temp_file) = create_test_buffer_pool(100)?;
    
    // Create page manager
    let page_manager = PageManager::new();
    
    // Create B+Tree index
    let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    
    // Test data
    let records = [
        (1, b"Record for key 1".to_vec()),
        (5, b"Record for key 5".to_vec()),
        (10, b"Record for key 10".to_vec()),
        (15, b"Record for key 15".to_vec()),
        (20, b"Record for key 20".to_vec()),
    ];
    
    // Insert records into pages and index them
    let mut record_pages = Vec::new();
    
    for &(key, ref data) in &records {
        // Create a new page for each record
        let (page, page_id) = buffer_pool.new_page()?;
        
        // Initialize the page
        {
            let mut page_guard = page.write();
            page_manager.init_page(&mut page_guard);
            
            // Insert record into page
            let rid = page_manager.insert_record(&mut page_guard, data)?;
            
            // Add to index
            btree.insert(key, rid)?;
            
            // Store page_id and rid for verification
            record_pages.push((key, page_id, rid));
        }
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, true)?;
    }
    
    // Verify records can be found through the index
    for &(key, ref expected_data) in &records {
        // Find the record using the index
        let rids = btree.find(&key)?;
        assert!(!rids.is_empty());
        
        // Get page_id for this record
        let record_info = record_pages.iter().find(|&&(k, _, _)| k == key).unwrap();
        let (_, page_id, rid) = *record_info;
        
        // Fetch the page
        let page = buffer_pool.fetch_page(page_id)?;
        
        // Get the record data
        let retrieved_data = {
            let page_guard = page.read();
            page_manager.get_record(&page_guard, rid)?
        };
        
        // Verify data matches
        assert_eq!(retrieved_data, *expected_data);
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, false)?;
    }
    
    // Test updating a record
    {
        // Get info for a record to update
        let (key, page_id, rid) = record_pages[2]; // Update the third record
        
        // Fetch the page
        let page = buffer_pool.fetch_page(page_id)?;
        
        // Update the record
        let updated_data = b"UPDATED: Record with new data".to_vec();
        {
            let mut page_guard = page.write();
            page_manager.update_record(&mut page_guard, rid, &updated_data)?;
        }
        
        // Unpin page with dirty flag
        buffer_pool.unpin_page(page_id, true)?;
        
        // Verify update through index lookup
        let rids = btree.find(&key)?;
        assert!(rids.contains(&rid));
        
        // Fetch page again
        let page = buffer_pool.fetch_page(page_id)?;
        
        // Get updated record
        let retrieved_data = {
            let page_guard = page.read();
            page_manager.get_record(&page_guard, rid)?
        };
        
        // Verify data updated correctly
        assert_eq!(retrieved_data, updated_data);
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, false)?;
    }
    
    // Test deleting a record and from the index
    {
        // Get info for a record to delete
        let (key, page_id, rid) = record_pages[0]; // Delete the first record
        
        // Remove from index
        btree.remove(&key)?;
        
        // Fetch the page
        let page = buffer_pool.fetch_page(page_id)?;
        
        // Delete the record
        {
            let mut page_guard = page.write();
            page_manager.delete_record(&mut page_guard, rid)?;
        }
        
        // Unpin page with dirty flag
        buffer_pool.unpin_page(page_id, true)?;
        
        // Verify record is no longer in index
        let rids = btree.find(&key)?;
        assert!(rids.is_empty());
        
        // Fetch page again
        let page = buffer_pool.fetch_page(page_id)?;
        
        // Verify record is deleted
        let result = {
            let page_guard = page.read();
            page_manager.get_record(&page_guard, rid)
        };
        assert!(result.is_err());
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, false)?;
    }
    
    // Flush all changes to disk
    buffer_pool.flush_all_pages()?;
    
    Ok(())
} 