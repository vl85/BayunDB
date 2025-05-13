use bayundb::storage::page::PageManager;
use bayundb::common::types::{Page, Rid};
use anyhow::Result;

mod common;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::common::types::PAGE_SIZE;
use std::sync::Arc;
use tempfile::NamedTempFile;

#[test]
fn test_page_init() {
    let page_manager = PageManager::new();
    let mut page = Page::new(1);
    
    page_manager.init_page(&mut page);
    
    // After initialization, the page should have some free space
    let header = page_manager.get_header(&page);
    assert_eq!(header.record_count, 0);
    assert!(header.free_space_size > 0);
}

#[test]
fn test_insert_retrieve_record() -> Result<()> {
    let page_manager = PageManager::new();
    let mut page = Page::new(1);
    page_manager.init_page(&mut page);
    
    // Insert a record
    let test_data = b"Hello, Database!";
    let rid = page_manager.insert_record(&mut page, test_data)?;
    
    // Check that the RID is valid
    assert_eq!(rid, Rid::new(1, 0));
    
    // Retrieve the record
    let retrieved_data = page_manager.get_record(&page, rid)?;
    
    // Check that retrieved data matches inserted data
    assert_eq!(retrieved_data, test_data);
    
    Ok(())
}

#[test]
fn test_update_record() -> Result<()> {
    let page_manager = PageManager::new();
    let mut page = Page::new(1);
    page_manager.init_page(&mut page);
    
    // Insert a record
    let test_data = b"Hello, Database!";
    let rid = page_manager.insert_record(&mut page, test_data)?;
    
    // Update the record
    let new_data = b"Updated record data";
    page_manager.update_record(&mut page, rid, new_data)?;
    
    // Retrieve and check the updated record
    let retrieved_data = page_manager.get_record(&page, rid)?;
    assert_eq!(retrieved_data, new_data);
    
    Ok(())
}

#[test]
fn test_delete_record() -> Result<()> {
    let page_manager = PageManager::new();
    let mut page = Page::new(1);
    page_manager.init_page(&mut page);
    
    // Insert a record
    let test_data = b"Hello, Database!";
    let rid = page_manager.insert_record(&mut page, test_data)?;
    
    // Delete the record
    page_manager.delete_record(&mut page, rid)?;
    
    // Attempt to retrieve the deleted record should fail
    let result = page_manager.get_record(&page, rid);
    assert!(result.is_err());
    
    Ok(())
}

#[test]
fn test_multiple_records() -> Result<()> {
    let page_manager = PageManager::new();
    let mut page = Page::new(1);
    page_manager.init_page(&mut page);
    
    // Insert multiple records
    let records = vec![
        b"Record 1".to_vec(),
        b"Record 2 with more data".to_vec(),
        b"Record 3 with even more data for testing".to_vec(),
    ];
    
    let mut rids = Vec::new();
    for record in &records {
        let rid = page_manager.insert_record(&mut page, record)?;
        rids.push(rid);
    }
    
    // Verify all records can be retrieved
    for (i, rid) in rids.iter().enumerate() {
        let retrieved = page_manager.get_record(&page, *rid)?;
        assert_eq!(retrieved, records[i]);
    }
    
    Ok(())
}

#[test]
fn test_page_compaction() -> Result<()> {
    let page_manager = PageManager::new();
    let mut page = Page::new(1);
    page_manager.init_page(&mut page);
    
    // Insert several records
    let records = vec![
        b"Record 1".to_vec(),
        b"Record 2".to_vec(),
        b"Record 3".to_vec(),
        b"Record 4".to_vec(),
        b"Record 5".to_vec(),
    ];
    
    let mut rids = Vec::new();
    for record in &records {
        let rid = page_manager.insert_record(&mut page, record)?;
        rids.push(rid);
    }
    
    // Check free space before deletion
    let free_space_before = page_manager.get_free_space(&page);
    
    // Delete some records
    page_manager.delete_record(&mut page, rids[1])?; // Delete "Record 2"
    page_manager.delete_record(&mut page, rids[3])?; // Delete "Record 4"
    
    // Compact the page
    page_manager.compact_page(&mut page)?;
    
    // Check free space after compaction
    let free_space_after = page_manager.get_free_space(&page);
    
    // Free space should increase after compaction
    assert!(free_space_after > free_space_before);
    
    // The records that should remain are:
    // - Record 1 (index 0)
    // - Record 3 (index 2)
    // - Record 5 (index 4)
    
    // After compaction, the RIDs might have changed.
    // So we need to retrieve by RID 0, 1, and 2 which is the order
    // they would be placed in the compacted page
    
    // Verify remaining records can still be retrieved in the compacted page
    let expected_records = [&records[0], &records[2], &records[4]];
    
    for i in 0..expected_records.len() {
        let result = page_manager.get_record(&page, Rid::new(1, i as u32));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), *expected_records[i]);
    }
    
    Ok(())
}

// Helper to create a BufferPoolManager for tests
fn setup_bpm(db_file_path: &str, pool_size: usize) -> Arc<BufferPoolManager> {
    Arc::new(
        BufferPoolManager::new(pool_size, db_file_path)
            .expect("Failed to create BufferPoolManager for test")
    )
}

// Helper to get a new page from BPM and initialize it with PageManager
// This assumes PageManager might need a BPM or that we operate on pages managed by BPM.
// If PageManager is independent of BPM for its direct operations but BPM is needed for page *allocation*,
// the tests will need to reflect that.
fn get_initialized_page_from_bpm(
    bpm: Arc<BufferPoolManager>,
    page_manager: &PageManager,
) -> Result<(bayundb::common::types::PagePtr, u32)> { // Assuming PagePtr is Arc<RwLock<Page>>
    let (page_arc, page_id) = bpm.new_page()?;
    {
        let mut page_guard = page_arc.write();
        page_manager.init_page(&mut page_guard); // Assuming init_page takes &mut Page
    }
    Ok((page_arc, page_id))
}

#[test]
fn test_update_record_grows_no_split() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let bpm = setup_bpm(temp_file.path().to_str().unwrap(), 10);
    let page_manager = PageManager::new();

    let (page_arc, page_id) = get_initialized_page_from_bpm(bpm.clone(), &page_manager)?;
    let mut page_guard = page_arc.write();

    let initial_data = b"short";
    let rid = page_manager.insert_record(&mut page_guard, initial_data)?;
    assert_eq!(rid.page_id, page_id);

    let free_space_before_grow = page_manager.get_free_space(&page_guard);

    let grown_data = b"this is a longer string";
    page_manager.update_record(&mut page_guard, rid, grown_data)?;

    let retrieved_data = page_manager.get_record(&page_guard, rid)?;
    assert_eq!(retrieved_data, grown_data);

    let free_space_after_grow = page_manager.get_free_space(&page_guard);
    assert!(free_space_after_grow < free_space_before_grow);

    // Ensure no split occurred. PageManager.get_next_page_id returns Result<Option<u32>, PageError>
    // For a single page that hasn't split, next_page_id in its header should be 0 or logically None.
    assert_eq!(page_manager.get_next_page_id(&page_guard)?.unwrap_or(0), 0, "Expected no split, next_page_id should be 0 or None");


    drop(page_guard);
    bpm.unpin_page(page_id, true)?; 
    bpm.flush_all_pages()?;
    Ok(())
}

#[test]
fn test_update_record_shrinks() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let bpm = setup_bpm(temp_file.path().to_str().unwrap(), 10);
    let page_manager = PageManager::new();

    let (page_arc, page_id) = get_initialized_page_from_bpm(bpm.clone(), &page_manager)?;
    let mut page_guard = page_arc.write();

    let initial_data = b"this is a very long initial string for testing shrinkage";
    let rid = page_manager.insert_record(&mut page_guard, initial_data)?;
    
    let free_space_before_shrink = page_manager.get_free_space(&page_guard);

    let shrunk_data = b"short";
    page_manager.update_record(&mut page_guard, rid, shrunk_data)?;

    let retrieved_data = page_manager.get_record(&page_guard, rid)?;
    assert_eq!(retrieved_data, shrunk_data);

    let free_space_after_shrink = page_manager.get_free_space(&page_guard);
    assert!(free_space_after_shrink > free_space_before_shrink);
    
    drop(page_guard);
    bpm.unpin_page(page_id, true)?;
    bpm.flush_all_pages()?;
    Ok(())
}

// This test is more complex and relies heavily on PageManager's internal logic for splitting
// and its interaction with a BPM (or equivalent page allocation mechanism).
#[test]
fn test_insert_causes_page_split_or_reports_insufficient_space() -> Result<()> { // Renamed to reflect current PM capability
    let temp_file = NamedTempFile::new()?;
    let bpm = setup_bpm(temp_file.path().to_str().unwrap(), 10);
    let page_manager = PageManager::new(); 

    let (page1_arc, page1_id) = get_initialized_page_from_bpm(bpm.clone(), &page_manager)?;
    
    let mut _last_rid_page1 = None;
    let mut records_inserted_on_page1 = 0;

    { 
        let mut page1_guard = page1_arc.write();
        // Fill the page until PageManager reports InsufficientSpace
        loop {
            let data = format!("record_data_long_enough_to_fill_{}", records_inserted_on_page1).repeat(5); // ~50-60 bytes + text
            let data_bytes = data.as_bytes();
            
            match page_manager.insert_record(&mut page1_guard, data_bytes) {
                Ok(rid) => {
                    _last_rid_page1 = Some(rid);
                    records_inserted_on_page1 += 1;
                }
                Err(bayundb::storage::page::PageError::InsufficientSpace) => { // Assuming PageError is accessible
                    println!("Page {} is now full after {} records.", page1_id, records_inserted_on_page1);
                    break; 
                }
                Err(e) => return Err(e.into()), // Other unexpected error
            }
            // Safety break if PAGE_SIZE is very small or records very small, to avoid infinite loop in test
            if records_inserted_on_page1 > (PAGE_SIZE / 10) + 100 { // Arbitrary large number
                 panic!("Test inserted too many records, possible issue in free space logic or test itself.");
            }
        }
        
        assert!(records_inserted_on_page1 > 0, "Should have inserted some records on page 1 before it became full");
        // Check next_page_id before attempting an operation that *would* split if PM supported it.
        // It should still be 0 (or None) as no split has occurred.
        assert_eq!(page_manager.get_next_page_id(&page1_guard)?.unwrap_or(0), 0, "Page 1 next_page_id should be 0 before any split attempt");
    }
    bpm.unpin_page(page1_id, true)?;


    // Now attempt to insert one more record, which should fail due to InsufficientSpace
    // as PageManager does not currently implement splits.
    let page1_arc_refetch = bpm.fetch_page(page1_id)?; 
    let mut page1_guard_for_final_insert = page1_arc_refetch.write();

    let split_trigger_data = b"THIS RECORD SHOULD NOT FIT AND CAUSE InsufficientSpace";
    
    match page_manager.insert_record(&mut page1_guard_for_final_insert, split_trigger_data) {
        Ok(_rid) => {
            panic!("PageManager insert_record succeeded on a full page. This test expects it to fail without split logic, or page was not actually full.");
        }
        Err(bayundb::storage::page::PageError::InsufficientSpace) => {
            // This is the expected behavior for the current PageManager
            println!("Successfully confirmed PageManager returns InsufficientSpace on a full page.");
        }
        Err(e) => return Err(e.into()), // Other unexpected error
    }

    // Verify that page1's next_page_id is still 0 (or None) because no split occurred.
    let page1_next_page_id_after_attempt = page_manager.get_next_page_id(&page1_guard_for_final_insert)?.unwrap_or(0);
    assert_eq!(page1_next_page_id_after_attempt, 0, "Page 1 next_page_id should still be 0 as no split happened");
    
    drop(page1_guard_for_final_insert);
    bpm.unpin_page(page1_id, true)?; // Page was dirtied by attempts to fill it
    
    bpm.flush_all_pages()?;
    Ok(())
}

// test_update_causes_page_split would be similar to test_insert_causes_page_split,
// but would call update_record on a nearly full page with data that makes it grow.

// test_page_chain_integrity_after_splits would involve multiple splits and then
// iterating through the pages using bpm.fetch_page(next_id) and reading page.get_next_page_id()
// until 0 is encountered, counting pages to ensure the chain is correct. 