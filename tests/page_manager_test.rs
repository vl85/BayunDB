use bayundb::storage::page::PageManager;
use bayundb::common::types::Page;
use anyhow::Result;

mod common;

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
    assert_eq!(rid, 0);
    
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
    let expected_records = vec![&records[0], &records[2], &records[4]];
    
    for i in 0..expected_records.len() {
        let result = page_manager.get_record(&page, i as u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), *expected_records[i]);
    }
    
    Ok(())
} 