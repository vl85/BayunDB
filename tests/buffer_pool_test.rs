use anyhow::Result;

mod common;
use common::create_test_buffer_pool;

#[test]
fn test_new_page() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create a new page
    let (page, page_id) = buffer_pool.new_page()?;
    
    // Check that page_id is valid (should be greater than 0)
    assert!(page_id > 0);
    
    // Check that page can be accessed
    {
        let page_guard = page.read();
        assert_eq!(page_guard.page_id, page_id);
    }
    
    // Unpin the page
    buffer_pool.unpin_page(page_id, false)?;
    
    Ok(())
}

#[test]
fn test_fetch_page() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create a new page
    let (_, page_id) = buffer_pool.new_page()?;
    
    // Unpin the page
    buffer_pool.unpin_page(page_id, false)?;
    
    // Fetch the page
    let fetched_page = buffer_pool.fetch_page(page_id)?;
    
    // Check that the page ID matches
    {
        let page_guard = fetched_page.read();
        assert_eq!(page_guard.page_id, page_id);
    }
    
    // Unpin the fetched page
    buffer_pool.unpin_page(page_id, false)?;
    
    Ok(())
}

#[test]
fn test_page_modification() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create a new page
    let (page, page_id) = buffer_pool.new_page()?;
    
    // Modify the page
    {
        let mut page_guard = page.write();
        let test_data = b"Test Data";
        page_guard.data[100..100 + test_data.len()].copy_from_slice(test_data);
    }
    
    // Unpin the page with dirty flag
    buffer_pool.unpin_page(page_id, true)?;
    
    // Fetch the page again
    let fetched_page = buffer_pool.fetch_page(page_id)?;
    
    // Check that modifications persisted
    {
        let page_guard = fetched_page.read();
        let test_data = b"Test Data";
        let page_slice = &page_guard.data[100..100 + test_data.len()];
        assert_eq!(page_slice, test_data);
    }
    
    // Unpin the fetched page
    buffer_pool.unpin_page(page_id, false)?;
    
    Ok(())
}

#[test]
fn test_buffer_pool_eviction() -> Result<()> {
    // Create a buffer pool with just 3 pages
    let (buffer_pool, _temp_file) = create_test_buffer_pool(3)?;
    
    // Create 5 pages to force eviction
    let mut page_ids = Vec::new();
    for _ in 0..5 {
        let (_, page_id) = buffer_pool.new_page()?;
        buffer_pool.unpin_page(page_id, true)?; // Mark as dirty to force flush
        page_ids.push(page_id);
    }
    
    // Now fetch the first page again - this should trigger eviction of some page
    let first_page = buffer_pool.fetch_page(page_ids[0])?;
    
    // Verify we can access the page
    {
        let page_guard = first_page.read();
        assert_eq!(page_guard.page_id, page_ids[0]);
    }
    
    buffer_pool.unpin_page(page_ids[0], false)?;
    
    Ok(())
}

#[test]
fn test_flush_page() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create a new page
    let (page, page_id) = buffer_pool.new_page()?;
    
    // Modify the page
    {
        let mut page_guard = page.write();
        let test_data = b"Test Data For Flushing";
        page_guard.data[100..100 + test_data.len()].copy_from_slice(test_data);
    }
    
    // Unpin the page with dirty flag
    buffer_pool.unpin_page(page_id, true)?;
    
    // Flush the page
    buffer_pool.flush_page(page_id)?;
    
    // Fetch the page again
    let fetched_page = buffer_pool.fetch_page(page_id)?;
    
    // Check that modifications persisted after flush
    {
        let page_guard = fetched_page.read();
        let test_data = b"Test Data For Flushing";
        let page_slice = &page_guard.data[100..100 + test_data.len()];
        assert_eq!(page_slice, test_data);
    }
    
    buffer_pool.unpin_page(page_id, false)?;
    
    Ok(())
}

#[test]
fn test_flush_all_pages() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create several pages and modify them
    let mut page_ids = Vec::new();
    for i in 0..5 {
        let (page, page_id) = buffer_pool.new_page()?;
        
        // Modify the page
        {
            let mut page_guard = page.write();
            let test_data = format!("Test Data {}", i).into_bytes();
            page_guard.data[100..100 + test_data.len()].copy_from_slice(&test_data);
        }
        
        buffer_pool.unpin_page(page_id, true)?; // Mark as dirty
        page_ids.push(page_id);
    }
    
    // Flush all pages
    buffer_pool.flush_all_pages()?;
    
    // Fetch each page and verify data
    for (i, &page_id) in page_ids.iter().enumerate() {
        let fetched_page = buffer_pool.fetch_page(page_id)?;
        
        {
            let page_guard = fetched_page.read();
            let expected_data = format!("Test Data {}", i).into_bytes();
            let page_slice = &page_guard.data[100..100 + expected_data.len()];
            assert_eq!(page_slice, expected_data.as_slice());
        }
        
        buffer_pool.unpin_page(page_id, false)?;
    }
    
    Ok(())
}

#[test]
fn test_delete_page() -> Result<()> {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Create a new page
    let (_, page_id) = buffer_pool.new_page()?;
    
    // Unpin the page
    buffer_pool.unpin_page(page_id, false)?;
    
    // Delete the page
    buffer_pool.delete_page(page_id)?;
    
    // After deletion, the page should be removed from the page table
    // Our implementation allows fetching a deleted page (it will just read from disk)
    // but the page should not be in the buffer pool's page table
    
    // We'll verify this by creating a new page - which should reuse the frame
    // that was previously allocated to our deleted page
    let (new_page, new_page_id) = buffer_pool.new_page()?;
    
    // Check that new page works properly
    {
        let page_guard = new_page.read();
        assert_eq!(page_guard.page_id, new_page_id);
    }
    
    buffer_pool.unpin_page(new_page_id, false)?;
    
    Ok(())
} 