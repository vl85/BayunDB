use std::sync::Arc;
use anyhow::Result;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::index::btree::BTreeIndex;

fn main() -> Result<()> {
    // Create a database file path
    let db_path = "database.db";
    
    // Create buffer pool manager with 1000 pages
    let buffer_pool = Arc::new(BufferPoolManager::new(1000, db_path)?);
    
    // Create page manager
    let page_manager = PageManager::new();
    
    // Create B+Tree index for integer keys
    let btree_index = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    
    // Example: Create a new page
    let (page, page_id) = buffer_pool.new_page()?;
    println!("Created new page with ID: {}", page_id);
    
    // Initialize the page
    {
        let mut page_guard = page.write();
        page_manager.init_page(&mut page_guard);
    }
    
    // Example: Insert a record
    {
        let mut page_guard = page.write();
        let data = b"Hello, Database!";
        if let Ok(rid) = page_manager.insert_record(&mut page_guard, data) {
            println!("Inserted record with RID: {}", rid);
            
            // Example: Insert into B+Tree
            btree_index.insert(42, rid)?;
        }
    }
    
    // Unpin the page when done (with dirty flag)
    buffer_pool.unpin_page(page_id, true)?;
    
    Ok(())
} 