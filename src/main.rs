use std::sync::Arc;
use std::path::PathBuf;
use anyhow::Result;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::index::btree::BTreeIndex;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;

fn main() -> Result<()> {
    // Create a database file path
    let db_path = "database.db";
    
    // Create logs directory if it doesn't exist
    let log_dir = PathBuf::from("logs");
    std::fs::create_dir_all(&log_dir)?;
    
    // Configure log manager
    let log_config = LogManagerConfig {
        log_dir,
        log_file_base_name: "bayun_log".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: true, // Force sync for safety
    };
    
    // Create log manager
    let log_manager = Arc::new(LogManager::new(log_config)?);
    println!("Log manager initialized successfully");
    
    // Create buffer pool manager with 1000 pages and WAL support
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        1000,
        db_path,
        log_manager.clone(),
    )?);
    println!("Buffer pool manager with WAL initialized successfully");
    
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
    
    println!("Database operations completed successfully with WAL support");
    
    Ok(())
} 