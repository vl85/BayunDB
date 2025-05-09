use std::sync::Arc;
use tempfile::NamedTempFile;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::query::executor::result::{DataValue, Row};
use anyhow::Result;

// Create a temporary database file for testing
pub fn create_temp_db_file() -> Result<(NamedTempFile, String)> {
    let file = NamedTempFile::new()?;
    let path = file.path().to_str().unwrap().to_string();
    Ok((file, path))
}

// Create a buffer pool manager with a temporary database
pub fn create_test_buffer_pool(pool_size: usize) -> Result<(Arc<BufferPoolManager>, NamedTempFile)> {
    let (file, path) = create_temp_db_file()?;
    let buffer_pool = Arc::new(BufferPoolManager::new(pool_size, path)?);
    Ok((buffer_pool, file))
}

// Generate test data of specified size
pub fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

// Create a test table with data for query tests
// Returns the table ID (simplified) and a vector of sample rows
pub fn create_test_table_with_data(buffer_pool: Arc<BufferPoolManager>) -> Result<(u32, Vec<Row>)> {
    // Create page manager
    let page_manager = PageManager::new();
    
    // Define test data columns
    let columns = vec![
        "id".to_string(),
        "name".to_string(),
        "value".to_string(),
        "active".to_string(),
    ];
    
    // Create test data
    let test_data = vec![
        vec![
            DataValue::Integer(1),
            DataValue::Text("test_item_1".to_string()),
            DataValue::Float(10.5),
            DataValue::Boolean(true),
        ],
        vec![
            DataValue::Integer(2),
            DataValue::Text("test_item_2".to_string()),
            DataValue::Float(20.75),
            DataValue::Boolean(false),
        ],
        vec![
            DataValue::Integer(5),
            DataValue::Text("another_test".to_string()),
            DataValue::Float(50.25),
            DataValue::Boolean(true),
        ],
        vec![
            DataValue::Integer(10),
            DataValue::Text("test_item_10".to_string()),
            DataValue::Float(100.0),
            DataValue::Boolean(true),
        ],
        vec![
            DataValue::Integer(15),
            DataValue::Text("not_a_test".to_string()),
            DataValue::Float(150.5),
            DataValue::Boolean(false),
        ],
    ];
    
    // Create table metadata page
    let (meta_page, meta_page_id) = buffer_pool.new_page()?;
    {
        let mut page_guard = meta_page.write();
        page_manager.init_page(&mut page_guard);
        // Store schema information in the page
        // In a real implementation, this would use a catalog manager
    }
    buffer_pool.unpin_page(meta_page_id, true)?;
    
    // Create a page for test data
    let table_id = 1; // Simplified for testing
    
    // Insert test records
    let mut test_rows = Vec::new();
    
    for row_values in test_data {
        // Create a new page for each record (simplified)
        let (page, page_id) = buffer_pool.new_page()?;
        
        {
            let mut page_guard = page.write();
            page_manager.init_page(&mut page_guard);
            
            // Serialize record data (simplified)
            // In a real implementation, this would use a tuple serializer
            let record_data = format!("{:?}", row_values).into_bytes();
            
            // Insert record into page
            let _rid = page_manager.insert_record(&mut page_guard, &record_data)?;
        }
        
        buffer_pool.unpin_page(page_id, true)?;
        
        // Create a Row object for test verification
        let mut row = Row::new();
        for (i, col) in columns.iter().enumerate() {
            if i < row_values.len() {
                row.set(col.clone(), row_values[i].clone());
            }
        }
        test_rows.push(row);
    }
    
    Ok((table_id, test_rows))
} 