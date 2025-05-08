use std::sync::Arc;
use tempfile::NamedTempFile;
use bayundb::storage::buffer::BufferPoolManager;
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