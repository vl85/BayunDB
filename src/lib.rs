// Export public modules
pub mod common;
pub mod storage;
pub mod index;

// Re-export key items for convenient access
pub use storage::buffer::BufferPoolManager;
pub use storage::buffer::BufferPoolError;
pub use storage::page::PageManager;
pub use storage::page::PageError;
pub use index::btree::BTreeIndex; 