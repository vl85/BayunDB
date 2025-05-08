use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use parking_lot::Mutex;
use thiserror::Error;

use crate::common::types::{Page, PageId, PAGE_SIZE};
use crate::storage::page::PageManager;

const INVALID_PAGE_ID: PageId = 0;

#[derive(Error, Debug)]
pub enum DiskManagerError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid page ID: {0}")]
    InvalidPageId(PageId),
}

/// DiskManager is responsible for handling the actual disk I/O operations
pub struct DiskManager {
    db_file: Mutex<File>,
    page_manager: PageManager,
}

impl DiskManager {
    /// Create a new DiskManager with the specified database file
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self, DiskManagerError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)
            .map_err(DiskManagerError::IoError)?;
        
        Ok(Self {
            db_file: Mutex::new(file),
            page_manager: PageManager::new(),
        })
    }
    
    /// Read a page from disk
    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), DiskManagerError> {
        if page_id == INVALID_PAGE_ID {
            return Err(DiskManagerError::InvalidPageId(page_id));
        }
        
        let offset = self.page_offset(page_id);
        let mut buffer = [0u8; PAGE_SIZE];
        
        {
            let mut file = self.db_file.lock();
            
            // Check if the file is long enough to contain this page
            let file_size = file.metadata()
                .map_err(DiskManagerError::IoError)?
                .len();
                
            // If the file isn't long enough, initialize a new page
            if offset as u64 >= file_size {
                page.page_id = page_id;
                self.page_manager.init_page(page);
                return Ok(());
            }
            
            // Seek to the page location and read it
            file.seek(SeekFrom::Start(offset as u64))
                .map_err(DiskManagerError::IoError)?;
                
            file.read_exact(&mut buffer)
                .map_err(DiskManagerError::IoError)?;
        }
        
        // Copy read data to the page
        page.data.copy_from_slice(&buffer);
        page.page_id = page_id;
        
        Ok(())
    }
    
    /// Write a page to disk
    pub fn write_page(&self, page: &Page) -> Result<(), DiskManagerError> {
        if page.page_id == INVALID_PAGE_ID {
            return Err(DiskManagerError::InvalidPageId(page.page_id));
        }
        
        let offset = self.page_offset(page.page_id);
        
        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(DiskManagerError::IoError)?;
            
        file.write_all(&page.data)
            .map_err(DiskManagerError::IoError)?;
            
        file.flush()
            .map_err(DiskManagerError::IoError)?;
            
        Ok(())
    }
    
    /// Allocate a new page on disk and return its ID
    pub fn allocate_page(&self) -> Result<PageId, DiskManagerError> {
        let mut file = self.db_file.lock();
        
        // Get the current file size to determine the next page ID
        let file_size = file.metadata()
            .map_err(DiskManagerError::IoError)?
            .len();
            
        // Calculate the new page ID (first page is 1, not 0)
        let new_page_id = (file_size / PAGE_SIZE as u64) as PageId + 1;
        
        // Extend the file with a new page of zeros
        file.seek(SeekFrom::End(0))
            .map_err(DiskManagerError::IoError)?;
            
        let zeros = [0u8; PAGE_SIZE];
        file.write_all(&zeros)
            .map_err(DiskManagerError::IoError)?;
            
        file.flush()
            .map_err(DiskManagerError::IoError)?;
            
        Ok(new_page_id)
    }
    
    /// Calculate the offset of a page in the file
    fn page_offset(&self, page_id: PageId) -> usize {
        (page_id as usize - 1) * PAGE_SIZE
    }
    
    /// Get a reference to the page manager
    pub fn page_manager(&self) -> &PageManager {
        &self.page_manager
    }
} 