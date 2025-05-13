use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use parking_lot::Mutex;
use thiserror::Error;

use crate::common::types::{Page, PageId, PAGE_SIZE};

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
            
            let file_size = file.metadata()
                .map_err(DiskManagerError::IoError)?
                .len();
                
            if offset as u64 >= file_size {
                page.page_id = page_id;
                page.data = [0; PAGE_SIZE];
                return Ok(());
            }
            
            file.seek(SeekFrom::Start(offset as u64))
                .map_err(DiskManagerError::IoError)?;
                
            file.read_exact(&mut buffer)
                .map_err(DiskManagerError::IoError)?;
        }
        
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
        
        let file_size = file.metadata()
            .map_err(DiskManagerError::IoError)?
            .len();
            
        let new_page_id = (file_size / PAGE_SIZE as u64) as PageId + 1;
        
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
} 