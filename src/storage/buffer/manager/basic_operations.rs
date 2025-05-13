use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;
use anyhow::Result;

use crate::common::types::{Page, PageId, PagePtr, Frame, FrameId, FramePtr, Lsn};
use crate::storage::disk::DiskManager;
use crate::storage::buffer::error::BufferPoolError;
use crate::storage::buffer::replacer::LRUReplacer;
use crate::storage::buffer::manager::frame_management::allocate_frame;
use crate::storage::page::PageManager;
use crate::transaction::Transaction;
use crate::transaction::wal::log_manager::LogManager;

const INVALID_PAGE_ID: PageId = 0;

#[allow(dead_code)]
pub struct BufferPoolManager {
    pub(crate) pool_size: usize,
    pub(crate) frames: Vec<FramePtr>,
    pub(crate) page_table: RwLock<HashMap<PageId, FrameId>>,
    pub(crate) free_list: RwLock<VecDeque<FrameId>>,
    pub(crate) replacer: RwLock<LRUReplacer>,
    pub(crate) disk_manager: Arc<DiskManager>,
    pub(crate) next_page_id: RwLock<PageId>,
    pub(crate) log_manager: Option<Arc<LogManager>>,
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, db_path: impl AsRef<Path>) -> Result<Self, BufferPoolError> {
        let disk_manager = Arc::new(DiskManager::new(db_path)?);
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = VecDeque::with_capacity(pool_size);
        
        for i in 0..pool_size {
            let frame_id = i as FrameId;
            let frame = Frame::new(frame_id, Arc::new(RwLock::new(Page::new(INVALID_PAGE_ID))));
            frames.push(Arc::new(RwLock::new(frame)));
            free_list.push_back(frame_id);
        }

        let bpm_instance = Self {
            pool_size,
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: RwLock::new(free_list),
            replacer: RwLock::new(LRUReplacer::new(pool_size)),
            disk_manager,
            next_page_id: RwLock::new(1),
            log_manager: None,
        };

        Ok(bpm_instance)
    }
    
    /// Create a new buffer pool manager with WAL support
    pub fn new_with_wal(pool_size: usize, db_path: impl AsRef<Path>, 
                         log_manager: Arc<LogManager>) -> Result<Self, BufferPoolError> {
        let mut pool = Self::new(pool_size, db_path)?;
        pool.log_manager = Some(log_manager);
        Ok(pool)
    }

    /// Fetch a page from the buffer pool or disk
    pub fn fetch_page(&self, page_id: PageId) -> Result<PagePtr, BufferPoolError> {
        if page_id == INVALID_PAGE_ID {
            return Err(BufferPoolError::InvalidOperation("Cannot fetch invalid page ID".to_string()));
        }
        
        // Check if page is already in the buffer pool
        if let Some(&frame_id) = self.page_table.read().get(&page_id) {
            // Get the frame reference
            let frame = &self.frames[frame_id as usize];
            
            // Increment pin count
            {
                let mut frame_guard = frame.write();
                frame_guard.pin_count += 1;
            }
            
            // Page is pinned, so it's not an eviction candidate. Remove from replacer.
            self.replacer.write().remove(frame_id);
            
            // Return the page
            let frame_guard = self.frames[frame_id as usize].read();
            return Ok(frame_guard.page.clone());
        }

        // Page not in buffer pool, need to allocate a frame and read from disk
        let frame_id = allocate_frame(self)?;
        
        // Handle potential dirty page in the frame
        {
            let frame_ptr = Arc::clone(&self.frames[frame_id as usize]); // Use Arc::clone
            let dirty;
            let page_to_write_opt: Option<Page>; // Option to hold cloned page
            let lsn: Lsn;
            
            {
                let frame_guard = frame_ptr.read();
                dirty = frame_guard.is_dirty;
                if dirty {
                    page_to_write_opt = Some(frame_guard.page.read().clone());
                    lsn = page_to_write_opt.as_ref().unwrap().lsn; // Get LSN from cloned page
                } else {
                    page_to_write_opt = None;
                    lsn = 0;
                }
            }
            
            if dirty {
                if let Some(ref log_manager) = self.log_manager {
                    log_manager.flush_till_lsn(lsn)?;
                }
                self.disk_manager.write_page(page_to_write_opt.as_ref().unwrap())?; // Use cloned page
            }
        }
        
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, &mut new_page)?;
        
        {
            let frame_ptr = Arc::clone(&self.frames[frame_id as usize]); // Use Arc::clone
            let mut frame_guard = frame_ptr.write();
            
            {
                let mut page_guard = frame_guard.page.write();
                *page_guard = new_page;
            }
            
            frame_guard.pin_count = 1;
            frame_guard.is_dirty = false;
            // Frame is pinned, DO NOT add to replacer.
        }
        
        self.page_table.write().insert(page_id, frame_id);
        // REMOVED: self.replacer.write().record_access(frame_id);
        
        Ok(self.frames[frame_id as usize].read().page.clone())
    }

    /// Create a new page
    pub fn new_page(&self) -> Result<(PagePtr, PageId), BufferPoolError> {
        // Allocate a new page ID from disk manager
        let page_id = self.disk_manager.allocate_page()?;
        
        // Allocate a frame for the new page
        let frame_id = allocate_frame(self)?;
        let frame = &self.frames[frame_id as usize];
        
        {
            let mut frame_guard = frame.write();
            
            // If frame contains a dirty page, flush it first - with WAL protection
            if frame_guard.is_dirty {
                let page_guard = frame_guard.page.read();
                
                // Ensure WAL records are flushed up to page's LSN before writing page
                if let Some(ref log_manager) = self.log_manager {
                    log_manager.flush_till_lsn(page_guard.lsn)?;
                }
                
                self.disk_manager.write_page(&page_guard)?;
            }
            
            // Initialize the new page
            {
                let mut page_guard = frame_guard.page.write();
                *page_guard = Page::new(page_id);
                // Use stateless PageManager by creating an instance
                PageManager::new().init_page(&mut page_guard);
            }
            
            // Update frame metadata
            frame_guard.pin_count = 1;
            frame_guard.is_dirty = true;
        }
        
        // Update page table and LRU replacer
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
            
            // REMOVED: self.replacer.write().record_access(frame_id); // Page is pinned
        }
        
        // Return the page
        let frame_guard = frame.read();
        Ok((frame_guard.page.clone(), page_id))
    }

    /// Unpin a page, potentially marking it as dirty
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<(), BufferPoolError> {
        self.unpin_page_with_txn(page_id, is_dirty, None, None)
    }
    
    /// Flush a page to disk with WAL support
    pub fn flush_page(&self, page_id: PageId) -> Result<(), BufferPoolError> {
        if page_id == INVALID_PAGE_ID {
            return Err(BufferPoolError::InvalidOperation("Cannot flush invalid page ID".to_string()));
        }
        
        // Find the frame containing this page
        let frame_id = {
            let page_table = self.page_table.read();
            match page_table.get(&page_id) {
                Some(&id) => id,
                None => return Err(BufferPoolError::PageNotFound(page_id)),
            }
        };
        
        // Get the page and write to disk
        let frame = &self.frames[frame_id as usize];
        let page_to_write;
        let lsn;
        
        {
            let frame_guard = frame.read();
            let page_guard = frame_guard.page.read();
            page_to_write = page_guard.clone();
            lsn = page_guard.lsn;
        }
        
        // WAL rule: flush log records before writing page to disk
        if let Some(ref log_manager) = self.log_manager {
            log_manager.flush_till_lsn(lsn)?;
        }
        
        // Write the page to disk
        self.disk_manager.write_page(&page_to_write)?;
        
        // Clear the dirty flag
        {
            let mut frame_guard = frame.write();
            frame_guard.is_dirty = false;
        }
        
        Ok(())
    }

    /// Flush all dirty pages to disk
    pub fn flush_all_pages(&self) -> Result<(), BufferPoolError> {
        // Flush log manager first to ensure durability
        if let Some(ref log_manager) = self.log_manager {
            log_manager.flush()?;
        }
        
        // Get all page IDs from the page table
        let page_ids: Vec<PageId> = {
            let page_table = self.page_table.read();
            page_table.keys().cloned().collect()
        };
        
        // Flush each page
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        
        Ok(())
    }
    
    /// Delete a page from the buffer pool
    pub fn delete_page(&self, page_id: PageId) -> Result<(), BufferPoolError> {
        if page_id == INVALID_PAGE_ID {
            return Err(BufferPoolError::InvalidOperation("Cannot delete invalid page ID".to_string()));
        }
        
        // Find the frame containing this page
        let frame_id = {
            let mut page_table = self.page_table.write();
            match page_table.remove(&page_id) {
                Some(id) => id,
                None => return Ok(()), // Page not in buffer pool, nothing to do
            }
        };
        
        // Get the frame and check if it's safe to delete
        let frame = &self.frames[frame_id as usize];
        let pin_count = {
            let frame_guard = frame.read();
            
            // Cannot delete if page is pinned
            if frame_guard.pin_count > 0 {
                return Err(BufferPoolError::PagePinned(page_id));
            }
            
            frame_guard.pin_count
        };
        
        if pin_count == 0 {
            // Reset the frame to hold an invalid page
            {
                let mut frame_guard = frame.write();
                {
                    let mut page_guard = frame_guard.page.write();
                    *page_guard = Page::new(INVALID_PAGE_ID);
                }
                // Now page_guard is dropped, we can modify frame_guard
                frame_guard.is_dirty = false;
                frame_guard.pin_count = 0;
            }
            
            // Remove from replacer and add to free list
            {
                let mut replacer = self.replacer.write();
                replacer.remove(frame_id); // Remove from replacer
                
                // Add to free list
                self.free_list.write().push_back(frame_id);
            }
        }
        
        Ok(())
    }
    
    /// Get a reference to the log manager, if available
    pub fn log_manager(&self) -> Option<Arc<LogManager>> {
        self.log_manager.clone()
    }

    /// Get a reference to the disk manager
    pub fn disk_manager(&self) -> Arc<DiskManager> {
        self.disk_manager.clone()
    }
} 