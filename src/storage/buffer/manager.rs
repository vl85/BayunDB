use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;
use anyhow::Result;

use crate::common::types::{Page, PageId, PagePtr, Frame, FrameId, FramePtr, Lsn};
use crate::storage::disk::DiskManager;
use crate::storage::buffer::error::BufferPoolError;
use crate::storage::buffer::replacer::LRUReplacer;
use crate::transaction::Transaction;
use crate::transaction::wal::log_manager::LogManager;

const INVALID_PAGE_ID: PageId = 0;

pub struct BufferPoolManager {
    pool_size: usize,
    frames: Vec<FramePtr>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free_list: RwLock<VecDeque<FrameId>>,
    replacer: RwLock<LRUReplacer>,
    disk_manager: Arc<DiskManager>,
    next_page_id: RwLock<PageId>,
    log_manager: Option<Arc<LogManager>>,
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

        Ok(Self {
            pool_size,
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: RwLock::new(free_list),
            replacer: RwLock::new(LRUReplacer::new(pool_size)),
            disk_manager,
            next_page_id: RwLock::new(1), // Start with page ID 1
            log_manager: None,
        })
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
            
            // Update access time in LRU replacer
            self.replacer.write().record_access(frame_id);
            
            // Return the page
            let frame_guard = self.frames[frame_id as usize].read();
            return Ok(frame_guard.page.clone());
        }

        // Page not in buffer pool, need to allocate a frame and read from disk
        let frame_id = self.allocate_frame()?;
        
        // Handle potential dirty page in the frame
        {
            let frame = &self.frames[frame_id as usize];
            let dirty;
            let page_to_write;
            let lsn: Lsn;
            
            // Check if the frame contains a dirty page
            {
                let frame_guard = frame.read();
                dirty = frame_guard.is_dirty;
                if dirty {
                    page_to_write = frame_guard.page.read().clone();
                    lsn = page_to_write.lsn;
                } else {
                    page_to_write = Page::new(0); // Dummy page, won't be used
                    lsn = 0;
                }
            }
            
            // Write dirty page to disk if needed - WAL rule: must flush log before writing page
            if dirty {
                // Ensure WAL records are flushed up to page's LSN before writing page
                if let Some(ref log_manager) = self.log_manager {
                    log_manager.flush_till_lsn(lsn)?;
                }
                
                self.disk_manager.write_page(&page_to_write)?;
            }
        }
        
        // Read the page from disk into a temporary buffer
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, &mut new_page)?;
        
        // Update the frame with new page data
        {
            let frame = &self.frames[frame_id as usize];
            let mut frame_guard = frame.write();
            
            // Replace the page in the frame
            {
                let mut page_guard = frame_guard.page.write();
                *page_guard = new_page;
            }
            
            frame_guard.pin_count = 1;
            frame_guard.is_dirty = false;
        }
        
        // Update page table and LRU replacer
        self.page_table.write().insert(page_id, frame_id);
        self.replacer.write().record_access(frame_id);
        
        // Return the page
        let frame_guard = self.frames[frame_id as usize].read();
        Ok(frame_guard.page.clone())
    }

    /// Create a new page with transaction support
    pub fn new_page_with_txn(&self, txn: &Transaction) -> Result<(PagePtr, PageId), BufferPoolError> {
        // Allocate a new page ID from disk manager
        let page_id = self.disk_manager.allocate_page()?;
        
        // Allocate a frame for the new page
        let frame_id = self.allocate_frame()?;
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
                self.disk_manager.page_manager().init_page(&mut page_guard);
                
                // Log the page creation operation (create a "page header" record)
                // This is purely for recovery purposes
                if let Some(_) = self.log_manager {
                    // Extract page header for logging
                    let header_bytes = self.disk_manager.page_manager().get_raw_page_header(&page_guard);
                    
                    // Log the page initialization - this updates the LSN on the page
                    let lsn = txn.log_insert(0, page_id, 0, &header_bytes)?;
                    page_guard.lsn = lsn;
                }
            }
            
            // Update frame metadata
            frame_guard.pin_count = 1;
            frame_guard.is_dirty = true;
        }
        
        // Update page table and LRU replacer
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
            
            self.replacer.write().record_access(frame_id);
        }
        
        // Return the page
        let frame_guard = frame.read();
        Ok((frame_guard.page.clone(), page_id))
    }

    /// Create a new page
    pub fn new_page(&self) -> Result<(PagePtr, PageId), BufferPoolError> {
        // Allocate a new page ID from disk manager
        let page_id = self.disk_manager.allocate_page()?;
        
        // Allocate a frame for the new page
        let frame_id = self.allocate_frame()?;
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
                self.disk_manager.page_manager().init_page(&mut page_guard);
            }
            
            // Update frame metadata
            frame_guard.pin_count = 1;
            frame_guard.is_dirty = true;
        }
        
        // Update page table and LRU replacer
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
            
            self.replacer.write().record_access(frame_id);
        }
        
        // Return the page
        let frame_guard = frame.read();
        Ok((frame_guard.page.clone(), page_id))
    }

    /// Unpin a page with transaction support
    pub fn unpin_page_with_txn(&self, page_id: PageId, is_dirty: bool, 
                              txn: Option<&Transaction>, lsn: Option<Lsn>) 
                             -> Result<(), BufferPoolError> {
        if page_id == INVALID_PAGE_ID {
            return Err(BufferPoolError::InvalidOperation("Cannot unpin invalid page ID".to_string()));
        }
        
        // Find the frame containing this page
        let frame_id = {
            let page_table = self.page_table.read();
            match page_table.get(&page_id) {
                Some(&id) => id,
                None => return Err(BufferPoolError::PageNotFound(page_id)),
            }
        };
        
        // Update the pin count, dirty flag, and LSN 
        let pin_count = {
            let frame = &self.frames[frame_id as usize];
            let mut frame_guard = frame.write();
            
            // Decrement pin count if positive
            if frame_guard.pin_count > 0 {
                frame_guard.pin_count -= 1;
            }
            
            // Mark as dirty if requested, and update LSN if provided
            if is_dirty {
                frame_guard.is_dirty = true;
                
                // If transaction and LSN are provided, update the page's LSN
                if let (Some(_), Some(record_lsn)) = (txn, lsn) {
                    let mut page_guard = frame_guard.page.write();
                    page_guard.lsn = record_lsn;
                }
            }
            
            // Return the new pin count
            frame_guard.pin_count
        };
        
        // If pin count is zero, page can be evicted, add to replacer
        if pin_count == 0 {
            let mut replacer = self.replacer.write();
            replacer.record_access(frame_id);
        }
        
        Ok(())
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
    
    /// Allocate a frame for a new page
    fn allocate_frame(&self) -> Result<FrameId, BufferPoolError> {
        // Try to get a frame from the free list first
        {
            let mut free_list = self.free_list.write();
            if let Some(frame_id) = free_list.pop_front() {
                return Ok(frame_id);
            }
        }
        
        // No frames in free list, need to evict a page
        let frame_id = {
            let mut replacer = self.replacer.write();
            match replacer.victim() {
                Some(id) => id,
                None => return Err(BufferPoolError::NoFreeFrames),
            }
        };
        
        // Remove the evicted page from the page table
        {
            let frame = &self.frames[frame_id as usize];
            let page_id = frame.read().page.read().page_id;
            
            // Only remove from page table if it's a valid page
            if page_id != INVALID_PAGE_ID {
                self.page_table.write().remove(&page_id);
            }
        }
        
        Ok(frame_id)
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