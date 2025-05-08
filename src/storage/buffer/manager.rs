use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;
use anyhow::Result;

use crate::common::types::{Page, PageId, PagePtr, Frame, FrameId, FramePtr};
use crate::storage::disk::DiskManager;
use crate::storage::buffer::error::BufferPoolError;
use crate::storage::buffer::replacer::LRUReplacer;

const INVALID_PAGE_ID: PageId = 0;

pub struct BufferPoolManager {
    pool_size: usize,
    frames: Vec<FramePtr>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free_list: RwLock<VecDeque<FrameId>>,
    replacer: RwLock<LRUReplacer>,
    disk_manager: Arc<DiskManager>,
    next_page_id: RwLock<PageId>,
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
        })
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
            
            // Check if the frame contains a dirty page
            {
                let frame_guard = frame.read();
                dirty = frame_guard.is_dirty;
                if dirty {
                    page_to_write = frame_guard.page.read().clone();
                } else {
                    page_to_write = Page::new(0); // Dummy page, won't be used
                }
            }
            
            // Write dirty page to disk if needed
            if dirty {
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

    /// Create a new page
    pub fn new_page(&self) -> Result<(PagePtr, PageId), BufferPoolError> {
        // Allocate a new page ID from disk manager
        let page_id = self.disk_manager.allocate_page()?;
        
        // Allocate a frame for the new page
        let frame_id = self.allocate_frame()?;
        let frame = &self.frames[frame_id as usize];
        
        {
            let mut frame_guard = frame.write();
            
            // If frame contains a dirty page, flush it first
            if frame_guard.is_dirty {
                let page_guard = frame_guard.page.read();
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

    /// Unpin a page, potentially marking it as dirty
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<(), BufferPoolError> {
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
        
        // Update the pin count and dirty flag
        let pin_count = {
            let frame = &self.frames[frame_id as usize];
            let mut frame_guard = frame.write();
            
            // Decrement pin count if positive
            if frame_guard.pin_count > 0 {
                frame_guard.pin_count -= 1;
            }
            
            // Mark as dirty if requested
            if is_dirty {
                frame_guard.is_dirty = true;
            }
            
            // Return the new pin count
            frame_guard.pin_count
        };
        
        // If pin count is now 0, make it available for replacement
        if pin_count == 0 {
            self.replacer.write().record_access(frame_id);
        }
        
        Ok(())
    }

    /// Flush a specific page to disk
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
        
        // Check if the page is dirty and get a copy if needed
        let frame = &self.frames[frame_id as usize];
        let needs_flush;
        let page_copy;
        
        {
            let frame_guard = frame.read();
            needs_flush = frame_guard.is_dirty;
            
            if needs_flush {
                page_copy = frame_guard.page.read().clone();
            } else {
                page_copy = Page::new(0); // Dummy page, won't be used
            }
        }
        
        // Flush to disk if needed
        if needs_flush {
            // Write the page to disk
            self.disk_manager.write_page(&page_copy)?;
            
            // Update the dirty flag
            let mut frame_guard = frame.write();
            frame_guard.is_dirty = false;
        }
        
        Ok(())
    }

    /// Flush all pages in the buffer pool to disk
    pub fn flush_all_pages(&self) -> Result<(), BufferPoolError> {
        let page_table = self.page_table.read();
        
        for (&page_id, _) in page_table.iter() {
            self.flush_page(page_id)?;
        }
        
        Ok(())
    }

    /// Delete a page from the buffer pool and disk
    pub fn delete_page(&self, page_id: PageId) -> Result<(), BufferPoolError> {
        if page_id == INVALID_PAGE_ID {
            return Err(BufferPoolError::InvalidOperation("Cannot delete invalid page ID".to_string()));
        }
        
        // Check if the page is in the buffer pool
        let frame_id_opt = {
            let mut page_table = self.page_table.write();
            page_table.remove(&page_id)
        };
        
        // If found in buffer pool, reset the frame
        if let Some(frame_id) = frame_id_opt {
            let frame = &self.frames[frame_id as usize];
            let mut frame_guard = frame.write();
            
            // Only delete if not pinned
            if frame_guard.pin_count > 0 {
                // Reinsert into page table since we can't delete it now
                self.page_table.write().insert(page_id, frame_id);
                return Err(BufferPoolError::InvalidOperation(
                    format!("Cannot delete page {} because it is pinned", page_id)
                ));
            }
            
            // Reset the frame
            {
                let mut page_guard = frame_guard.page.write();
                *page_guard = Page::new(INVALID_PAGE_ID);
            }
            
            frame_guard.is_dirty = false;
            frame_guard.pin_count = 0;
            
            // Add the frame to the free list
            drop(frame_guard); // Release lock before modifying free list
            self.replacer.write().remove(frame_id);
            self.free_list.write().push_back(frame_id);
        }
        
        // Note: In a real system, we would also update disk metadata 
        // to mark this page as free for future allocation
        
        Ok(())
    }

    /// Allocate a frame, either from the free list or by page replacement
    fn allocate_frame(&self) -> Result<FrameId, BufferPoolError> {
        // Try to get a frame from the free list first
        if let Some(frame_id) = self.free_list.write().pop_front() {
            return Ok(frame_id);
        }
        
        // No free frames, try to find a victim using the replacer
        if let Some(victim_id) = self.replacer.write().victim() {
            // Check if the frame is unpinned and get the page ID
            let frame = &self.frames[victim_id as usize];
            
            // Get page ID and check if unpinned
            let page_id;
            {
                let frame_guard = frame.read();
                
                // Check pin count
                if frame_guard.pin_count > 0 {
                    return Err(BufferPoolError::BufferPoolFull);
                }
                
                // Get page ID
                let page_guard = frame_guard.page.read();
                page_id = page_guard.page_id;
            }
            
            // Remove page from page table if valid
            if page_id != INVALID_PAGE_ID {
                self.page_table.write().remove(&page_id);
            }
            
            return Ok(victim_id);
        }
        
        // Buffer pool is full (all frames are pinned)
        Err(BufferPoolError::BufferPoolFull)
    }
} 