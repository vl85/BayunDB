use crate::common::types::{PageId, PagePtr, Lsn};
use crate::storage::buffer::error::BufferPoolError;
use crate::storage::page::PageManager;
use crate::transaction::Transaction;
use super::basic_operations::BufferPoolManager;
use super::frame_management::allocate_frame;

impl BufferPoolManager {
    /// Create a new page with transaction support
    pub fn new_page_with_txn(&self, txn: &Transaction) -> Result<(PagePtr, PageId), BufferPoolError> {
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
                *page_guard = crate::common::types::Page::new(page_id);
                // Use stateless PageManager by creating an instance
                PageManager::new().init_page(&mut page_guard);
                
                // Log the page creation operation (create a "page header" record)
                // This is purely for recovery purposes
                if let Some(_) = self.log_manager {
                    // Extract page header for logging
                    // Use stateless PageManager by creating an instance
                    let header_bytes = PageManager::new().get_raw_page_header(&page_guard);
                    
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
            
            // REMOVED: self.replacer.write().record_access(frame_id); // Page is pinned
        }
        
        // Return the page
        let frame_guard = frame.read();
        Ok((frame_guard.page.clone(), page_id))
    }

    /// Unpin a page with transaction support
    pub fn unpin_page_with_txn(&self, page_id: PageId, is_dirty: bool, 
                               txn: Option<&Transaction>, lsn: Option<Lsn>) 
                              -> Result<(), BufferPoolError> {
        if page_id == 0 { // INVALID_PAGE_ID
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
} 