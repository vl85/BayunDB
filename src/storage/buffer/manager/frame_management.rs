use crate::common::types::{FrameId, PageId};
use crate::storage::buffer::error::BufferPoolError;
use super::basic_operations::BufferPoolManager;

/// Allocate a frame for a new page
pub fn allocate_frame(bpm: &BufferPoolManager) -> Result<FrameId, BufferPoolError> {
    // Try to get a frame from the free list first
    {
        let mut free_list = bpm.free_list.write();
        if let Some(frame_id) = free_list.pop_front() {
            return Ok(frame_id);
        }
    }
    
    // No frames in free list, need to evict a page
    let frame_id = {
        let mut replacer = bpm.replacer.write();
        match replacer.victim() {
            Some(id) => id,
            None => return Err(BufferPoolError::NoFreeFrames),
        }
    };
    
    // Remove the evicted page from the page table
    {
        let frame = &bpm.frames[frame_id as usize];
        let page_id = frame.read().page.read().page_id;
        
        // Only remove from page table if it's a valid page
        if page_id != 0 {  // INVALID_PAGE_ID
            bpm.page_table.write().remove(&page_id);
        }
    }
    
    Ok(frame_id)
} 