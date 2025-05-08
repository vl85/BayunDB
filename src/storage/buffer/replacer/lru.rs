use std::collections::VecDeque;
use crate::common::types::FrameId;

/// Simple LRU (Least Recently Used) page replacement policy
pub struct LRUReplacer {
    lru_list: VecDeque<FrameId>,
}

impl LRUReplacer {
    pub fn new(pool_size: usize) -> Self {
        Self {
            lru_list: VecDeque::with_capacity(pool_size),
        }
    }
    
    /// Record that a frame has been accessed
    pub fn record_access(&mut self, frame_id: FrameId) {
        // Remove the frame if it's already in the list
        if let Some(pos) = self.lru_list.iter().position(|&id| id == frame_id) {
            self.lru_list.remove(pos);
        }
        
        // Add it to the front (most recently used)
        self.lru_list.push_front(frame_id);
    }
    
    /// Remove a frame from the replacer
    pub fn remove(&mut self, frame_id: FrameId) {
        if let Some(pos) = self.lru_list.iter().position(|&id| id == frame_id) {
            self.lru_list.remove(pos);
        }
    }
    
    /// Victim selection: get the least recently used frame
    pub fn victim(&mut self) -> Option<FrameId> {
        self.lru_list.pop_back()
    }
} 