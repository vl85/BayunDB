use std::mem;
use crate::common::types::{Rid, PAGE_SIZE};
use super::encoding::HEADER_SIZE;

/// Calculate the order of the B+Tree based on key size
pub fn calculate_btree_order<K>() -> usize {
    // Rough estimate based on key size and overhead
    let key_size = mem::size_of::<K>();
    let value_size = mem::size_of::<Rid>();
    
    // Formula: (PAGE_SIZE - HEADER_SIZE) / (key_size + value_size)
    // This is a simplification - in reality, we'd need to account for serialization overhead
    let order = (PAGE_SIZE - HEADER_SIZE) / (key_size + value_size);
    
    // Ensure a minimum order of 2 (to maintain B+Tree properties)
    order.max(2)
} 