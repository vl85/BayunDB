use std::any::TypeId;
use byteorder::{ByteOrder, LittleEndian};
use serde::Deserialize;

use crate::common::types::{Page, Rid, PAGE_SIZE};
use crate::index::btree::node::BTreeNode;
use crate::index::btree::error::BTreeError;
use super::encoding::HEADER_SIZE;

/// Deserialize a B+Tree node from a page
pub fn deserialize_node<K>(page: &Page) -> Result<BTreeNode<K>, BTreeError>
where
    K: for<'de> Deserialize<'de> + Clone + Ord + 'static,
{
    // Read header
    let is_leaf = page.data[0] == 1;
    let key_count = LittleEndian::read_u16(&page.data[1..3]) as usize;
    let next_leaf_val = LittleEndian::read_u32(&page.data[3..7]);
    let next_leaf = if next_leaf_val == 0 { None } else { Some(next_leaf_val) };
    
    // Get the offset where values/children start
    let values_offset = LittleEndian::read_u16(&page.data[7..9]) as usize;
    
    // Read keys
    let mut offset = HEADER_SIZE + 2; // Skip header and values_offset
    let mut keys = Vec::with_capacity(key_count);
    
    // Special case for i32 keys to avoid bincode overhead
    if TypeId::of::<K>() == TypeId::of::<i32>() {
        for _ in 0..key_count {
            let key_value = LittleEndian::read_i32(&page.data[offset..offset+4]);
            // Safety: This is safe because we checked the TypeId matches i32
            let key = unsafe { std::mem::transmute_copy::<i32, K>(&key_value) };
            keys.push(key);
            offset += 4;
        }
    } else {
        // Generic deserialization for other types
        for _ in 0..key_count {
            // Read key length
            let key_len = LittleEndian::read_u16(&page.data[offset..offset+2]) as usize;
            offset += 2;
            
            // Read key data
            let key_bytes = &page.data[offset..offset+key_len];
            let key = bincode::deserialize(key_bytes)
                .map_err(|_| BTreeError::DeserializationError("Failed to deserialize key".to_string()))?;
            
            keys.push(key);
            offset += key_len;
        }
    }
    
    // Read values or children
    let mut values = Vec::with_capacity(if is_leaf { key_count } else { 0 });
    let mut children = Vec::with_capacity(if is_leaf { 0 } else { key_count + 1 });
    
    // Reset offset to where values/children start
    offset = values_offset;
    
    if is_leaf {
        // Read values (Rids: page_id and slot_num)
        for _ in 0..key_count {
            if offset + 8 > PAGE_SIZE { // Check for space for two u32s
                return Err(BTreeError::DeserializationError("Not enough data for Rid".to_string()));
            }
            let page_id = LittleEndian::read_u32(&page.data[offset..offset+4]);
            offset += 4;
            let slot_num = LittleEndian::read_u32(&page.data[offset..offset+4]);
            offset += 4;
            values.push(Rid::new(page_id, slot_num));
        }
    } else {
        // Read children (page IDs)
        for _ in 0..key_count + 1 { // Internal nodes have key_count + 1 children
            let child = LittleEndian::read_u32(&page.data[offset..offset+4]);
            children.push(child);
            offset += 4;
        }
    }
    
    Ok(BTreeNode {
        is_leaf,
        keys,
        children,
        values,
        next_leaf,
    })
} 