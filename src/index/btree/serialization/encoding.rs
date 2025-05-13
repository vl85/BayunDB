use std::any::TypeId;
use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;

use crate::common::types::{Page, PAGE_SIZE};
use crate::index::btree::node::BTreeNode;
use crate::index::btree::error::BTreeError;

// Header layout:
// - is_leaf: u8 (1 byte)
// - key_count: u16 (2 bytes)
// - next_leaf: u32 (4 bytes, 0 if None)
pub(crate) const HEADER_SIZE: usize = 7;

/// Serialize a B+Tree node to a page
pub fn serialize_node<K>(node: &BTreeNode<K>, page: &mut Page) -> Result<(), BTreeError>
where
    K: Serialize + Clone + Ord + 'static,
{
    // Clear the page
    page.data.fill(0);

    // Write the header
    page.data[0] = if node.is_leaf { 1 } else { 0 };
    LittleEndian::write_u16(&mut page.data[1..3], node.keys.len() as u16);
    
    let next_leaf_val = node.next_leaf.unwrap_or(0);
    LittleEndian::write_u32(&mut page.data[3..7], next_leaf_val);

    // Assign storage for values offset (will be updated later)
    let values_offset_loc = 7;
    
    // Serialize the keys
    let mut offset = HEADER_SIZE + 2; // Skip header + values_offset

    // Special case for i32 keys to avoid bincode overhead
    if TypeId::of::<K>() == TypeId::of::<i32>() {
        let keys = unsafe { &*(node.keys.as_slice() as *const [K] as *const [i32]) };
        for &key in keys {
            LittleEndian::write_i32(&mut page.data[offset..offset+4], key);
            offset += 4;
        }
    } else {
        // Generic serialization for other types
        for key in &node.keys {
            let key_bytes = bincode::serialize(key)
                .map_err(|_| BTreeError::SerializationError("Failed to serialize key".to_string()))?;
            
            // Make sure we have enough space
            if offset + key_bytes.len() + 2 > PAGE_SIZE {
                return Err(BTreeError::NodeTooLarge);
            }
            
            // Write key length (2 bytes)
            LittleEndian::write_u16(&mut page.data[offset..offset+2], key_bytes.len() as u16);
            offset += 2;
            
            // Write key data
            page.data[offset..offset+key_bytes.len()].copy_from_slice(&key_bytes);
            offset += key_bytes.len();
        }
    }

    // Store the offset where values/children start
    let values_offset = offset;
    LittleEndian::write_u16(&mut page.data[values_offset_loc..values_offset_loc+2], values_offset as u16);
    
    if node.is_leaf {
        // Write values (Rids: page_id and slot_num)
        for value in &node.values { // value is &Rid
            if offset + 8 > PAGE_SIZE { // Each Rid takes 8 bytes (2 * u32)
                return Err(BTreeError::NodeTooLarge);
            }
            
            LittleEndian::write_u32(&mut page.data[offset..offset+4], value.page_id);
            offset += 4;
            LittleEndian::write_u32(&mut page.data[offset..offset+4], value.slot_num);
            offset += 4;
        }
    } else {
        // Write children (page IDs)
        for child in &node.children {
            if offset + 4 > PAGE_SIZE {
                return Err(BTreeError::NodeTooLarge);
            }
            
            LittleEndian::write_u32(&mut page.data[offset..offset+4], *child);
            offset += 4;
        }
    }

    Ok(())
} 