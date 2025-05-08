use std::mem;
use std::any::TypeId;
use byteorder::{ByteOrder, LittleEndian};
use serde::{Serialize, Deserialize};
use anyhow::Result;

use crate::common::types::{Page, Rid, PAGE_SIZE};
use crate::index::btree::node::BTreeNode;
use crate::index::btree::error::BTreeError;

// Header layout:
// - is_leaf: u8 (1 byte)
// - key_count: u16 (2 bytes)
// - next_leaf: u32 (4 bytes, 0 if None)
const HEADER_SIZE: usize = 7;

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
        // Write values (record IDs)
        for value in &node.values {
            if offset + 4 > PAGE_SIZE {
                return Err(BTreeError::NodeTooLarge);
            }
            
            LittleEndian::write_u32(&mut page.data[offset..offset+4], *value);
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
        // Read values (record IDs)
        for _ in 0..key_count {
            let value = LittleEndian::read_u32(&page.data[offset..offset+4]);
            values.push(value);
            offset += 4;
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