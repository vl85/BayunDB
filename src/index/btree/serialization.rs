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

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_calculate_btree_order() {
        // Test with different key types
        let i32_order = calculate_btree_order::<i32>();
        let i64_order = calculate_btree_order::<i64>();
        let string_order = calculate_btree_order::<String>(); // This will use pointer size
        
        // Check that order scales with key size (larger keys = smaller order)
        assert!(i32_order >= i64_order);
        
        // Check minimum order
        assert!(i32_order >= 2);
        assert!(i64_order >= 2);
        assert!(string_order >= 2);
    }
    
    #[test]
    fn test_serialize_deserialize_leaf_node_i32() {
        // Create a leaf node with i32 keys
        let mut node = BTreeNode::<i32>::new_leaf();
        node.keys = vec![5, 10, 15, 20];
        node.values = vec![1005, 1010, 1015, 1020];
        node.next_leaf = Some(999);
        
        // Create a page for serialization
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 1,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Deserialize the node
        let deserialized = deserialize_node::<i32>(&page).unwrap();
        
        // Verify node structure
        assert_eq!(deserialized.is_leaf, true);
        assert_eq!(deserialized.keys, vec![5, 10, 15, 20]);
        assert_eq!(deserialized.values, vec![1005, 1010, 1015, 1020]);
        assert_eq!(deserialized.next_leaf, Some(999));
        assert!(deserialized.children.is_empty());
    }
    
    #[test]
    fn test_serialize_deserialize_internal_node_i32() {
        // Create an internal node with i32 keys
        let mut node = BTreeNode::<i32>::new_internal();
        node.keys = vec![10, 20, 30];
        node.children = vec![100, 200, 300, 400]; // One more child than keys
        
        // Create a page for serialization
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 2,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Deserialize the node
        let deserialized = deserialize_node::<i32>(&page).unwrap();
        
        // Verify node structure
        assert_eq!(deserialized.is_leaf, false);
        assert_eq!(deserialized.keys, vec![10, 20, 30]);
        assert_eq!(deserialized.children, vec![100, 200, 300, 400]);
        assert!(deserialized.values.is_empty());
        assert_eq!(deserialized.next_leaf, None);
    }
    
    #[test]
    fn test_serialize_deserialize_empty_leaf() {
        // Create an empty leaf node
        let node = BTreeNode::<i32>::new_leaf();
        
        // Create a page for serialization
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 3,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Deserialize the node
        let deserialized = deserialize_node::<i32>(&page).unwrap();
        
        // Verify node structure
        assert_eq!(deserialized.is_leaf, true);
        assert!(deserialized.keys.is_empty());
        assert!(deserialized.values.is_empty());
        assert!(deserialized.children.is_empty());
        assert_eq!(deserialized.next_leaf, None);
    }
    
    #[test]
    fn test_header_values() {
        // Create a leaf node with some keys
        let mut node = BTreeNode::<i32>::new_leaf();
        node.keys = vec![1, 2, 3];
        node.values = vec![101, 102, 103];
        node.next_leaf = Some(42);
        
        // Create a page
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 4,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Directly inspect the header values in the page
        assert_eq!(page.data[0], 1); // is_leaf = true
        assert_eq!(LittleEndian::read_u16(&page.data[1..3]), 3); // key_count = 3
        assert_eq!(LittleEndian::read_u32(&page.data[3..7]), 42); // next_leaf = 42
    }
    
    #[test]
    fn test_string_keys() {
        // Create a leaf node with string keys
        let mut node = BTreeNode::<String>::new_leaf();
        node.keys = vec![
            "apple".to_string(), 
            "banana".to_string(), 
            "cherry".to_string()
        ];
        node.values = vec![1, 2, 3];
        
        // Create a page
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 5,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Deserialize the node
        let deserialized = deserialize_node::<String>(&page).unwrap();
        
        // Verify node structure
        assert_eq!(deserialized.is_leaf, true);
        assert_eq!(deserialized.keys, vec![
            "apple".to_string(), 
            "banana".to_string(), 
            "cherry".to_string()
        ]);
        assert_eq!(deserialized.values, vec![1, 2, 3]);
    }
} 