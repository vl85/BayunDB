#[cfg(test)]
mod tests {
    use crate::common::types::{Page, Rid, PAGE_SIZE};
    use crate::index::btree::node::BTreeNode;
    use super::super::{serialize_node, deserialize_node, calculate_btree_order};
    
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
        // Use Rid::new for values, assuming page_id 0 for test purposes
        node.values = vec![
            Rid::new(0, 1005),
            Rid::new(0, 1010),
            Rid::new(0, 1015),
            Rid::new(0, 1020),
        ];
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
        assert!(deserialized.is_leaf);
        assert_eq!(deserialized.keys, vec![5, 10, 15, 20]);
        assert_eq!(deserialized.values, vec![
            Rid::new(0, 1005),
            Rid::new(0, 1010),
            Rid::new(0, 1015),
            Rid::new(0, 1020),
        ]);
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
        assert!(!deserialized.is_leaf);
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
        assert!(deserialized.is_leaf);
        assert!(deserialized.keys.is_empty());
        assert!(deserialized.values.is_empty());
        assert!(deserialized.children.is_empty());
        assert_eq!(deserialized.next_leaf, None);
    }
    
    #[test]
    fn test_header_values() {
        // Create a leaf node
        let mut node = BTreeNode::<i32>::new_leaf();
        node.keys = vec![1, 2];
        node.values = vec![Rid::new(0, 1001), Rid::new(0, 1002)];
        node.next_leaf = Some(123);

        // Create a page
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 4,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Deserialize the node
        let deserialized_node = deserialize_node::<i32>(&page).unwrap();
        
        // Check header values
        assert!(deserialized_node.is_leaf);
        assert_eq!(deserialized_node.keys.len(), 2);
        assert_eq!(deserialized_node.values, vec![Rid::new(0, 1001), Rid::new(0, 1002)]);
        assert_eq!(deserialized_node.next_leaf, Some(123));
    }
    
    #[test]
    fn test_string_keys() {
        // Create a leaf node with String keys
        let mut node = BTreeNode::<String>::new_leaf();
        node.keys = vec!["apple".to_string(), "banana".to_string()];
        node.values = vec![Rid::new(0, 2001), Rid::new(0, 2002)];
        node.next_leaf = Some(789);

        // Create a page
        let mut page = Page {
            data: [0; PAGE_SIZE],
            page_id: 5,
            lsn: 0,
        };
        
        // Serialize the node
        serialize_node(&node, &mut page).unwrap();
        
        // Deserialize the node
        let deserialized_node = deserialize_node::<String>(&page).unwrap();
        
        // Check node structure
        assert!(deserialized_node.is_leaf);
        assert_eq!(deserialized_node.keys, vec!["apple".to_string(), "banana".to_string()]);
        assert_eq!(deserialized_node.values, vec![Rid::new(0, 2001), Rid::new(0, 2002)]);
        assert_eq!(deserialized_node.next_leaf, Some(789));
    }
} 