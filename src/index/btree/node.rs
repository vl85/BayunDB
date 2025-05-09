use crate::common::types::{PageId, Rid};

/// B+Tree node implementation
/// - Leaf nodes store keys and record IDs (values)
/// - Internal nodes store keys and child page IDs
pub struct BTreeNode<K> {
    pub is_leaf: bool,
    pub keys: Vec<K>,
    pub children: Vec<PageId>,  // For internal nodes
    pub values: Vec<Rid>,       // For leaf nodes
    pub next_leaf: Option<PageId>, // For leaf nodes
}

impl<K: Clone + Ord> BTreeNode<K> {
    pub fn new_leaf() -> Self {
        Self {
            is_leaf: true,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        }
    }

    pub fn new_internal() -> Self {
        Self {
            is_leaf: false,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        }
    }
    
    /// Find the position of a key in the node using binary search
    pub fn find_key_index(&self, key: &K) -> Result<usize, usize> {
        self.keys.binary_search(key)
    }
    
    /// Find the index of the child that should contain the key
    pub fn find_child_index(&self, key: &K) -> usize {
        match self.keys.binary_search(key) {
            Ok(i) => i + 1, // If key found, go to the right child
            Err(i) => i,    // If key not found, i is the insertion point
        }
    }
    
    /// Insert a key-value pair into a leaf node
    /// Returns true if the node needs to be split
    pub fn insert_into_leaf(&mut self, key: K, rid: Rid, order: usize) -> bool {
        debug_assert!(self.is_leaf, "insert_into_leaf called on non-leaf node");
        
        // Find position to insert
        let pos = match self.keys.binary_search(&key) {
            Ok(i) => {
                // If key already exists, just update the value
                self.values[i] = rid;
                return false;
            }
            Err(i) => i,
        };
        
        // Insert key and value
        self.keys.insert(pos, key);
        self.values.insert(pos, rid);
        
        // Return whether the node needs to be split
        self.keys.len() > order
    }
    
    /// Insert a key and child pointer into an internal node
    /// Returns true if the node needs to be split
    pub fn insert_into_internal(&mut self, key: K, child: PageId, order: usize) -> bool {
        debug_assert!(!self.is_leaf, "insert_into_internal called on leaf node");
        
        // Find position to insert
        let pos = match self.keys.binary_search(&key) {
            Ok(i) => i, // Key exists, unusual case for internal nodes
            Err(i) => i,
        };
        
        // Insert key and child pointer
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child); // Insert child to the right of the key
        
        // Return whether the node needs to be split
        self.keys.len() > order
    }
    
    /// Split a leaf node into two nodes
    /// Returns the new node and its first key
    pub fn split_leaf(&mut self) -> (Self, K) {
        debug_assert!(self.is_leaf, "split_leaf called on non-leaf node");
        
        // Split at the middle
        let split_point = self.keys.len() / 2;
        
        // Create a new node with the second half of keys and values
        let mut new_node = Self::new_leaf();
        
        // First capture the key we need to return before moving data
        let promotion_key = self.keys[split_point].clone();
        
        // Now move the data
        new_node.keys = self.keys.split_off(split_point);
        new_node.values = self.values.split_off(split_point);
        
        // Set up the leaf node chain
        new_node.next_leaf = self.next_leaf;
        self.next_leaf = None; // We'll set this when we know the page ID
        
        // Return the new node and the promotion key
        (new_node, promotion_key)
    }
    
    /// Split an internal node into two nodes
    /// Returns the new node, the middle key, and the right child
    pub fn split_internal(&mut self) -> (Self, K, PageId) {
        debug_assert!(!self.is_leaf, "split_internal called on leaf node");
        
        // Split at the middle
        let split_point = self.keys.len() / 2;
        
        // Extract the middle key that will be pushed to the parent
        let middle_key = self.keys.remove(split_point);
        
        // Create a new node with the second half of keys and children
        let mut new_node = Self::new_internal();
        new_node.keys = self.keys.split_off(split_point);
        new_node.children = self.children.split_off(split_point + 1);
        
        // Return the new node, middle key, and reference to the new node
        (new_node, middle_key, 0) // PageId will be set later
    }
    
    /// Return the value associated with the key, if present in this leaf node
    pub fn get_value(&self, key: &K) -> Option<Rid> {
        debug_assert!(self.is_leaf, "get_value called on non-leaf node");
        
        match self.keys.binary_search(key) {
            Ok(i) => Some(self.values[i]),
            Err(_) => None,
        }
    }
    
    /// Return all values in the range [start_key, end_key]
    pub fn range_values(&self, start_key: &K, end_key: &K) -> Vec<Rid> {
        debug_assert!(self.is_leaf, "range_values called on non-leaf node");
        
        let mut result = Vec::new();
        
        // Find the position of the start key
        let start_pos = match self.keys.binary_search(start_key) {
            Ok(i) => i,
            Err(i) => i,
        };
        
        // Collect all values within the range
        for i in start_pos..self.keys.len() {
            if &self.keys[i] <= end_key {
                result.push(self.values[i]);
            } else {
                break;
            }
        }
        
        result
    }
    
    /// Remove a key from a leaf node
    /// Returns whether the key was found and removed
    pub fn remove_from_leaf(&mut self, key: &K) -> bool {
        debug_assert!(self.is_leaf, "remove_from_leaf called on non-leaf node");
        
        match self.keys.binary_search(key) {
            Ok(i) => {
                self.keys.remove(i);
                self.values.remove(i);
                true
            }
            Err(_) => false,
        }
    }
    
    /// Remove a key and its associated child from an internal node
    /// Returns whether the key was found and removed
    pub fn remove_from_internal(&mut self, key: &K) -> bool {
        debug_assert!(!self.is_leaf, "remove_from_internal called on leaf node");
        
        match self.keys.binary_search(key) {
            Ok(i) => {
                self.keys.remove(i);
                self.children.remove(i + 1); // Remove the right child
                true
            }
            Err(_) => false,
        }
    }
    
    /// Check if the node has fewer keys than the minimum required
    pub fn is_underflow(&self, order: usize) -> bool {
        self.keys.len() < (order + 1) / 2
    }
    
    /// Count the number of key-value pairs in leaf nodes
    pub fn count(&self) -> usize {
        if self.is_leaf {
            self.keys.len()
        } else {
            0 // For internal nodes, we don't count them
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_creation() {
        let node = BTreeNode::<i32>::new_leaf();
        assert!(node.is_leaf);
        assert!(node.keys.is_empty());
        assert!(node.values.is_empty());
        assert!(node.children.is_empty());
        assert!(node.next_leaf.is_none());
    }

    #[test]
    fn test_internal_node_creation() {
        let node = BTreeNode::<i32>::new_internal();
        assert!(!node.is_leaf);
        assert!(node.keys.is_empty());
        assert!(node.values.is_empty());
        assert!(node.children.is_empty());
        assert!(node.next_leaf.is_none());
    }

    #[test]
    fn test_find_child_index() {
        // Create internal node with keys [10, 20, 30]
        let mut node = BTreeNode::<i32>::new_internal();
        node.keys = vec![10, 20, 30];
        node.children = vec![1, 2, 3, 4]; // Page IDs for children

        // Test cases:
        // Key < all keys: should return leftmost child (index 0)
        assert_eq!(node.find_child_index(&5), 0);
        
        // Key between keys: should return child between those keys
        assert_eq!(node.find_child_index(&15), 1);
        assert_eq!(node.find_child_index(&25), 2);
        
        // Key > all keys: should return rightmost child
        assert_eq!(node.find_child_index(&35), 3);
        
        // Key equal to a key: should return the right child of that key
        assert_eq!(node.find_child_index(&10), 1);
        assert_eq!(node.find_child_index(&20), 2);
        assert_eq!(node.find_child_index(&30), 3);
    }

    #[test]
    fn test_insert_into_leaf() {
        let mut node = BTreeNode::<i32>::new_leaf();
        let order = 3; // Small order for testing

        // First insert
        let needs_split = node.insert_into_leaf(5, 1005, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5]);
        assert_eq!(node.values, vec![1005]);

        // Insert in order
        let needs_split = node.insert_into_leaf(10, 1010, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5, 10]);
        assert_eq!(node.values, vec![1005, 1010]);

        // Insert out of order (middle)
        let needs_split = node.insert_into_leaf(7, 1007, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5, 7, 10]);
        assert_eq!(node.values, vec![1005, 1007, 1010]);

        // Insert that causes split (exceeds order)
        let needs_split = node.insert_into_leaf(12, 1012, order);
        assert!(needs_split);
        assert_eq!(node.keys, vec![5, 7, 10, 12]);
        assert_eq!(node.values, vec![1005, 1007, 1010, 1012]);

        // Update existing key
        let needs_split = node.insert_into_leaf(7, 2007, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5, 7, 10, 12]);
        assert_eq!(node.values, vec![1005, 2007, 1010, 1012]);
    }

    #[test]
    fn test_insert_into_internal() {
        let mut node = BTreeNode::<i32>::new_internal();
        node.children.push(100); // Initial left child
        let order = 3; // Small order for testing

        // First insert
        let needs_split = node.insert_into_internal(5, 105, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5]);
        assert_eq!(node.children, vec![100, 105]);

        // Insert larger key
        let needs_split = node.insert_into_internal(10, 110, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5, 10]);
        assert_eq!(node.children, vec![100, 105, 110]);

        // Insert middle key
        let needs_split = node.insert_into_internal(7, 107, order);
        assert!(!needs_split);
        assert_eq!(node.keys, vec![5, 7, 10]);
        assert_eq!(node.children, vec![100, 105, 107, 110]);

        // Insert that causes split
        let needs_split = node.insert_into_internal(12, 112, order);
        assert!(needs_split);
    }

    #[test]
    fn test_split_leaf() {
        let mut node = BTreeNode::<i32>::new_leaf();
        // Fill with keys in ascending order
        for i in 1..=6 {
            node.keys.push(i);
            node.values.push((1000 + i) as u32);
        }
        node.next_leaf = Some(999); // Set a next leaf pointer

        // Split the leaf
        let (new_right_node, promotion_key) = node.split_leaf();

        // Check original node (left half after split)
        assert_eq!(node.keys, vec![1, 2, 3]);
        assert_eq!(node.values, vec![1001, 1002, 1003]);
        assert_eq!(node.next_leaf, None);

        // Check new node (right half)
        assert_eq!(new_right_node.keys, vec![4, 5, 6]);
        assert_eq!(new_right_node.values, vec![1004, 1005, 1006]);
        assert_eq!(new_right_node.next_leaf, Some(999));

        // Check promotion key (should be the first key of the new node)
        assert_eq!(promotion_key, 4);
    }

    #[test]
    fn test_split_internal() {
        let mut node = BTreeNode::<i32>::new_internal();
        
        // Fill with keys and children
        // Children: [100, 101, 102, 103, 104, 105]
        // Keys: [10, 20, 30, 40, 50]
        node.children.push(100);
        for i in 0..5 {
            node.keys.push(10 * (i + 1));
            node.children.push((101 + i) as u32);
        }

        // Split the internal node
        let (new_right_node, middle_key, _) = node.split_internal();

        // Check the middle key that was removed
        assert_eq!(middle_key, 30);

        // Check original node (left half after split)
        assert_eq!(node.keys, vec![10, 20]);
        assert_eq!(node.children, vec![100, 101, 102]);

        // Check new right node
        assert_eq!(new_right_node.keys, vec![40, 50]);
        assert_eq!(new_right_node.children, vec![103, 104, 105]);
    }

    #[test]
    fn test_get_value() {
        let mut node = BTreeNode::<i32>::new_leaf();
        
        // Add some key-value pairs
        node.keys.push(5);
        node.values.push(1005);
        node.keys.push(10);
        node.values.push(1010);
        
        // Test get existing keys
        assert_eq!(node.get_value(&5), Some(1005));
        assert_eq!(node.get_value(&10), Some(1010));
        
        // Test get non-existent key
        assert_eq!(node.get_value(&7), None);
        assert_eq!(node.get_value(&20), None);
    }

    #[test]
    fn test_range_values() {
        let mut node = BTreeNode::<i32>::new_leaf();
        
        // Add key-value pairs in order
        for i in 1..=10 {
            node.keys.push(i);
            node.values.push((1000 + i) as u32);
        }
        
        // Test different ranges
        
        // Exact match on both ends
        let result = node.range_values(&3, &7);
        assert_eq!(result, vec![1003, 1004, 1005, 1006, 1007]);
        
        // Start key doesn't exist
        let result = node.range_values(&2, &6);
        assert_eq!(result, vec![1002, 1003, 1004, 1005, 1006]);
        
        // End key doesn't exist
        let result = node.range_values(&8, &15);
        assert_eq!(result, vec![1008, 1009, 1010]);
        
        // Range outside node keys
        let result = node.range_values(&20, &30);
        assert!(result.is_empty());
        
        // Range completely before node keys
        let result = node.range_values(&-10, &-5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_remove_from_leaf() {
        let mut node = BTreeNode::<i32>::new_leaf();
        
        // Add some key-value pairs
        node.keys = vec![5, 10, 15, 20];
        node.values = vec![1005, 1010, 1015, 1020];
        
        // Remove existing key
        let removed = node.remove_from_leaf(&10);
        assert!(removed);
        assert_eq!(node.keys, vec![5, 15, 20]);
        assert_eq!(node.values, vec![1005, 1015, 1020]);
        
        // Remove non-existent key
        let removed = node.remove_from_leaf(&12);
        assert!(!removed);
        assert_eq!(node.keys, vec![5, 15, 20]);
        assert_eq!(node.values, vec![1005, 1015, 1020]);
    }

    #[test]
    fn test_remove_from_internal() {
        let mut node = BTreeNode::<i32>::new_internal();
        
        // Set up keys and children
        node.keys = vec![10, 20, 30];
        node.children = vec![100, 110, 120, 130];
        
        // Remove existing key - should remove key and right child
        let removed = node.remove_from_internal(&20);
        assert!(removed);
        assert_eq!(node.keys, vec![10, 30]);
        assert_eq!(node.children, vec![100, 110, 130]); // 120 was removed
        
        // Remove non-existent key
        let removed = node.remove_from_internal(&15);
        assert!(!removed);
        assert_eq!(node.keys, vec![10, 30]);
        assert_eq!(node.children, vec![100, 110, 130]);
    }

    #[test]
    fn test_is_underflow() {
        let mut node = BTreeNode::<i32>::new_leaf();
        
        // For order 5, min keys should be (5+1)/2 = 3
        let order = 5;
        
        // Empty node is in underflow
        assert!(node.is_underflow(order));
        
        // Add 1 key - still in underflow
        node.keys.push(10);
        assert!(node.is_underflow(order));
        
        // Add 2nd key - still in underflow
        node.keys.push(20);
        assert!(node.is_underflow(order));
        
        // Add 3rd key - no longer in underflow
        node.keys.push(30);
        assert!(!node.is_underflow(order));
    }

    #[test]
    fn test_count() {
        // Test leaf node count
        let mut leaf_node = BTreeNode::<i32>::new_leaf();
        leaf_node.keys = vec![1, 2, 3];
        leaf_node.values = vec![101, 102, 103];
        assert_eq!(leaf_node.count(), 3);
        
        // Test internal node count (should be 0)
        let mut internal_node = BTreeNode::<i32>::new_internal();
        internal_node.keys = vec![10, 20, 30];
        internal_node.children = vec![100, 200, 300, 400];
        assert_eq!(internal_node.count(), 0);
    }
} 