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