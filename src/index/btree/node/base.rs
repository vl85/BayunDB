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
} 