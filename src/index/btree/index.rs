use std::sync::Arc;
use std::marker::PhantomData;
use std::cell::Cell;
use anyhow::Result;
use serde::{Serialize, Deserialize};

use crate::common::types::{PageId, Rid};
use crate::storage::buffer::BufferPoolManager;
use crate::index::btree::error::BTreeError;
use crate::index::btree::node::BTreeNode;
use crate::index::btree::serialization::{serialize_node, deserialize_node, calculate_btree_order};

/// B+Tree index implementation
pub struct BTreeIndex<K> {
    buffer_pool: Arc<BufferPoolManager>,
    root_page_id: Cell<PageId>,  // Use Cell for interior mutability
    order: usize,
    _phantom: PhantomData<K>,
}

impl<K> BTreeIndex<K>
where 
    K: Clone + Ord + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    /// Create a new B+Tree index with a buffer pool manager
    pub fn new(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, BTreeError> {
        // Create root page
        let (root_page, root_page_id) = buffer_pool.new_page()?;
        
        // Initialize root as leaf node
        let root_node: BTreeNode<K> = BTreeNode::new_leaf();
        
        // Calculate B+Tree order based on key size
        let order = calculate_btree_order::<K>();
        
        // Serialize root node to page
        {
            let mut page_guard = root_page.write();
            serialize_node(&root_node, &mut page_guard)?;
        }
        
        // Unpin the root page (with dirty flag)
        buffer_pool.unpin_page(root_page_id, true)?;
        
        Ok(Self {
            buffer_pool,
            root_page_id: Cell::new(root_page_id),
            order,
            _phantom: PhantomData,
        })
    }
    
    /// Get the current root page ID
    fn get_root_page_id(&self) -> PageId {
        self.root_page_id.get()
    }
    
    /// Set a new root page ID
    fn set_root_page_id(&self, page_id: PageId) {
        self.root_page_id.set(page_id);
    }
    
    /// Find all record IDs associated with the given key
    pub fn find(&self, key: &K) -> Result<Vec<Rid>, BTreeError> {
        // Start from the root
        let mut current_page_id = self.get_root_page_id();
        
        loop {
            // Fetch the current page
            let page = self.buffer_pool.fetch_page(current_page_id)?;
            
            // Deserialize the page to a node
            let node = {
                let page_guard = page.read();
                deserialize_node::<K>(&page_guard)?
            };
            
            // If this is a leaf node, search for the key
            if node.is_leaf {
                // Try to get the value
                let result = if let Some(value) = node.get_value(key) {
                    vec![value]
                } else {
                    Vec::new()
                };
                
                // Unpin the page
                self.buffer_pool.unpin_page(current_page_id, false)?;
                
                return Ok(result);
            }
            
            // If this is an internal node, find the child to follow
            let child_index = node.find_child_index(key);
            
            // Unpin the current page
            self.buffer_pool.unpin_page(current_page_id, false)?;
            
            // Move to the child node
            current_page_id = node.children[child_index];
        }
    }
    
    /// Insert a key-value pair into the tree
    pub fn insert(&self, key: K, rid: Rid) -> Result<(), BTreeError> {
        // Handle potential root split
        if let Some((left_child_id, middle_key, right_child_id)) = self.insert_internal(self.get_root_page_id(), key, rid)? {
            // Need to create a new root
            let (new_root_page, new_root_id) = self.buffer_pool.new_page()?;
            
            // Create a new internal node as the root
            let mut new_root = BTreeNode::new_internal();
            new_root.keys.push(middle_key);
            new_root.children.push(left_child_id);
            new_root.children.push(right_child_id);
            
            // Serialize the new root
            {
                let mut page_guard = new_root_page.write();
                serialize_node(&new_root, &mut page_guard)?;
            }
            
            // Unpin the new root page
            self.buffer_pool.unpin_page(new_root_id, true)?;
            
            // Update the root page ID
            self.set_root_page_id(new_root_id);
        }
        
        Ok(())
    }
    
    /// Internal recursive insert function
    /// Returns Some((left_page_id, middle_key, right_page_id)) if a split occurred
    fn insert_internal(&self, page_id: PageId, key: K, rid: Rid) -> Result<Option<(PageId, K, PageId)>, BTreeError> {
        // Fetch the page
        let page = self.buffer_pool.fetch_page(page_id)?;
        
        // Deserialize to a node
        let mut node = {
            let page_guard = page.read();
            deserialize_node::<K>(&page_guard)?
        };
        
        // Check if this is a leaf node
        let result = if node.is_leaf {
            // Insert into the leaf
            let needs_split = node.insert_into_leaf(key, rid, self.order);
            
            if needs_split {
                // Split the leaf node
                let (new_node, promotion_key) = node.split_leaf();
                
                // Create a new page for the right half
                let (new_page, new_page_id) = self.buffer_pool.new_page()?;
                
                // Update the leaf chain
                node.next_leaf = Some(new_page_id);
                
                // Serialize the new node to the new page
                {
                    let mut page_guard = new_page.write();
                    serialize_node(&new_node, &mut page_guard)?;
                }
                
                // Unpin the new page
                self.buffer_pool.unpin_page(new_page_id, true)?;
                
                // Return the split information
                Some((page_id, promotion_key, new_page_id))
            } else {
                None
            }
        } else {
            // Find the child to insert into
            let child_index = node.find_child_index(&key);
            let child_page_id = node.children[child_index];
            
            // Recursively insert into the child
            if let Some((_, middle_key, right_child_id)) = self.insert_internal(child_page_id, key, rid)? {
                // Child was split, update this node
                let needs_split = node.insert_into_internal(middle_key, right_child_id, self.order);
                
                if needs_split {
                    // Split this internal node
                    let (new_node, promotion_key, _) = node.split_internal();
                    
                    // Create a new page for the right half
                    let (new_page, new_page_id) = self.buffer_pool.new_page()?;
                    
                    // Serialize the new node to the new page
                    {
                        let mut page_guard = new_page.write();
                        serialize_node(&new_node, &mut page_guard)?;
                    }
                    
                    // Unpin the new page
                    self.buffer_pool.unpin_page(new_page_id, true)?;
                    
                    // Return the split information
                    Some((page_id, promotion_key, new_page_id))
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        // Serialize the updated node back to the page
        {
            let mut page_guard = page.write();
            serialize_node(&node, &mut page_guard)?;
        }
        
        // Unpin the page
        self.buffer_pool.unpin_page(page_id, true)?;
        
        Ok(result)
    }
    
    /// Perform a range scan and return all record IDs in the range [start_key, end_key]
    pub fn range_scan(&self, start_key: &K, end_key: &K) -> Result<Vec<Rid>, BTreeError> {
        // Validate range
        if start_key > end_key {
            return Ok(Vec::new());
        }
        
        let mut result = Vec::new();
        
        // Find the leaf containing the start key
        let mut current_page_id = self.find_leaf_page(start_key)?;
        
        loop {
            // Fetch the current page
            let page = self.buffer_pool.fetch_page(current_page_id)?;
            
            // Deserialize the page to a node
            let node: BTreeNode<K> = {
                let page_guard = page.read();
                deserialize_node(&page_guard)?
            };
            
            // Ensure this is a leaf node
            if !node.is_leaf {
                self.buffer_pool.unpin_page(current_page_id, false)?;
                return Err(BTreeError::InvalidPageFormat);
            }
            
            // Get all values in the range
            let range_values = node.range_values(start_key, end_key);
            result.extend(range_values);
            
            // Check if there's a next leaf to scan
            let next_leaf = node.next_leaf;
            
            // Unpin the current page
            self.buffer_pool.unpin_page(current_page_id, false)?;
            
            // If there's no next leaf or all values were found, break the loop
            if next_leaf.is_none() {
                break;
            }
            
            // Move to the next leaf
            current_page_id = next_leaf.unwrap();
            
            // If the first key in the next leaf is greater than end_key, we're done
            let page = self.buffer_pool.fetch_page(current_page_id)?;
            let next_node: BTreeNode<K> = {
                let page_guard = page.read();
                deserialize_node(&page_guard)?
            };
            
            if !next_node.keys.is_empty() && &next_node.keys[0] > end_key {
                self.buffer_pool.unpin_page(current_page_id, false)?;
                break;
            }
            
            // Unpin the page (we'll fetch it again in the next loop iteration)
            self.buffer_pool.unpin_page(current_page_id, false)?;
        }
        
        Ok(result)
    }
    
    /// Find the leaf page containing the given key
    fn find_leaf_page(&self, key: &K) -> Result<PageId, BTreeError> {
        // Start from the root
        let mut current_page_id = self.get_root_page_id();
        
        loop {
            // Fetch the current page
            let page = self.buffer_pool.fetch_page(current_page_id)?;
            
            // Deserialize the page to a node
            let node: BTreeNode<K> = {
                let page_guard = page.read();
                deserialize_node(&page_guard)?
            };
            
            // If this is a leaf node, we're done
            if node.is_leaf {
                self.buffer_pool.unpin_page(current_page_id, false)?;
                return Ok(current_page_id);
            }
            
            // If this is an internal node, find the child to follow
            let child_index = node.find_child_index(key);
            let next_page_id = node.children[child_index];
            
            // Unpin the current page
            self.buffer_pool.unpin_page(current_page_id, false)?;
            
            // Move to the child node
            current_page_id = next_page_id;
        }
    }
    
    /// Remove a key from the tree
    pub fn remove(&self, key: &K) -> Result<(), BTreeError> {
        // Find the leaf containing the key
        let leaf_page_id = self.find_leaf_page(key)?;
        
        // Fetch the leaf page
        let page = self.buffer_pool.fetch_page(leaf_page_id)?;
        
        // Deserialize the leaf node
        let mut node: BTreeNode<K> = {
            let page_guard = page.read();
            deserialize_node(&page_guard)?
        };
        
        // Remove the key from the leaf
        let removed = node.remove_from_leaf(key);
        
        // Serialize the updated node back to the page
        {
            let mut page_guard = page.write();
            serialize_node(&node, &mut page_guard)?;
        }
        
        // Unpin the page
        self.buffer_pool.unpin_page(leaf_page_id, true)?;
        
        // Check for underflow conditions - for simplicity, we'll skip this for now
        // In a complete implementation, we would handle merging nodes and rebalancing
        
        if !removed {
            return Err(BTreeError::KeyNotFound);
        }
        
        Ok(())
    }
    
    /// Count the number of key-value pairs in the tree
    pub fn count(&self) -> Result<usize, BTreeError> {
        self.count_internal(self.get_root_page_id())
    }
    
    /// Count the number of key-value pairs in the subtree rooted at page_id
    fn count_internal(&self, page_id: PageId) -> Result<usize, BTreeError> {
        // Fetch the page
        let page = self.buffer_pool.fetch_page(page_id)?;
        
        // Deserialize the page to a node
        let node: BTreeNode<K> = {
            let page_guard = page.read();
            deserialize_node(&page_guard)?
        };
        
        let count = if node.is_leaf {
            // Count keys in the leaf
            node.count()
        } else {
            // Recursively count in children
            let mut sum = 0;
            for &child_id in &node.children {
                sum += self.count_internal(child_id)?;
            }
            sum
        };
        
        // Unpin the page
        self.buffer_pool.unpin_page(page_id, false)?;
        
        Ok(count)
    }
} 