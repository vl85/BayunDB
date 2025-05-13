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
    pub(crate) buffer_pool: Arc<BufferPoolManager>,
    pub(crate) root_page_id: Cell<PageId>,  // Use Cell for interior mutability
    pub(crate) order: usize,
    pub(crate) _phantom: PhantomData<K>,
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
    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id.get()
    }
    
    /// Set a new root page ID
    pub fn set_root_page_id(&self, page_id: PageId) {
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
} 