use crate::common::types::PageId;
use crate::index::btree::error::BTreeError;
use crate::index::btree::node::BTreeNode;
use crate::index::btree::serialization::{serialize_node, deserialize_node};
use super::base::BTreeIndex;

impl<K> BTreeIndex<K>
where 
    K: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
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