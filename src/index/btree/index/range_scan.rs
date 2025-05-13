use crate::common::types::Rid;
use crate::index::btree::error::BTreeError;
use crate::index::btree::node::BTreeNode;
use crate::index::btree::serialization::deserialize_node;
use super::base::BTreeIndex;

impl<K> BTreeIndex<K>
where 
    K: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
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
} 