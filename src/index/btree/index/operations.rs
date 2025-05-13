use crate::common::types::{PageId, Rid};
use crate::index::btree::error::BTreeError;
use crate::index::btree::node::BTreeNode;
use crate::index::btree::serialization::{serialize_node, deserialize_node};
use super::base::BTreeIndex;

impl<K> BTreeIndex<K>
where 
    K: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
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
    
    /// Find the leaf page containing the given key
    pub fn find_leaf_page(&self, key: &K) -> Result<PageId, BTreeError> {
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
} 