// Table Scan Operator
//
// This module implements a simple table scan operator for query execution.

use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
// use std::collections::HashSet; // Removed unused import
// use std::io::Write; // Ensure this is removed if not used elsewhere

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult, QueryError, DataValue};
use crate::storage::buffer::BufferPoolManager;
// use crate::storage::page::{PageManager, PageError}; // Removed PageManager, PageError might still be used by Page struct methods
use crate::storage::page::{PageManager, PageError}; // Keep PageError if get_record_bytes returns it.
use crate::common::types::{PageId, Rid};
use crate::catalog::{Catalog, Table};
use crate::catalog::column::Column;

// Imported within the module for testing
#[cfg(test)]
use std::iter::Iterator;

// Define a static atomic counter for unique operator IDs within this module or context
static NEXT_OPERATOR_ID: AtomicUsize = AtomicUsize::new(0);

/// A table scan operator that scans all tuples in a table
pub struct TableScanOperator {
    /// Table name to scan
    table_name: String,
    /// Alias for the table
    alias: Option<String>,
    /// Buffer pool manager
    buffer_pool: Arc<BufferPoolManager>,
    /// Catalog to get table metadata
    catalog: Arc<RwLock<Catalog>>,
    
    /// Schema of the table, loaded during init
    table_metadata: Option<Table>,

    /// Current page ID being scanned
    current_page_id: Option<PageId>,
    /// Indicates if scan is complete
    done: bool, 
    /// Initialization status
    initialized: bool,

    /// Schema columns loaded during init
    schema_columns: Vec<Column>,

    /// RIDs of records on the current page
    rids_on_current_page: Vec<Rid>,
    /// Current record index on the current page
    current_record_index: usize,

    /// Operator ID for easier debugging
    operator_id: usize,
}

impl TableScanOperator {
    /// Create a new table scan operator
    pub fn new(table_name: String, alias: Option<String>, buffer_pool: Arc<BufferPoolManager>, catalog: Arc<RwLock<Catalog>>) -> Self {
        let operator_id = NEXT_OPERATOR_ID.fetch_add(1, Ordering::SeqCst);
        TableScanOperator {
            table_name,
            alias,
            buffer_pool,
            catalog,
            table_metadata: None,
            schema_columns: Vec::new(),
            current_page_id: None,
            rids_on_current_page: Vec::new(),
            current_record_index: 0,
            done: false, // Start as not done, init will determine actual state
            initialized: false,
            operator_id,
        }
    }

    /// Helper to get the effective column name (aliased if alias exists)
    fn get_effective_column_name(&self, column_name: &str) -> String {
        // Reverted diagnostic hack, restored original logic:
        if let Some(alias_str) = &self.alias {
            if !alias_str.is_empty() { // Check if alias string is non-empty
                return format!("{}.{}", alias_str, column_name);
            }
        }
        column_name.to_string() // Default to column_name if no alias or alias is empty
    }

    /// Advances to the next page that contains records, updating current_page_id,
    /// rids_on_current_page, and current_record_index.
    /// Sets self.done = true if no such page is found.
    fn advance_to_next_page_with_records(&mut self) -> QueryResult<()> {
        loop {
            let page_arc = match self.current_page_id {
                Some(id) => self.buffer_pool.fetch_page(id).map_err(QueryError::from)?,
                None => {
                    self.done = true;
                    self.rids_on_current_page.clear();
                    return Ok(());
                }
            };

            let page_read_guard = page_arc.try_read()
                .ok_or_else(|| QueryError::StorageError("RwLock read poisoned (advance)".to_string()))?;
            
            let pm = PageManager::new();
            let header = pm.get_header(&page_read_guard); // Deref guard to get &Page

            self.rids_on_current_page.clear();
            for i in 0..header.record_count {
                // Add all potential RIDs. `next()` will handle if a specific RID is deleted.
                self.rids_on_current_page.push(Rid::new(page_read_guard.page_id, i));
            }
            self.current_record_index = 0;

            if !self.rids_on_current_page.is_empty() {
                // Even if RIDs are just potential, if count > 0, this page *might* have records.
                // The actual check for deleted records happens in next() when trying to get_record.
                // For advance_to_next_page, having record_count > 0 is enough to stop.
                self.done = false;
                return Ok(());
            }

            if let Some(next_page_id_val) = header.next_page_id {
                self.current_page_id = Some(next_page_id_val);
            } else {
                self.done = true;
                self.current_page_id = None;
                self.rids_on_current_page.clear();
                return Ok(());
            }
        }
    }
}

impl Operator for TableScanOperator {
    /// Initialize the operator
    fn init(&mut self) -> QueryResult<()> {
        if self.initialized && !self.done { 
            return Ok(());
        }
        if self.initialized && self.done { 
            self.initialized = false; 
        }

        let table_meta_ref = { 
            let catalog_guard = self.catalog.read()
                .map_err(|e| QueryError::CatalogError(format!("Failed to acquire catalog read lock (init): {}", e)))?;
            catalog_guard.get_table(&self.table_name)
                .ok_or_else(|| QueryError::TableNotFound(self.table_name.clone()))?
                .clone() 
        };
        self.table_metadata = Some(table_meta_ref); 
        
        self.schema_columns = self.table_metadata.as_ref().unwrap().columns().to_vec();
        self.current_page_id = self.table_metadata.as_ref().unwrap().first_page_id();
        self.rids_on_current_page.clear();
        self.current_record_index = 0;
        self.done = false; 

        if self.current_page_id.is_some() {
            self.advance_to_next_page_with_records()?;
            // self.done is now correctly set by advance_to_next_page_with_records
        } else {
            self.done = true;
        }
        self.initialized = true;
        Ok(())
    }
    
    /// Get the next row
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if self.done { return Ok(None); }
        if !self.initialized { self.init()?; }

        loop { 
            let pm_loop_scope = PageManager::new(); // For use within this loop iteration

            if self.current_record_index >= self.rids_on_current_page.len() {
                if let Some(current_pid) = self.current_page_id {
                    let page_arc_next_pid = self.buffer_pool.fetch_page(current_pid) // Renamed to avoid conflict
                        .map_err(QueryError::from)?;
                    let page_read_guard_next_pid = page_arc_next_pid.try_read() // Renamed
                        .ok_or_else(|| QueryError::StorageError("RwLock read poisoned (next - get next PID)".to_string()))?;
                    
                    let pm = PageManager::new();
                    let header = pm.get_header(&page_read_guard_next_pid); // Deref guard
                    self.current_page_id = header.next_page_id;
                    drop(page_read_guard_next_pid); // Drop guard
                    self.buffer_pool.unpin_page(current_pid, false)
                        .map_err(QueryError::from)?;
                } else {
                    self.done = true;
                    return Ok(None);
                }

                self.advance_to_next_page_with_records()?;
                if self.done { 
                    return Ok(None);
                }
            }

            if self.rids_on_current_page.is_empty() {
                 self.done = true; 
                 return Ok(None);
            }

            let rid_to_fetch = self.rids_on_current_page[self.current_record_index];
            self.current_record_index += 1;

            let page_arc_record = self.buffer_pool.fetch_page(rid_to_fetch.page_id).map_err(QueryError::from)?; // Renamed
            let page_read_guard_record = page_arc_record.try_read() // Renamed
                .ok_or_else(|| QueryError::StorageError("RwLock read poisoned (next - get record)".to_string()))?;
            
            let pm = PageManager::new();
            let get_record_result = pm.get_record(&page_read_guard_record, rid_to_fetch);

            match get_record_result { // Deref guard
                Ok(record_data_bytes) => {
                    let data_copy = record_data_bytes.clone(); 
                    drop(page_read_guard_record); // Drop guard before unpin
                    self.buffer_pool.unpin_page(rid_to_fetch.page_id, false).map_err(QueryError::from)?;

                    let _catalog_guard = self.catalog.read().map_err(|e| QueryError::CatalogError(format!("Scan.Next: Failed to acquire catalog read lock (deserialize): {}", e)))?;
                    let values: Vec<DataValue> = bincode::deserialize(&record_data_bytes)
                        .map_err(|e| QueryError::ExecutionError(format!("Failed to deserialize record data for RID {:?}: {}", rid_to_fetch, e)))?;

                    let mut output_row = Row::new();
                    if values.len() == self.schema_columns.len() {
                        for (idx, col_def) in self.schema_columns.iter().enumerate() {
                            let key_name = self.get_effective_column_name(col_def.name());
                            output_row.set(key_name, values[idx].clone());
                        }
                    } else {
                        return Err(QueryError::ExecutionError(format!(
                            "Data integrity error: Deserialized record column count ({}) does not match schema column count ({}) for table '{}', column '{}', RID: {:?}. Record bytes length: {}",
                            values.len(), self.schema_columns.len(), self.table_name, self.schema_columns.first().map(|c| c.name()).unwrap_or("N/A"), rid_to_fetch, record_data_bytes.len()
                        )));
                    }
                    return Ok(Some(output_row));
                }
                Err(PageError::RecordNotFound) => {
                    drop(page_read_guard_record); // Drop guard
                    self.buffer_pool.unpin_page(rid_to_fetch.page_id, false).ok();
                    continue; // Skip deleted or non-existent record
                }
                Err(e) => {
                    drop(page_read_guard_record); // Drop guard
                    self.buffer_pool.unpin_page(rid_to_fetch.page_id, false).ok();
                    return Err(QueryError::from(e));
                }
            }
        } 
    }
    
    /// Close the scan and release resources
    fn close(&mut self) -> QueryResult<()> {
        self.initialized = false;
        self.current_page_id = None;
        self.rids_on_current_page.clear();
        self.current_record_index = 0;
        self.done = true;
        Ok(())
    }
}

/// Factory function to create a table scan operator
pub fn create_table_scan(
    table_name: &str, 
    alias: &str, 
    buffer_pool: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    let op = TableScanOperator::new(
        table_name.to_string(), 
        Some(alias.to_string()), 
        buffer_pool,
        catalog
    );
    Ok(Arc::new(Mutex::new(op)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Table};
    // We need to use the *actual* Page, BufferPoolManager, Catalog, PageManager for the operator
    use crate::storage::buffer::BufferPoolManager;
    use crate::catalog::Catalog;
    // PageManager is already in scope via super::* if TableScanOperator uses one from its own module.
    // If it's crate::storage::page::PageManager, then:
    use crate::storage::page::PageManager;

    // If these are made pub through `src/storage/page/mod.rs` using `pub use`
    // Assuming these are now public: e.g., in src/storage/page/mod.rs:
    // pub mod header; pub mod layout;
    // pub use self::header::PageHeader;
    // pub use self::layout::{HEADER_SIZE, PAGE_CONSTANTS, RECORD_OFFSET_SIZE, RecordLocation};
    use crate::storage::page::layout::{HEADER_SIZE}; // HEADER_SIZE is used, others were unused.
    use tempfile; // For creating temporary DB files in tests

    // Helper function to set up a real BPM and Catalog with specific page data for tests.
    // This function will create actual Page objects with byte-level constructed data.
    fn setup_test_environment_with_pages(
        table_name: &str,
        schema_cols: Vec<Column>,
        first_page_id: Option<PageId>,
        page_definitions: Vec<(PageId, Option<PageId>, Vec<Vec<DataValue>>)>, // Vec<(page_id, next_page_id, records_on_this_page)>
    ) -> (Arc<BufferPoolManager>, Arc<RwLock<Catalog>>) {
        
        let catalog_for_scan_op = Arc::new(RwLock::new(Catalog::new()));
        {
            let cat_w = catalog_for_scan_op.write().unwrap();
            let mut table_obj = Table::new(table_name.to_string(), schema_cols.clone());
            if let Some(fp_id) = first_page_id {
                table_obj.set_first_page_id(fp_id);
            }
            // Table ID will be assigned by catalog.create_table
            cat_w.create_table(table_obj).unwrap();
        }

        let temp_db_file = tempfile::NamedTempFile::new().unwrap();
        let db_path = temp_db_file.path();
        let bpm_for_scan_op = Arc::new(BufferPoolManager::new(10, db_path).unwrap());
        let page_manager_for_setup = PageManager::new(); // Create once for setup

        for (page_id, next_pid, records_data_values) in page_definitions {
            let (page_ptr, actual_new_pid) = bpm_for_scan_op.new_page().unwrap();
            // Ensure BPM allocates pages in the order we expect for test setup simplicity
            assert_eq!(actual_new_pid, page_id, "BPM allocated page ID {} when {} was expected for test setup", actual_new_pid, page_id);

            let mut page_guard = page_ptr.write();
            page_manager_for_setup.init_page(&mut page_guard);

            for record_values in records_data_values {
                // DataValue::serialize_row needs the schema of the row being serialized,
                // which corresponds to the full table schema here.
                let record_bytes = DataValue::serialize_row(&record_values)
                    .expect("Test data serialization failed");
                page_manager_for_setup.insert_record(&mut page_guard, &record_bytes).unwrap();
            }
            
            // Only update header if next_pid is different from what init_page might have set (or if explicitly Some)
            if next_pid.is_some() || page_manager_for_setup.get_header(&page_guard).next_page_id != next_pid { 
                let mut header = page_manager_for_setup.get_header(&page_guard);
                header.next_page_id = next_pid;
                // This direct write to page_guard.data assumes HEADER_SIZE is accessible
                // and that PageManager doesn't have a higher-level "set_next_page_id" or "update_header".
                let header_bytes = header.to_bytes();
                page_guard.data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
            }
            drop(page_guard); // Release write guard before unpinning
            bpm_for_scan_op.unpin_page(page_id, true).unwrap();
        }
        (bpm_for_scan_op, catalog_for_scan_op)
    }

    #[test]
    fn test_scan_single_page_multiple_records() {
        let table_name = "test_single";
        let col_id = Column::new("id".to_string(), DataType::Integer, false, false, None);
        let schema_cols = vec![col_id.clone()];

        // Page definitions: (page_id, next_page_id, Vec<records_as_Vec<DataValue>>)
        let page_defs = vec![
            (1, None, vec![vec![DataValue::Integer(10)], vec![DataValue::Integer(20)]]),
        ];

        let (bpm, catalog) = setup_test_environment_with_pages(
            table_name, 
            schema_cols.clone(), 
            Some(1), 
            page_defs
        );
        
        let mut operator = TableScanOperator::new(
            table_name.to_string(),
            Some("".to_string()),
            bpm,
            catalog
        );
        
        operator.init().expect("Operator init failed");

        let row1 = operator.next().expect("Failed to get row1").expect("Row1 should exist");
        assert_eq!(row1.get("id"), Some(&DataValue::Integer(10)));
        
        let row2 = operator.next().expect("Failed to get row2").expect("Row2 should exist");
        assert_eq!(row2.get("id"), Some(&DataValue::Integer(20)));

        assert!(operator.next().expect("Failed to get next after row2").is_none(), "Should be no more rows");
        operator.close().unwrap();
    }

    #[test]
    fn test_scan_advances_over_multiple_pages() {
        let table_name = "test_multi_page";
        let col_id = Column::new("id".to_string(), DataType::Integer, false, false, None);
        let schema_cols = vec![col_id.clone()];

        let page_defs = vec![
            (1, Some(2), vec![vec![DataValue::Integer(11)]]), // Page 1, links to 2, 1 record
            (2, None,    vec![vec![DataValue::Integer(22)]]), // Page 2, end of chain, 1 record
        ];
        
        let (bpm, catalog) = setup_test_environment_with_pages(
            table_name,
            schema_cols.clone(),
            Some(1),
            page_defs
        );
        
        let mut operator = TableScanOperator::new(
            table_name.to_string(),
            Some("".to_string()),
            bpm,
            catalog
        );

        operator.init().expect("Operator init failed");

        let row1 = operator.next().expect("Next failed for r1").expect("Row1 should exist");
        assert_eq!(row1.get("id"), Some(&DataValue::Integer(11)));
        
        let row2 = operator.next().expect("Next failed for r2").expect("Row2 should exist");
        assert_eq!(row2.get("id"), Some(&DataValue::Integer(22)));

        assert!(operator.next().expect("Next failed after r2").is_none(), "Should be no more rows");
        operator.close().unwrap();
    }
    
    #[test]
    fn test_scan_skips_empty_intermediate_page() {
        let table_name = "test_skip_empty";
        let col_val = Column::new("val".to_string(), DataType::Integer, false, false, None); // Changed col name to val
        let schema_cols = vec![col_val.clone()];

        let page_defs = vec![
            (1, Some(2), vec![vec![DataValue::Integer(101)]]),       // Page 1, links to 2, 1 record
            (2, Some(3), vec![]),                                   // Page 2, links to 3, 0 records (empty)
            (3, None,    vec![vec![DataValue::Integer(303)]]),       // Page 3, end of chain, 1 record
        ];

        let (bpm, catalog) = setup_test_environment_with_pages(
            table_name,
            schema_cols.clone(),
            Some(1),
            page_defs
        );
        
        let mut operator = TableScanOperator::new(
            table_name.to_string(),
            Some("".to_string()),
            bpm,
            catalog
        );

        operator.init().expect("Operator init failed");

        let r1 = operator.next().expect("Next failed for r1").expect("Row1 should exist");
        assert_eq!(r1.get("val"), Some(&DataValue::Integer(101))); // Check for "val"
        
        let r3 = operator.next().expect("Next failed for r3").expect("Row3 should exist");
        assert_eq!(r3.get("val"), Some(&DataValue::Integer(303))); // Check for "val"

        assert!(operator.next().expect("Next failed after r3").is_none(), "Should be no more rows");
        operator.close().unwrap();
    }

    #[test]
    fn test_scan_empty_table_no_pages() {
        let table_name = "test_empty_no_pages";
        let col_id = Column::new("id".to_string(), DataType::Integer, false, false, None);
        let schema_cols = vec![col_id.clone()];

        // No page definitions, first_page_id is None
        let (bpm, catalog) = setup_test_environment_with_pages(
            table_name, 
            schema_cols.clone(), 
            None, // Important: first_page_id is None
            vec![] // No pages
        );
        
        let mut operator = TableScanOperator::new(
            table_name.to_string(),
            Some("".to_string()),
            bpm,
            catalog
        );
        
        operator.init().expect("Operator init failed for empty table");
        assert!(operator.next().expect("Next on empty table failed").is_none(), "Should be no rows from empty table");
        operator.close().unwrap();
    }

    #[test]
    fn test_scan_table_with_only_empty_pages() {
        let table_name = "test_all_empty_pages";
        let col_id = Column::new("id".to_string(), DataType::Integer, false, false, None);
        let schema_cols = vec![col_id.clone()];

        let page_defs = vec![
            (1, Some(2), vec![]), // Page 1, empty, links to 2
            (2, None,    vec![]), // Page 2, empty, end of chain
        ];
        
        let (bpm, catalog) = setup_test_environment_with_pages(
            table_name,
            schema_cols.clone(),
            Some(1), // Starts at page 1
            page_defs
        );
        
        let mut operator = TableScanOperator::new(
            table_name.to_string(),
            Some("".to_string()),
            bpm,
            catalog
        );

        operator.init().expect("Operator init failed for table with only empty pages");
        assert!(operator.next().expect("Next on table with only empty pages failed").is_none(), "Should be no rows");
        operator.close().unwrap();
    }

    // New tests for advance_to_next_page_with_records
    #[test]
    fn test_advance_current_page_has_records() {
        let table_name = "test_advance_current_has_data";
        let col = Column::new("data".to_string(), DataType::Integer, false, false, None);
        let schema = vec![col.clone()];
        let page_defs = vec![(1, None, vec![vec![DataValue::Integer(10)]])]; // Page 1 with 1 record
        let (bpm, catalog) = setup_test_environment_with_pages(table_name, schema.clone(), Some(1), page_defs);

        let mut operator = TableScanOperator::new(table_name.to_string(), Some("".to_string()), bpm, catalog.clone());
        // Manually set up minimal state for advance_to_next_page_with_records to be called
        operator.table_metadata = catalog.read().unwrap().get_table(table_name);
        operator.schema_columns = schema;
        operator.current_page_id = Some(1);

        operator.advance_to_next_page_with_records().expect("Advance failed");
        assert!(!operator.done, "Should find records on the current page");
        assert_eq!(operator.current_page_id, Some(1));
        assert!(!operator.rids_on_current_page.is_empty(), "RIDs should be loaded");
        assert_eq!(operator.rids_on_current_page.len(), 1);
        operator.close().unwrap();
    }

    #[test]
    fn test_advance_skips_one_empty_page() {
        let table_name = "test_advance_skip_one";
        let col = Column::new("data".to_string(), DataType::Integer, false, false, None);
        let schema = vec![col.clone()];
        let page_defs = vec![
            (1, Some(2), vec![]),                                 // Page 1, empty
            (2, None,    vec![vec![DataValue::Integer(20)]]),    // Page 2, 1 record
        ];
        let (bpm, catalog) = setup_test_environment_with_pages(table_name, schema.clone(), Some(1), page_defs);

        let mut operator = TableScanOperator::new(table_name.to_string(), Some("".to_string()), bpm, catalog.clone());
        operator.table_metadata = catalog.read().unwrap().get_table(table_name);
        operator.schema_columns = schema;
        operator.current_page_id = Some(1); // Start scan from page 1

        operator.advance_to_next_page_with_records().expect("Advance failed");
        assert!(!operator.done, "Should find records on page 2");
        assert_eq!(operator.current_page_id, Some(2));
        assert!(!operator.rids_on_current_page.is_empty(), "RIDs should be loaded from page 2");
        assert_eq!(operator.rids_on_current_page.len(), 1);
        operator.close().unwrap();
    }

    #[test]
    fn test_advance_skips_multiple_empty_pages() {
        let table_name = "test_advance_skip_multiple";
        let col = Column::new("val".to_string(), DataType::Text, false, false, None);
        let schema = vec![col.clone()];
        let page_defs = vec![
            (1, Some(2), vec![]),                                // Page 1, empty
            (2, Some(3), vec![]),                                // Page 2, empty
            (3, None,    vec![vec![DataValue::Text("data".to_string())]]), // Page 3, 1 record
        ];
        let (bpm, catalog) = setup_test_environment_with_pages(table_name, schema.clone(), Some(1), page_defs);

        let mut operator = TableScanOperator::new(table_name.to_string(), Some("".to_string()), bpm, catalog.clone());
        operator.table_metadata = catalog.read().unwrap().get_table(table_name);
        operator.schema_columns = schema;
        operator.current_page_id = Some(1);

        operator.advance_to_next_page_with_records().expect("Advance failed");
        assert!(!operator.done, "Should find records on page 3");
        assert_eq!(operator.current_page_id, Some(3));
        assert!(!operator.rids_on_current_page.is_empty());
        assert_eq!(operator.rids_on_current_page.len(), 1);
        operator.close().unwrap();
    }

    #[test]
    fn test_advance_reaches_end_of_table_after_empty_pages() {
        let table_name = "test_advance_reaches_end";
        let col = Column::new("id".to_string(), DataType::Integer, false, false, None);
        let schema = vec![col.clone()];
        let page_defs = vec![
            (1, Some(2), vec![]), // Page 1, empty
            (2, None,    vec![]), // Page 2, empty, end of chain
        ];
        let (bpm, catalog) = setup_test_environment_with_pages(table_name, schema.clone(), Some(1), page_defs);

        let mut operator = TableScanOperator::new(table_name.to_string(), Some("".to_string()), bpm, catalog.clone());
        operator.table_metadata = catalog.read().unwrap().get_table(table_name);
        operator.schema_columns = schema;
        operator.current_page_id = Some(1);

        operator.advance_to_next_page_with_records().expect("Advance failed but shouldn't error");
        assert!(operator.done, "Should not find any page with records");
        assert!(operator.current_page_id.is_none(), "Current page ID should be None at end of table");
        assert!(operator.rids_on_current_page.is_empty(), "RIDs should be empty");
        operator.close().unwrap();
    }

    #[test]
    fn test_advance_from_non_existent_start_page() { // e.g. table exists but first_page_id was wrong
        let table_name = "test_advance_bad_start";
        let col = Column::new("c".to_string(), DataType::Integer, false, false, None);
        let schema = vec![col.clone()];
        // Setup with a valid page 1, but we'll try to start scan from page 99 (non-existent)
        let page_defs = vec![(1, None, vec![vec![DataValue::Integer(100)]])]; 
        let (bpm, catalog) = setup_test_environment_with_pages(table_name, schema.clone(), Some(1), page_defs);

        let mut operator = TableScanOperator::new(table_name.to_string(), Some("".to_string()), bpm, catalog.clone());
        operator.table_metadata = catalog.read().unwrap().get_table(table_name);
        operator.schema_columns = schema;
        operator.current_page_id = Some(99); // Non-existent start page ID

        // Expecting an error because page 99 cannot be fetched
        let result = operator.advance_to_next_page_with_records();
        assert!(result.is_err(), "Should error when starting from a non-existent page");
        if let Err(QueryError::StorageError(e)) = result {
            // The exact error message might vary depending on BufferPoolManager implementation
            // when a page is not found (e.g. "PageId 99 not found" or "Cannot fetch invalid page ID")
            // For this test, we are primarily concerned that *an* error related to storage/page access occurs.
            // A more robust check might involve specific error variants if BPM provides them.
            assert!(e.contains("PageId 99 not found") || e.contains("Page not found in buffer pool") || e.contains("Cannot fetch invalid page ID"), "Error message mismatch: {}", e);
        } else {
            panic!("Expected StorageError, got {:?}", result);
        }
        operator.close().unwrap();
    }

    #[test]
    fn test_advance_empty_table_no_first_page() { // Table exists but has no pages
        let table_name = "test_advance_empty_no_pages";
        let col = Column::new("c".to_string(), DataType::Integer, false, false, None);
        let schema = vec![col.clone()];
        // Setup table with no pages, first_page_id = None
        let (bpm, catalog) = setup_test_environment_with_pages(table_name, schema.clone(), None, vec![]); 

        let mut operator = TableScanOperator::new(table_name.to_string(), Some("".to_string()), bpm, catalog.clone());
        operator.table_metadata = catalog.read().unwrap().get_table(table_name);
        operator.schema_columns = schema;
        operator.current_page_id = None; // Explicitly None, as it would be if table.first_page_id() was None

        operator.advance_to_next_page_with_records().expect("Advance should not error on empty table");
        assert!(operator.done, "Should not find any page with records in an empty table");
        assert!(operator.current_page_id.is_none(), "Current page ID should remain None");
        assert!(operator.rids_on_current_page.is_empty(), "RIDs should be empty");
        operator.close().unwrap();
    }
}