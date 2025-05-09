use anyhow::Result;

use crate::common::types::{Page, Rid};
use crate::storage::page::header::PageHeader;
use crate::storage::page::error::PageError;
use crate::storage::page::layout::{HEADER_SIZE, RECORD_OFFSET_SIZE, RecordLocation, PAGE_CONSTANTS};

pub struct PageManager {
    // Records location are stored from the end of the page
    // actual record data is stored from the beginning of the page (after the header)
}

impl PageManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_page(&self, page: &mut Page) {
        let header = PageHeader::new();
        let header_bytes = header.to_bytes();
        page.data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    pub fn insert_record(&self, page: &mut Page, data: &[u8]) -> Result<Rid, PageError> {
        let mut header = self.get_header(page);
        
        // Record size plus the slot entry for the record
        let record_size = data.len() as u32;
        let total_space_needed = record_size + RECORD_OFFSET_SIZE as u32;
        
        // Check if there's enough space
        if header.free_space_size < total_space_needed {
            return Err(PageError::InsufficientSpace);
        }

        // Calculate slot location for the record (from the end)
        let slot_array_pos = PAGE_CONSTANTS.page_size - RECORD_OFFSET_SIZE * (header.record_count as usize + 1);
        
        // Create record location
        let record_loc = RecordLocation {
            offset: header.free_space_offset,
            length: record_size,
        };
        
        // Write record data
        let data_end = header.free_space_offset as usize + data.len();
        page.data[header.free_space_offset as usize..data_end].copy_from_slice(data);
        
        // Write slot entry
        let slot_bytes = record_loc.to_bytes();
        page.data[slot_array_pos..slot_array_pos+RECORD_OFFSET_SIZE].copy_from_slice(&slot_bytes);
        
        // Update header
        header.free_space_offset += record_size;
        header.free_space_size -= total_space_needed;
        header.record_count += 1;
        
        // Write updated header
        let header_bytes = header.to_bytes();
        page.data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
        
        // Return the record ID (slot index)
        Ok(header.record_count - 1)
    }

    pub fn delete_record(&self, page: &mut Page, rid: Rid) -> Result<(), PageError> {
        let mut header = self.get_header(page);
        
        if rid >= header.record_count {
            return Err(PageError::InvalidRecordId);
        }
        
        // Mark record as deleted by setting its length to 0
        // This is a simple approach; a more efficient one would involve compaction immediately
        let slot_pos = self.get_slot_position(rid, header.record_count);
        let mut record_loc = self.get_record_location(page, slot_pos);
        
        if record_loc.length == 0 {
            return Err(PageError::RecordNotFound); // Already deleted
        }
        
        // Mark as deleted by setting length to 0
        record_loc.length = 0;
        let slot_bytes = record_loc.to_bytes();
        page.data[slot_pos..slot_pos+RECORD_OFFSET_SIZE].copy_from_slice(&slot_bytes);

        // Update header - we're not recovering the space here, just marking it as deleted
        // Actual space recovery happens during compaction
        header.free_space_size += RECORD_OFFSET_SIZE as u32; // Only account for slot array entry
        
        // Write updated header
        let header_bytes = header.to_bytes();
        page.data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
        
        Ok(())
    }

    pub fn update_record(&self, page: &mut Page, rid: Rid, data: &[u8]) -> Result<(), PageError> {
        let header = self.get_header(page);
        
        if rid >= header.record_count {
            return Err(PageError::InvalidRecordId);
        }
        
        let slot_pos = self.get_slot_position(rid, header.record_count);
        let record_loc = self.get_record_location(page, slot_pos);
        
        if record_loc.length == 0 {
            return Err(PageError::RecordNotFound); // Deleted record
        }
        
        let new_size = data.len() as u32;
        
        // If new data is larger than old data, perform delete + insert
        if new_size > record_loc.length {
            // Instead of using delete + insert, which would change the RID,
            // we'll directly update the record location and write the new data
            
            // First check if we have enough space
            let space_needed = new_size - record_loc.length;
            let mut header = self.get_header(page);
            
            if header.free_space_size < space_needed {
                return Err(PageError::InsufficientSpace);
            }
            
            // Write new data at the end of the current data section
            let new_offset = header.free_space_offset;
            let new_end = new_offset as usize + data.len();
            page.data[new_offset as usize..new_end].copy_from_slice(data);
            
            // Update the record location
            let new_record_loc = RecordLocation {
                offset: new_offset,
                length: new_size,
            };
            
            let slot_bytes = new_record_loc.to_bytes();
            page.data[slot_pos..slot_pos+RECORD_OFFSET_SIZE].copy_from_slice(&slot_bytes);
            
            // Update header
            header.free_space_offset += new_size;
            header.free_space_size -= space_needed;
            
            // Write updated header
            let header_bytes = header.to_bytes();
            page.data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
            
            return Ok(());
        }
        
        // If new data is smaller or equal, just update in place
        page.data[record_loc.offset as usize..(record_loc.offset as usize + data.len())]
            .copy_from_slice(data);
        
        // If smaller, update record location
        if new_size < record_loc.length {
            let new_record_loc = RecordLocation {
                offset: record_loc.offset,
                length: new_size,
            };
            
            let slot_bytes = new_record_loc.to_bytes();
            page.data[slot_pos..slot_pos+RECORD_OFFSET_SIZE].copy_from_slice(&slot_bytes);
        }
        
        Ok(())
    }

    pub fn get_record(&self, page: &Page, rid: Rid) -> Result<Vec<u8>, PageError> {
        let header = self.get_header(page);
        
        if rid >= header.record_count {
            return Err(PageError::InvalidRecordId);
        }
        
        let slot_pos = self.get_slot_position(rid, header.record_count);
        let record_loc = self.get_record_location(page, slot_pos);
        
        if record_loc.length == 0 {
            return Err(PageError::RecordNotFound); // Deleted record
        }
        
        // Read record data
        let start = record_loc.offset as usize;
        let end = start + record_loc.length as usize;
        let mut data = vec![0u8; record_loc.length as usize];
        data.copy_from_slice(&page.data[start..end]);
        
        Ok(data)
    }

    pub fn get_header(&self, page: &Page) -> PageHeader {
        PageHeader::from_bytes(&page.data[0..HEADER_SIZE])
    }

    pub fn get_free_space(&self, page: &Page) -> u32 {
        self.get_header(page).free_space_size
    }

    pub fn compact_page(&self, page: &mut Page) -> Result<(), PageError> {
        let header = self.get_header(page);
        if header.record_count == 0 {
            return Ok(()); // Nothing to compact
        }
        
        // Create a new temporary page for the compacted data
        let mut compacted_data = vec![0u8; PAGE_CONSTANTS.page_size];
        let mut new_offset = HEADER_SIZE as u32;
        let mut new_record_count = 0;
        
        // Copy non-deleted records to the new page
        for rid in 0..header.record_count {
            let slot_pos = self.get_slot_position(rid, header.record_count);
            let record_loc = self.get_record_location(page, slot_pos);
            
            // Skip deleted records
            if record_loc.length == 0 {
                continue;
            }
            
            // Copy record data to new location
            let record_data = &page.data[record_loc.offset as usize
                ..(record_loc.offset as usize + record_loc.length as usize)];
            
            // Write to compacted data
            let new_end = new_offset as usize + record_loc.length as usize;
            compacted_data[new_offset as usize..new_end].copy_from_slice(record_data);
            
            // Create new record location
            let new_record_loc = RecordLocation {
                offset: new_offset,
                length: record_loc.length,
            };
            
            // Calculate new slot position
            let new_slot_pos = PAGE_CONSTANTS.page_size - RECORD_OFFSET_SIZE * (new_record_count as usize + 1);
            let new_slot_bytes = new_record_loc.to_bytes();
            compacted_data[new_slot_pos..new_slot_pos+RECORD_OFFSET_SIZE].copy_from_slice(&new_slot_bytes);
            
            // Update pointers
            new_offset += record_loc.length;
            new_record_count += 1;
        }
        
        // Update header
        let new_free_space_size = (PAGE_CONSTANTS.page_size as u32) - new_offset - (new_record_count * (RECORD_OFFSET_SIZE as u32));
        let new_header = PageHeader {
            free_space_offset: new_offset,
            free_space_size: new_free_space_size,
            record_count: new_record_count,
            next_page_id: header.next_page_id,
            prev_page_id: header.prev_page_id,
        };
        
        // Write new header
        let header_bytes = new_header.to_bytes();
        compacted_data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
        
        // Copy compacted data back to the page
        page.data.copy_from_slice(&compacted_data);
        
        Ok(())
    }
    
    // Helper methods
    
    // Calculate slot position based on RID
    fn get_slot_position(&self, rid: Rid, _record_count: u32) -> usize {
        PAGE_CONSTANTS.page_size - RECORD_OFFSET_SIZE * (rid as usize + 1)
    }
    
    // Get record location from slot
    fn get_record_location(&self, page: &Page, slot_pos: usize) -> RecordLocation {
        RecordLocation::from_bytes(&page.data[slot_pos..slot_pos+RECORD_OFFSET_SIZE])
    }

    /// Get raw page header bytes for WAL logging
    pub fn get_raw_page_header(&self, page: &Page) -> Vec<u8> {
        // Get the page header
        let header = self.get_header(page);
        
        // Serialize the header to bytes
        header.to_bytes().to_vec()
    }

    /// Insert a record at a specific RID (for recovery)
    /// This is primarily used during recovery to restore a record to its original location
    pub fn insert_record_at(&self, page: &mut Page, rid: Rid, data: &[u8]) -> Result<(), PageError> {
        let mut header = self.get_header(page);
        
        // Check if the RID is valid
        if rid >= header.record_count {
            return Err(PageError::InvalidRecordId);
        }
        
        // Get the slot position
        let slot_pos = self.get_slot_position(rid, header.record_count);
        let record_loc = self.get_record_location(page, slot_pos);
        
        // Check if the record already exists
        if record_loc.length > 0 {
            return Err(PageError::DuplicateRecord);
        }
        
        // Record size
        let record_size = data.len() as u32;
        
        // Check if there's enough space
        if header.free_space_size < record_size {
            return Err(PageError::InsufficientSpace);
        }
        
        // Write the record data at the current free space offset
        let data_end = header.free_space_offset as usize + data.len();
        page.data[header.free_space_offset as usize..data_end].copy_from_slice(data);
        
        // Update the record location
        let new_record_loc = RecordLocation {
            offset: header.free_space_offset,
            length: record_size,
        };
        
        let slot_bytes = new_record_loc.to_bytes();
        page.data[slot_pos..slot_pos+RECORD_OFFSET_SIZE].copy_from_slice(&slot_bytes);
        
        // Update header
        header.free_space_offset += record_size;
        header.free_space_size -= record_size;
        
        // Write updated header
        let header_bytes = header.to_bytes();
        page.data[0..HEADER_SIZE].copy_from_slice(&header_bytes);
        
        Ok(())
    }
} 