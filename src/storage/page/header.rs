use byteorder::{ByteOrder, LittleEndian};
use crate::common::types::PageId;
use crate::storage::page::layout::{HEADER_SIZE, PAGE_CONSTANTS};

#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub free_space_offset: u32,
    pub free_space_size: u32,
    pub record_count: u32,
    pub next_page_id: Option<PageId>,
    pub prev_page_id: Option<PageId>,
}

impl Default for PageHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl PageHeader {
    pub fn new() -> Self {
        Self {
            free_space_offset: HEADER_SIZE as u32,
            free_space_size: (PAGE_CONSTANTS.page_size - HEADER_SIZE) as u32,
            record_count: 0,
            next_page_id: None,
            prev_page_id: None,
        }
    }

    // Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        
        LittleEndian::write_u32(&mut bytes[0..4], self.free_space_offset);
        LittleEndian::write_u32(&mut bytes[4..8], self.free_space_size);
        LittleEndian::write_u32(&mut bytes[8..12], self.record_count);
        
        let next_id = self.next_page_id.unwrap_or(u32::MAX);
        LittleEndian::write_u32(&mut bytes[12..16], next_id);
        
        let prev_id = self.prev_page_id.unwrap_or(u32::MAX);
        LittleEndian::write_u32(&mut bytes[16..20], prev_id);
        
        bytes
    }

    // Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let free_space_offset = LittleEndian::read_u32(&bytes[0..4]);
        let free_space_size = LittleEndian::read_u32(&bytes[4..8]);
        let record_count = LittleEndian::read_u32(&bytes[8..12]);
        
        let next_id = LittleEndian::read_u32(&bytes[12..16]);
        let next_page_id = if next_id == u32::MAX { None } else { Some(next_id) };
        
        let prev_id = LittleEndian::read_u32(&bytes[16..20]);
        let prev_page_id = if prev_id == u32::MAX { None } else { Some(prev_id) };
        
        Self {
            free_space_offset,
            free_space_size,
            record_count,
            next_page_id,
            prev_page_id,
        }
    }
} 