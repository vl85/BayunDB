use byteorder::{ByteOrder, LittleEndian};
use crate::common::types::PAGE_SIZE;

pub const HEADER_SIZE: usize = 20; // 4 bytes per field * 5 fields
pub const RECORD_OFFSET_SIZE: usize = 8; // 4 bytes for offset + 4 bytes for length

#[allow(dead_code)] // Allow dead code for fields that might be used in future or more detailed page impl
#[derive(Debug, Clone, Copy)]
pub struct PageConstants {
    pub page_size: usize,
    pub header_size: usize,
    pub record_offset_size: usize,
    pub slot_count_offset: usize,
    pub free_space_pointer_offset: usize,
    pub next_page_id_offset: usize,
    pub lsn_offset: usize, 
}

pub const PAGE_CONSTANTS: PageConstants = PageConstants {
    page_size: PAGE_SIZE,
    header_size: HEADER_SIZE,
    record_offset_size: RECORD_OFFSET_SIZE,
    slot_count_offset: 0,
    free_space_pointer_offset: 0,
    next_page_id_offset: 0,
    lsn_offset: 0,
};

#[derive(Debug, Clone, Copy)]
pub struct RecordLocation {
    pub offset: u32,
    pub length: u32,
}

impl RecordLocation {
    pub fn to_bytes(&self) -> [u8; RECORD_OFFSET_SIZE] {
        let mut bytes = [0u8; RECORD_OFFSET_SIZE];
        LittleEndian::write_u32(&mut bytes[0..4], self.offset);
        LittleEndian::write_u32(&mut bytes[4..8], self.length);
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let offset = LittleEndian::read_u32(&bytes[0..4]);
        let length = LittleEndian::read_u32(&bytes[4..8]);
        Self { offset, length }
    }
} 