use std::sync::Arc;
use parking_lot::RwLock;

/// Page size in bytes (8KB)
pub const PAGE_SIZE: usize = 8192;

/// Page ID type
pub type PageId = u32;

/// Transaction ID type
pub type TxnId = u32;

/// Record ID type
pub type Rid = u32;

/// Buffer pool frame ID type
pub type FrameId = u32;

/// LSN (Log Sequence Number) type
pub type Lsn = u64;

/// Page structure
#[derive(Debug, Clone)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
    pub page_id: PageId,
    pub lsn: Lsn,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            data: [0; PAGE_SIZE],
            page_id,
            lsn: 0,
        }
    }
}

/// Smart pointer to a page
pub type PagePtr = Arc<RwLock<Page>>;

/// Buffer pool frame structure
#[derive(Debug)]
pub struct Frame {
    pub page: PagePtr,
    pub frame_id: FrameId,
    pub is_dirty: bool,
    pub pin_count: TxnId,
}

impl Frame {
    pub fn new(frame_id: FrameId, page: PagePtr) -> Self {
        Self {
            page,
            frame_id,
            is_dirty: false,
            pin_count: 0,
        }
    }
}

/// Smart pointer to a frame
pub type FramePtr = Arc<RwLock<Frame>>; 