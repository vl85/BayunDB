use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom};

#[derive(Debug, Clone)]
pub struct LogFileHeader {
    pub magic: u32,
    pub version: u32,
    pub header_size: u32,
    pub first_lsn: u64,
}

impl LogFileHeader {
    pub const MAGIC: u32 = 0x57414C44;
    pub const VERSION: u32 = 1;
    pub const HEADER_SIZE: u32 = 16;

    pub fn new(first_lsn: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            header_size: Self::HEADER_SIZE,
            first_lsn,
        }
    }

    pub fn write_to(&self, file: &mut File) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.magic.to_le_bytes())?;
        file.write_all(&self.version.to_le_bytes())?;
        file.write_all(&self.header_size.to_le_bytes())?;
        file.write_all(&self.first_lsn.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn read_from(file: &mut File) -> io::Result<Self> {
        file.seek(SeekFrom::Start(0))?;
        let mut magic_bytes = [0; 4];
        file.read_exact(&mut magic_bytes)?;
        let magic = u32::from_le_bytes(magic_bytes);
        let mut version_bytes = [0; 4];
        file.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        let mut header_size_bytes = [0; 4];
        file.read_exact(&mut header_size_bytes)?;
        let header_size = u32::from_le_bytes(header_size_bytes);
        let mut first_lsn_bytes = [0; 8];
        file.read_exact(&mut first_lsn_bytes)?;
        let first_lsn = u64::from_le_bytes(first_lsn_bytes);
        Ok(Self {
            magic,
            version,
            header_size,
            first_lsn,
        })
    }

    pub fn validate(&self) -> bool {
        self.magic == Self::MAGIC && self.version == Self::VERSION
    }
} 