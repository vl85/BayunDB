use std::path::{Path, PathBuf};
use crate::transaction::wal::log_components::log_manager_core::LogManagerConfig;
use super::log_file_error::{LogFileError, Result};

pub fn find_log_files(config: &LogManagerConfig) -> Result<Vec<(u32, PathBuf)>> {
    let mut log_files = Vec::new();
    if !config.log_dir.exists() {
        std::fs::create_dir_all(&config.log_dir)?;
        return Ok(log_files);
    }
    for entry in std::fs::read_dir(&config.log_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Ok(sequence) = extract_sequence_from_path(config, &path) {
            log_files.push((sequence, path));
        }
    }
    Ok(log_files)
}

pub fn extract_sequence_from_path(config: &LogManagerConfig, path: &Path) -> Result<u32> {
    let file_name = path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| LogFileError::InvalidState("Invalid file name".to_string()))?;
    let prefix = format!("{}_", config.log_file_base_name);
    if !file_name.starts_with(&prefix) || !file_name.ends_with(".log") {
        return Err(LogFileError::InvalidState("Invalid log file name".to_string()));
    }
    let sequence_str = &file_name[prefix.len()..file_name.len() - 4];
    let sequence = sequence_str.parse::<u32>()
        .map_err(|_| LogFileError::InvalidState(format!("Invalid sequence number: {}", sequence_str)))?;
    Ok(sequence)
} 