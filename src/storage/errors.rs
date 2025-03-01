use std::io;
use sled;

#[derive(Debug)]
pub enum StorageError {
    DeserializeError(&'static str),
    CrcMismatch,
    InvalidData(&'static str),
    IoError(io::Error),
    DbError(sled::Error),
    EntryNotFound,
    OrphanedEntry,
    // This is here just as a safeguard. In reality I 
    // could probably imply that a new block to be added
    InvalidHeight, 
    CorruptDB(&'static str),
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> Self {
        StorageError::IoError(err)
    }
}

impl From<sled::Error> for StorageError {
    fn from(err: sled::Error) -> Self {
        StorageError::DbError(err)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::DeserializeError(msg) => write!(f, "Deserialization error: {}", msg),
            StorageError::CrcMismatch => write!(f, "CRC mismatch for tweaks"),
            StorageError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            StorageError::IoError(e) => write!(f, "IO error: {}", e),
            StorageError::DbError(e) => write!(f, "Database error: {}", e),
            StorageError::EntryNotFound => write!(f, "Not found"),
            StorageError::OrphanedEntry => write!(f, "Entry is marked as orphaned"),
            StorageError::InvalidHeight => write!(f, "Invalid height"),
            StorageError::CorruptDB(msg) => write!(f, "Corrupt database: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}