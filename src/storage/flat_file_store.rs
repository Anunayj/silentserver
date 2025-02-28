use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{BlockData, Index, IndexEntry, StorageError};

pub const BLOCK_DATA_DIR_NAME: &str = "block_data";
pub const INDEX_DIR_NAME: &str = "index_db";

const MAGIC_BYTES: [u8; 8] = *b"SPSDATA1";
const MAX_BLOCKDATA_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

macro_rules! block_file_name {
    ($file_number:expr) => {
        format!("sps{:06}.dat", $file_number)
    };
}

/// FlatFileStore manages appending BlockData records into files.
/// It creates a new file (with a magic header) when MAX_BLOCKDATA_SIZE is reached.
/// It also persists:
///  - blockhash -> IndexEntry (in the default sled tree)
///  - height (u32) -> blockhash (in "height_to_hash" tree)
///  - blockhash -> height (in "hash_to_height" tree)

pub struct FlatFileStore {
    block_data_dir: PathBuf,
    index_dir: PathBuf,
    index: Index,
    current_file_number: u64,
}

impl FlatFileStore {
    pub fn initialize(data_dir: &str) -> Result<Self, StorageError> {
        let block_data_dir = PathBuf::from(data_dir).join(BLOCK_DATA_DIR_NAME);
        // files are named in format sps00000.dat, sps00001.dat, etc.
        let mut current_file_number = 0;
        while Path::new(&block_data_dir.join(&block_file_name!(current_file_number))).exists() {
            current_file_number += 1;
        }

        if current_file_number == 0 {
            fs::create_dir_all(&block_data_dir)?;
            // create sps00000.dat file
            let mut file = File::create(&block_data_dir.join(&block_file_name!(0)))?;
            file.write_all(&MAGIC_BYTES)?;
        }

        let index_dir = PathBuf::from(data_dir).join(INDEX_DIR_NAME);
        let (index, is_created) = Index::initialize(&index_dir)?;
        if is_created && current_file_number > 0 {
            // this should rebuild the index, but that's a problem for future me.
            panic!("Block data directory already exists but index is newly created");
        }

        Ok(Self {
            block_data_dir,
            index_dir,
            index,
            current_file_number,
        })
    }

    fn get_current_file_path(&self) -> PathBuf {
        self.block_data_dir
            .join(&block_file_name!(self.current_file_number))
    }

    fn get_current_file_size(&self) -> Result<u64, StorageError> {
        let file_path = self.get_current_file_path();
        let metadata = fs::metadata(file_path)?;
        Ok(metadata.len())
    }

    fn create_new_file(&mut self) -> Result<(), StorageError> {
        self.current_file_number += 1;
        let mut file = File::create(&self.get_current_file_path())?;
        file.write_all(&MAGIC_BYTES)?;
        Ok(())
    }
    /// Adds a block data record to the end of the current file.
    /// If the file will be full after the addition, it creates a new file and updates the index.
    fn add_block(&mut self, block_data: &BlockData, height: u32) -> Result<(), StorageError> {
        let file_path = self.get_current_file_path();
        let mut file = File::options().append(true).open(&file_path)?;
        // Get current position for index
        let offset = file.seek(SeekFrom::End(0))?;

        let serialized = block_data.serialize();
        if offset + serialized.len() as u64 >= MAX_BLOCKDATA_SIZE {
            self.create_new_file()?;
            file = File::options().append(true).open(&file_path)?;
        }

        // This should be one "Atomic" Operation
        // Failure should revert file changes.
        // TODO: Fix this.
        {
            file.write_all(&serialized)?;

            let entry = IndexEntry {
                file_number: self.current_file_number,
                offset,
                length: serialized.len() as u64,
            };

            // Panic if this fails, for now.
            self.index
                .insert_block(height, &block_data.blockhash, &entry)
                .expect("Failed to insert block into index");
        }

        Ok(())
    }

    fn add_block_bulk(&mut self, blocks: &[BlockData], heights: &[u32]) -> Result<(), StorageError> {
        for (block, height) in blocks.iter().zip(heights.iter()) {
            self.add_block(block, *height)?;
        }
        Ok(())
    }
    
    /// This is a uninturrpted Buffered Stream of data that can be served to the client
    /// It automatically moves to a new file (skips over magic bytes) when the end of current
    /// file is reached. 
    fn get_block_stream_from_offset(&self, entry: &IndexEntry) -> Result<(), StorageError> {
        todo!();
    }

    fn get_block_stream(&self, blockhash: &[u8; 32]) -> Result<(), StorageError> {
        let entry = self.index.get_block_entry(blockhash)?;
        self.get_block_stream_from_offset(&entry)
    }

    /// Just for testing.
    fn get_block_stream_from_height(&self, height: u32) -> Result<(), StorageError> {
        let blockhash = self.index.get_blockhash_by_height(height)?;
        self.get_block_stream(&blockhash)
    }

    fn get_block_stream_from_genesis(&self) -> Result<(), StorageError> {
        self.get_block_stream_from_height(0)
    }
}
