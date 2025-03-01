use log::{debug, info, warn};
use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{BlockData, Index, IndexEntry, StorageError};

pub const BLOCK_DATA_DIR_NAME: &str = "block_data";
pub const INDEX_DIR_NAME: &str = "index_db";

const MAGIC_BYTES: [u8; 8] = *b"SPSDATA1";
const MAX_BLOCKDATA_SIZE: u64 = 128 * 1024 * 1024; // 128 MB

macro_rules! block_file_name {
    ($file_number:expr) => {
        format!("sps{:06}.dat", $file_number)
    };
}

// FlatFileStore stores block data in the following format:
// [MAGIC_BYTES][Serialized BlockData]*

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
    pub fn initialize(data_dir: PathBuf) -> Result<Self, StorageError> {
        let block_data_dir = data_dir.join(BLOCK_DATA_DIR_NAME);
        // files are named in format sps00000.dat, sps00001.dat, etc.
        info!(target: "FileStore", "Checking for existing FileStore in: {}", block_data_dir.display());

        let mut current_file_number: u64 = 0;

        let block_data_exists = block_data_dir.join(&block_file_name!(0)).exists();
        if !block_data_exists {
            // ensure no other file of form spsxxxxx.dat exists
            fs::create_dir_all(&block_data_dir)?;
            for file in fs::read_dir(&block_data_dir)? {
                let file = file?;
                if file.file_type()?.is_file() && file.path().to_string_lossy().starts_with("sps") {
                    return Err(StorageError::CorruptDB(
                        "Missing sps00000.dat, but other files present",
                    ));
                }
            }

            info!(target: "FileStore", "Creating initial block data directory and file");
            // create sps00000.dat file
            let mut file = File::create(&block_data_dir.join(&block_file_name!(0)))?;
            file.write_all(&MAGIC_BYTES)?;
        } else {
            // Find the highest numbered file
            while Path::new(&block_data_dir.join(&block_file_name!(current_file_number + 1)))
                .exists()
            {
                current_file_number += 1;
            }
            debug!(target: "FileStore", "Found {} block data files, ", current_file_number);
        }

        let index_dir = data_dir.join(INDEX_DIR_NAME);
        let (index, is_exists) = Index::initialize(&index_dir)?;

        if !is_exists {
            info!(target: "FileStore", "Created new index database at: {}", index_dir.display());
        } else {
            let current_height = index.get_current_height();
            info!(target: "FileStore", "Recovered existing index database from: {} (current height: {})", index_dir.display(), current_height);
        }
        
        if !is_exists && block_data_exists {
            // this should rebuild the index, but that's a problem for future me.
            // TODO: Fix this.
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
        let new_file_path = self.get_current_file_path();
        info!(target: "FileStore", "Creating new block data file: {}", new_file_path.display());
        let mut file = File::create(&new_file_path)?;
        file.write_all(&MAGIC_BYTES)?;
        Ok(())
    }
    /// Adds a block data record to the end of the current file.
    /// If the file will be full after the addition, it creates a new file and updates the index.
    pub fn add_block(&mut self, block_data: &BlockData, height: u32) -> Result<(), StorageError> {
        let file_path = self.get_current_file_path();
        let mut file = File::options().append(true).open(&file_path)?;
        // Get current position for index
        let offset = file.seek(SeekFrom::End(0))?;

        let serialized = block_data.serialize();
        if offset + serialized.len() as u64 >= MAX_BLOCKDATA_SIZE {
            debug!(target: "FileStore", "Current file size limit reached ({} bytes), creating new file", offset);
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

            info!(target: "FileStore", "Adding block at height {} (hash: {:?}) to file {} at offset {}", 
                  height, &block_data.blockhash[..4], self.current_file_number, offset);

            // Panic if this fails, for now.
            self.index
                .insert_block(height, &block_data.blockhash, &entry)
                .expect("Failed to insert block into index");
        }

        Ok(())
    }

    pub fn add_block_bulk(
        &mut self,
        blocks: &[BlockData],
        heights: &[u32],
    ) -> Result<(), StorageError> {
        for (block, height) in blocks.iter().zip(heights.iter()) {
            self.add_block(block, *height)?;
        }
        Ok(())
    }

    /// This is an uninterrupted Buffered Stream of data that can be served to the client
    /// It automatically moves to a new file (skips over magic bytes) when the end of current
    /// file is reached.
    pub fn get_block_stream_from_offset<'a>(
        &'a self,
        entry: &IndexEntry,
    ) -> Result<impl Read + 'a, StorageError> {
        let file_path = self
            .block_data_dir
            .join(&block_file_name!(entry.file_number));
        let file = File::open(&file_path)?;
        let reader = BufReader::new(file);

        // Create a BlockDataReader that will handle reading across file boundaries if needed
        Ok(BlockDataReader {
            store: self,
            current_file_number: entry.file_number,
            reader,
            current_position: entry.offset,
        })
    }

    fn get_block_stream<'a>(
        &'a self,
        blockhash: &[u8; 32],
    ) -> Result<impl Read + 'a, StorageError> {
        let entry = self.index.get_block_entry(blockhash)?;
        self.get_block_stream_from_offset(&entry)
    }

    /// Just for testing.
    fn get_block_stream_from_height<'a>(
        &'a self,
        height: u32,
    ) -> Result<impl Read + 'a, StorageError> {
        let blockhash = self.index.get_blockhash_by_height(height)?;
        self.get_block_stream(&blockhash)
    }

    fn get_block_stream_from_genesis<'a>(&'a self) -> Result<impl Read + 'a, StorageError> {
        self.get_block_stream_from_height(0)
    }
}

/// A reader that reads block data from flat files, automatically handling file boundaries
struct BlockDataReader<'a> {
    store: &'a FlatFileStore,
    current_file_number: u64,
    reader: BufReader<File>,
    current_position: u64,
}

impl<'a> BlockDataReader<'a> {
    /// Opens the next file and positions the reader at the start of the data (after magic bytes)
    fn move_to_next_file(&mut self) -> Result<(), StorageError> {
        self.current_file_number += 1;
        let file_path = self
            .store
            .block_data_dir
            .join(&block_file_name!(self.current_file_number));

        // Check if the next file exists
        if !file_path.exists() {
            return Err(StorageError::InvalidData("Next block file does not exist"));
        }

        debug!(target: "FileStore", "Moving to next block file: {}", file_path.display());
        let file = File::open(&file_path)?;
        self.reader = BufReader::new(file);

        // Skip the magic bytes at the beginning of the file
        self.reader
            .seek(SeekFrom::Start(MAGIC_BYTES.len() as u64))?;
        self.current_position = MAGIC_BYTES.len() as u64;

        Ok(())
    }
}

impl<'a> Read for BlockDataReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Position the reader at the current position if needed
        let current_pos = self.reader.stream_position()?;
        if current_pos != self.current_position {
            self.reader.seek(SeekFrom::Start(self.current_position))?;
        }

        let mut bytes_read = self.reader.read(buf)?;

        // Update position
        self.current_position += bytes_read as u64;

        // If we've reached the end of the file, try moving to the next file
        if bytes_read == 0 {
            match self.move_to_next_file() {
                Ok(_) => {
                    // Try reading from the new file
                    let additional = self.read(&mut buf[bytes_read..])?;
                    bytes_read += additional;
                }
                Err(_) => {
                    // End of all files reached, return 0 bytes read
                    return Ok(0);
                }
            }
        }

        Ok(bytes_read)
    }
}

#[cfg(test)]
mod tests {
    use super::super::block_data::TWEAK_SIZE;
    use super::*;
    use rand::{thread_rng, Rng};
    use std::env;
    use std::fs;
    use std::io::Read;

    fn temp_dir(name: &str) -> PathBuf {
        let mut dir = env::temp_dir();
        dir.push(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn create_random_block_data() -> BlockData {
        let mut rng = thread_rng();
        let mut blockhash = [0u8; 32];
        for i in 0..32 {
            blockhash[i] = rng.gen();
        }

        let num_tweaks = rng.gen_range(1..10);
        let mut tweaks = Vec::with_capacity(num_tweaks);

        for _ in 0..num_tweaks {
            let mut tweak = [0u8; TWEAK_SIZE];
            for i in 0..TWEAK_SIZE {
                tweak[i] = rng.gen();
            }
            tweaks.push(tweak);
        }

        BlockData { blockhash, tweaks }
    }

    #[test]
    fn test_add_and_read_single_block() {
        let test_dir = temp_dir("test_flat_file_store_single");

        // Initialize store
        let mut store = FlatFileStore::initialize(test_dir.clone()).unwrap();

        // Create and add a block
        let block = create_random_block_data();
        let height = 0;
        store.add_block(&block, height).unwrap();

        // Read the block back
        let mut reader = store.get_block_stream_from_height(height).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        // Deserialize and verify
        let read_block = BlockData::deserialize(&buffer).unwrap();
        assert_eq!(block.blockhash, read_block.blockhash);
        assert_eq!(block.tweaks, read_block.tweaks);

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_add_and_read_multiple_blocks() {
        let test_dir = temp_dir("test_flat_file_store_multiple");

        let mut store = FlatFileStore::initialize(test_dir.clone()).unwrap();

        // Create and add multiple blocks
        let num_blocks = 10;
        let mut blocks = Vec::with_capacity(num_blocks);
        let mut heights = Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let block = create_random_block_data();
            blocks.push(block);
            heights.push(i as u32);
        }

        store.add_block_bulk(&blocks, &heights).unwrap();

        // Read and verify each block
        for (i, original_block) in blocks.iter().enumerate() {
            let height = i as u32;
            let mut reader = store.get_block_stream_from_height(height).unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).unwrap();

            let read_block = BlockData::deserialize(&buffer).unwrap();
            assert_eq!(original_block.blockhash, read_block.blockhash);
            assert_eq!(original_block.tweaks, read_block.tweaks);
        }

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_cross_file_boundary() {
        let test_dir = temp_dir("test_flat_file_store_boundary");

        // Initialize store
        let mut store = FlatFileStore::initialize(test_dir.clone()).unwrap();

        // Create a large block with many tweaks to make it bigger
        let mut large_block = create_random_block_data();
        let mut rng = thread_rng();
        for _ in 0..100 {
            let mut tweak = [0u8; TWEAK_SIZE];
            for i in 0..TWEAK_SIZE {
                tweak[i] = rng.gen();
            }
            large_block.tweaks.push(tweak);
        }

        // Add the block 10,000 times
        for height in 0..10_000 {
            store.add_block(&large_block, height).unwrap();
        }

        // Test reading beyond the end of a file
        let mut reader = store.get_block_stream_from_height(0).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        // The buffer should contain all blocks concatenated
        let mut pos = 0;
        while pos < buffer.len() {
            let block = BlockData::deserialize(&buffer[pos..]).unwrap();
            assert_eq!(large_block.blockhash, block.blockhash);
            assert_eq!(large_block.tweaks.len(), block.tweaks.len());
            pos += block.serialize().len();
        }

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }
}
