use std::path::PathBuf;

use sled::Db;

use super::StorageError;

// TODO: Benchmark this with a HashMap Implementation
// I have a inkling the BTree used by sled is going to be a perform better than a HashMap based implementation.

/// IndexEntry represents the file number, offset, and length of a block
/// (number of outputs) in the flat file store.
#[derive(Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub file_number: u64,
    pub offset: u64,
    pub length: u64,
}

impl IndexEntry {
    /// IndexEntry is serialized as 24 bytes:
    /// [file_number (8 bytes)] [offset (8 bytes)] [length (8 bytes)]
    pub fn serialize(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.file_number.to_le_bytes());
        buf[8..16].copy_from_slice(&self.offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.length.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<IndexEntry> {
        if data.len() != 24 {
            return None;
        }

        Some(IndexEntry {
            file_number: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            length: u64::from_le_bytes(data[16..24].try_into().unwrap()),
        })
    }
}

pub struct Index {
    /// Maps blockhash -> IndexEntry
    index_db: Db,

    /// Maps height <-> blockhash
    // This HAS to be stupid, but idc.
    // Height -> Hash is probably sensible,
    // and can be stored very simply.
    // TODO: Fix this shit.
    height_to_hash: sled::Tree,
    hash_to_height: sled::Tree,
    next_height: u32,
}

impl Index {
    /// Returns (Index, bool) where the bool indicates if the database was newly created (true) or already existed (false)
    pub fn initialize(db_path: &PathBuf) -> Result<(Self, bool), StorageError> {
        let index_db = sled::open(db_path)?;
        let height_to_hash = index_db.open_tree("height_to_hash")?;
        let hash_to_height = index_db.open_tree("hash_to_height")?;

        // was_recovered() returns true if the database was recovered from a previous instance
        let is_new = !index_db.was_recovered();

        let next_height = if is_new {
            0
        } else {
            let data = height_to_hash.last()?;

            if let Some((height, _)) = data {
                let height_bytes: [u8; 4] = height
                    .as_ref()
                    .try_into()
                    .expect("IndexDb corrupted, height is not 4 bytes");
                u32::from_le_bytes(height_bytes) + 1
            } else {
                0
            }
        };

        Ok((
            Index {
                index_db,
                height_to_hash,
                hash_to_height,
                next_height,
            },
            is_new,
        ))
    }

    pub fn insert_block(
        &mut self,
        height: u32,
        blockhash: &[u8; 32],
        entry: &IndexEntry,
    ) -> Result<(), StorageError> {
        if height != self.next_height {
            return Err(StorageError::InvalidHeight);
        }
        // TODO: Make this "atomic".
        {
            self.height_to_hash
                .insert(&height.to_le_bytes(), blockhash)?;
            self.next_height += 1;

            // these panic on failure for now.
            self.hash_to_height
                .insert(blockhash, &height.to_le_bytes())
                .expect("Failed to insert hash to height");
            self.index_db
                .insert(blockhash, &entry.serialize())
                .expect("Failed to insert blockhash to index");
        }
        Ok(())
    }

    pub fn get_block_entry(&self, blockhash: &[u8; 32]) -> Result<IndexEntry, StorageError> {
        let data = self
            .index_db
            .get(blockhash)?
            .ok_or(StorageError::EntryNotFound)?;

        // Check if entry is marked as orphaned
        if data.len() == 1 && data[0] == 0 {
            return Err(StorageError::OrphanedEntry);
        }

        IndexEntry::deserialize(&data)
            .ok_or(StorageError::InvalidData("Invalid index entry format"))
    }

    pub fn get_blockhash_by_height(&self, height: u32) -> Result<[u8; 32], StorageError> {
        let data = self
            .height_to_hash
            .get(&height.to_le_bytes())?
            .ok_or(StorageError::EntryNotFound)?;
        if data.len() != 32 {
            return Err(StorageError::InvalidData("Invalid blockhash length"));
        }
        let mut blockhash = [0u8; 32];
        blockhash.copy_from_slice(&data);
        Ok(blockhash)
    }

    pub fn get_height_by_blockhash(&self, blockhash: &[u8; 32]) -> Result<u32, StorageError> {
        let data = self
            .hash_to_height
            .get(blockhash)?
            .ok_or(StorageError::EntryNotFound)?;
        if data.len() != 4 {
            return Err(StorageError::InvalidData("Invalid height data length"));
        }
        Ok(u32::from_le_bytes(data[..].try_into().unwrap()))
    }

    /// Marks a block as orphaned by setting its entry to a special value
    /// and removes its height mappings, this is helpful in case a client requests
    /// a block that has been reorganized away.
    pub fn remove_block(&mut self, blockhash: &[u8; 32]) -> Result<(), StorageError> {
        // First check if the block exists in the index
        if self.index_db.get(blockhash)?.is_none() {
            return Err(StorageError::EntryNotFound);
        }

        if let Ok(height) = self.get_height_by_blockhash(blockhash) {
            if height != self.next_height - 1 {
                // TODO: Technically, we should allow removing a deeper block and remove
                // all blocks in the chain leading from it.
                // This is a safeguard for now.
                return Err(StorageError::InvalidHeight); // Remove block should only attempt to remove tip
            }
            self.next_height -= 1;
            self.height_to_hash.remove(&height.to_le_bytes())?;
            self.hash_to_height.remove(blockhash)?;
            // Mark the entry as orphaned with a special zero value
            self.index_db.insert(blockhash, &[0u8; 1])?;

            Ok(())
        } else {
            Err(StorageError::EntryNotFound)
        }
    }
    /// Returns the height of chain
    /// returns -1 if the chain is empty
    pub fn get_current_height(&self) -> i32 {
        self.next_height as i32 - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    fn temp_dir(name: &str) -> PathBuf {
        let mut dir = env::temp_dir();
        dir.push(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_index_operations() {
        let index_dir = temp_dir("test_block_index");
        let (mut index, was_created) = Index::initialize(&index_dir).unwrap();
        assert!(
            was_created,
            "First initialization should create new database"
        );

        let height = 0u32;
        let blockhash = [42u8; 32];
        let entry = IndexEntry {
            file_number: 1,
            offset: 1000,
            length: 500,
        };

        index.insert_block(height, &blockhash, &entry).unwrap();

        let retrieved_entry = index.get_block_entry(&blockhash).unwrap();
        assert_eq!(entry, retrieved_entry);

        let retrieved_blockhash = index.get_blockhash_by_height(height).unwrap();
        assert_eq!(blockhash, retrieved_blockhash);

        let retrieved_height = index.get_height_by_blockhash(&blockhash).unwrap();
        assert_eq!(height, retrieved_height);

        let _ = fs::remove_dir_all(index_dir);
    }

    #[test]
    fn test_not_found_cases() {
        let index_dir = temp_dir("test_block_index_not_found");
        let (index, _) = Index::initialize(&index_dir).unwrap();

        let nonexistent_blockhash = [0u8; 32];
        let nonexistent_height = 99999u32;

        assert!(matches!(
            index.get_block_entry(&nonexistent_blockhash),
            Err(StorageError::EntryNotFound)
        ));

        assert!(matches!(
            index.get_blockhash_by_height(nonexistent_height),
            Err(StorageError::EntryNotFound)
        ));

        assert!(matches!(
            index.get_height_by_blockhash(&nonexistent_blockhash),
            Err(StorageError::EntryNotFound)
        ));

        let _ = fs::remove_dir_all(index_dir);
    }

    #[test]
    fn test_multiple_blocks() {
        let index_dir = temp_dir("test_multiple_blocks");
        let (mut index, _) = Index::initialize(&index_dir).unwrap();

        // Insert multiple blocks
        for i in 0..256 {
            let height = i;
            let blockhash: [u8; 32] = [i as u8; 32];
            let entry = IndexEntry {
                file_number: i as u64,
                offset: i as u64 * 1000,
                length: 500,
            };
            index.insert_block(height, &blockhash, &entry).unwrap();
        }

        // Verify all blocks
        for i in 0..256 {
            let height = i;
            let expected_blockhash = [i as u8; 32];
            let expected_entry = IndexEntry {
                file_number: i as u64,
                offset: i as u64 * 1000,
                length: 500,
            };

            // Verify block entry
            let entry = index.get_block_entry(&expected_blockhash).unwrap();
            assert_eq!(entry, expected_entry);

            // Verify height -> blockhash mapping
            let blockhash = index.get_blockhash_by_height(height).unwrap();
            assert_eq!(blockhash, expected_blockhash);

            // Verify blockhash -> height mapping
            let retrieved_height = index.get_height_by_blockhash(&expected_blockhash).unwrap();
            assert_eq!(retrieved_height, height);
        }

        // Clean up
        let _ = fs::remove_dir_all(index_dir);
    }

    #[test]
    fn test_orphaned_blocks() {
        let index_dir = temp_dir("test_orphaned_blocks");
        let (mut index, _) = Index::initialize(&index_dir).unwrap();

        // Insert a block
        let height = 0u32;
        let blockhash = [42u8; 32];
        let entry = IndexEntry {
            file_number: 1,
            offset: 1000,
            length: 500,
        };
        index.insert_block(height, &blockhash, &entry).unwrap();

        // Verify block exists initially
        assert!(matches!(index.get_block_entry(&blockhash), Ok(_)));

        // Mark block as orphaned
        index.remove_block(&blockhash).unwrap();

        // Verify block is now marked as orphaned
        assert!(matches!(
            index.get_block_entry(&blockhash),
            Err(StorageError::OrphanedEntry)
        ));

        // Verify height mappings are removed
        assert!(matches!(
            index.get_blockhash_by_height(height),
            Err(StorageError::EntryNotFound)
        ));
        assert!(matches!(
            index.get_height_by_blockhash(&blockhash),
            Err(StorageError::EntryNotFound)
        ));

        // Clean up
        let _ = fs::remove_dir_all(index_dir);
    }

    #[test]
    fn test_orphan_nonexistent_block() {
        let index_dir = temp_dir("test_orphan_nonexistent");
        let (mut index, _) = Index::initialize(&index_dir).unwrap();

        let nonexistent_blockhash = [0u8; 32];

        // Attempting to mark non-existent block as orphaned should fail
        assert!(matches!(
            index.remove_block(&nonexistent_blockhash),
            Err(StorageError::EntryNotFound)
        ));

        // Clean up
        let _ = fs::remove_dir_all(index_dir);
    }

    #[test]
    fn test_reopen_existing_db() {
        let index_dir = temp_dir("test_reopen_db");

        // First creation
        let (index1, was_created1) = Index::initialize(&index_dir).unwrap();
        assert!(
            was_created1,
            "First initialization should create new database"
        );
        drop(index1);

        // Reopen existing
        let (_, was_created2) = Index::initialize(&index_dir).unwrap();
        assert!(
            !was_created2,
            "Second initialization should open existing database"
        );

        let _ = fs::remove_dir_all(index_dir);
    }
}
