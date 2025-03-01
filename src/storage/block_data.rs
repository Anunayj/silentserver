use crc32fast::Hasher;
use std::convert::TryInto;
use super::StorageError;

pub const TWEAK_SIZE: usize = 33;

#[derive(Debug, PartialEq)]
pub struct BlockData {
    pub blockhash: [u8; 32],
    pub tweaks: Vec<[u8; TWEAK_SIZE]>,
}

impl BlockData {
    /// Serialize a BlockData record into our custom binary format.
    /// This is serialized as:
    /// [blockhash (32 bytes)] [lenTweaks (u32 little-endian)] [CRC32 of tweaks (u32 little-endian)] [<tweaks> (each tweak is 33 bytes)]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&self.blockhash);
        let len_tweaks = self.tweaks.len() as u32;
        buf.extend_from_slice(&len_tweaks.to_le_bytes());

        let crc = {
            let mut hasher = Hasher::new();
            for tweak in &self.tweaks {
                hasher.update(tweak);
            }
            hasher.finalize()
        };

        buf.extend_from_slice(&crc.to_le_bytes());
        for tweak in &self.tweaks {
            buf.extend_from_slice(tweak);
        }

        buf
    }

    /// Deserialize a BlockData record from a byte slice.
    pub fn deserialize(data: &[u8]) -> Result<BlockData, StorageError> {
        let mut pos = 0;

        if data.len() < pos + 32 {
            return Err(StorageError::DeserializeError("insufficient data for blockhash"));
        }
        let mut blockhash = [0u8; 32];
        blockhash.copy_from_slice(&data[pos..pos+32]);
        pos += 32;
        
        // Read lenTweaks.
        if data.len() < pos + 4 {
            return Err(StorageError::DeserializeError("insufficient data for lenTweaks"));
        }
        let len_tweaks = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
        pos += 4;
        
        // Read CRC32.
        if data.len() < pos + 4 {
            return Err(StorageError::DeserializeError("insufficient data for CRC"));
        }
        let crc_stored = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
        pos += 4;
        
        // Expected length for tweaks.
        let tweaks_bytes_len = len_tweaks * TWEAK_SIZE;
        if data.len() < pos + tweaks_bytes_len {
            return Err(StorageError::DeserializeError("insufficient data for tweaks"));
        }
        let tweaks_data = &data[pos..pos+tweaks_bytes_len];
        let mut hasher = Hasher::new();
        hasher.update(tweaks_data);
        let crc_computed = hasher.finalize();
        
        if crc_computed != crc_stored {
            return Err(StorageError::CrcMismatch);
        }
        
        let mut tweaks = Vec::with_capacity(len_tweaks);
        for i in 0..len_tweaks {
            let start = i * TWEAK_SIZE;
            let end = start + TWEAK_SIZE;
            let mut tweak = [0u8; TWEAK_SIZE];
            tweak.copy_from_slice(&tweaks_data[start..end]);
            tweaks.push(tweak);
        }
        Ok(BlockData { blockhash, tweaks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_data_serialization() {
        let block = BlockData {
            blockhash: [1u8; 32],
            tweaks: vec![[2u8; TWEAK_SIZE], [3u8; TWEAK_SIZE]],
        };

        let serialized = block.serialize();
        let deserialized = BlockData::deserialize(&serialized).unwrap();

        assert_eq!(block, deserialized);
    }

    #[test]
    fn test_block_data_invalid_crc() {
        let mut serialized = BlockData {
            blockhash: [1u8; 32],
            tweaks: vec![[2u8; TWEAK_SIZE]],
        }.serialize();

        // Corrupt the data by modifying a tweak
        if let Some(byte) = serialized.last_mut() {
            *byte ^= 1; // Flip a bit
        }

        assert!(matches!(
            BlockData::deserialize(&serialized),
            Err(StorageError::CrcMismatch)
        ));
    }
} 