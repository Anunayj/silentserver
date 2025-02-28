mod storage;

use bitcoinkernel::{
    ChainType, ChainstateManager, ChainstateManagerOptions, ContextBuilder, KernelError,
    Log, Logger,
};
use std::sync::Arc;
use env_logger::Builder;
use log::LevelFilter;

use storage::FlatFileStore;

struct MainLog {}

impl Log for MainLog {
    fn log(&self, message: &str) {
        log::info!(
            target: "libbitcoinkernel", 
            "{}", message.strip_suffix("\r\n").or_else(|| message.strip_suffix('\n')).unwrap_or(message));
    }
}

fn setup_logging() -> Result<Logger<MainLog>, KernelError> {
    let mut builder = Builder::from_default_env();
    builder.filter(None, LevelFilter::Info).init();
    Logger::new(MainLog {})
}


fn main() { 
    let _ = setup_logging().unwrap();
    // Initialize context with signet chain type and specific signet challenge
    let context = Arc::new(
        ContextBuilder::new()
            .chain_type(ChainType::REGTEST)
            .build()
            .unwrap(),
    );
    // Create ChainstateManagerOptions with the specified directories
    let options: ChainstateManagerOptions = ChainstateManagerOptions::new(&context, "/mnt/d/bitcoin_data/regtest", "/mnt/d/bitcoin_data/regtest/blocks").unwrap();
    // Create ChainstateManager
    let chainman = ChainstateManager::new(options, Arc::clone(&context)).unwrap();
    // chainman.import_blocks().unwrap();
    // Automatically fetch and process new blocks
    let mut tip_index = chainman.get_block_index_tip();
    // chainman.read_undo_data(block_index);
    dbg!(tip_index.height());
    
}