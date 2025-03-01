// // Initialize context with signet chain type and specific signet challenge
// let context = Arc::new(
//     ContextBuilder::new()
//         .chain_type(ChainType::REGTEST)
//         .build()
//         .unwrap(),
// );
// // Create ChainstateManagerOptions with the specified directories
// let options: ChainstateManagerOptions = ChainstateManagerOptions::new(&context, "/mnt/d/bitcoin_data/regtest", "/mnt/d/bitcoin_data/regtest/blocks").unwrap();
// // Create ChainstateManager
// let chainman = ChainstateManager::new(options, Arc::clone(&context)).unwrap();
// // chainman.import_blocks().unwrap();
// // Automatically fetch and process new blocks
// let mut genisis = chainman.get_block_index_genesis();