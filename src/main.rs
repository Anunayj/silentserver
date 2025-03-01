mod logging;
mod storage;

use clap::{Parser, ValueEnum};

use std::path::PathBuf;
use storage::FlatFileStore;

use env_logger::Env;
use log::info;
use logging::setup_logging;

#[derive(Debug, Clone, ValueEnum)]
enum Network {
    Mainnet,
    Testnet,
    Signet,
    Regtest,
}

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Network::Mainnet => write!(f, "mainnet"),
            Network::Testnet => write!(f, "testnet"),
            Network::Signet => write!(f, "signet"),
            Network::Regtest => write!(f, "regtest"),
        }
    }
}

impl Network {
    fn get_dirname(&self) -> &'static str {
        match self {
            Network::Mainnet => "", // Mainnet is stored in base directory, never liked this
            Network::Testnet => "testnet3",
            Network::Signet => "signet",
            Network::Regtest => "regtest",
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory where Silent Payment Server data will be stored
    #[arg(short, long)]
    data_dir: PathBuf,

    /// Bitcoin data directory (defaults to ~/.bitcoin)
    #[arg(short, long, default_value_os_t = default_bitcoin_dir())]
    bitcoin_datadir: PathBuf,

    /// Bitcoin network type
    #[arg(short, long, default_value_t = Network::Mainnet)]
    network: Network,
}

fn default_bitcoin_dir() -> PathBuf {
    dirs::home_dir()
        .expect("Could not determine home directory")
        .join(".bitcoin")
}

fn join_network_dir(base: impl Into<PathBuf>, network: &Network) -> PathBuf {
    base.into().join(network.get_dirname())
}

fn main() {
    let args = Args::parse();
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    setup_logging().expect("Failed to setup logging");

    let data_dir = join_network_dir(args.data_dir, &args.network);
    let store = FlatFileStore::initialize(data_dir).expect("Failed to initialize storage");

    let chain_dir = join_network_dir(&args.bitcoin_datadir, &args.network);
    info!("Using Bitcoin data directory: {}", chain_dir.display());

    // TODO: Initialize the kernel, read the chain state, sync it, etc.
}
