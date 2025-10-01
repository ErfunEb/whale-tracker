mod config;
mod proccessors;
mod providers;
mod repositories;
mod services;

use log::info;
use std::error::Error;

use config::Config;
use providers::EthereumProvider;
use repositories::{IndexedBlocksRepository, TransferRepository};
use services::IndexerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load()?;
    env_logger::init();
    info!("Starting whale tracker indexer...");
    info!("Configuration loaded!");

    let repository = TransferRepository::new(&config.database_url).await?;
    let index_repo = IndexedBlocksRepository::new(&config.database_url).await?;
    info!("Database connection established!");

    let provider = EthereumProvider::new(&config.ws_rpc_url).await?;
    info!("Connected to Ethereum node!");

    let indexer = IndexerService::new(provider, repository, index_repo, config);
    indexer.run().await?;

    Ok(())
}
