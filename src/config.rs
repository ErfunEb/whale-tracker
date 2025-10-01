use alloy::primitives::{Address, U256};
use dotenv::dotenv;
use std::{env, error::Error};

pub const USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
pub const DECIMALS: u32 = 6;
pub const THRESHOLD: u64 = 1_000_000;
pub const TRANSFER_TOPIC: &str = "Transfer(address,address,uint256)";
pub const CHUNK_SIZE: u64 = 50;
pub const RPC_RETRY_DELAY_MS: u64 = 1000;

#[derive(Debug, Clone)]
pub struct Config {
    pub ws_rpc_url: String,
    pub database_url: String,
    pub usdc_address: Address,
    pub threshold: U256,
    pub decimals: u32,
    pub start_block: u64,
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn Error>> {
        dotenv().ok();

        let start_block = env::var("START_BLOCK")
            .unwrap_or_else(|_| "latest".to_string())
            .parse::<u64>()
            .unwrap_or(0);

        Ok(Config {
            ws_rpc_url: env::var("WS_RPC_URL").expect("WS_RPC_URL must be set"),
            database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
            usdc_address: USDC_ADDRESS.parse()?,
            threshold: U256::from(THRESHOLD) * U256::from(10u64).pow(U256::from(DECIMALS)),
            decimals: DECIMALS,
            start_block,
        })
    }
}
