use alloy::{
    primitives::{Address, B256, U256, keccak256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};
use dotenv::dotenv;
use std::error::Error;
use tokio_stream::StreamExt;

const USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const DECIMALS: u32 = 6;
const THRESHOLD: u64 = 1_000_000;
const TRANSFER_TOPIC: &str = "Transfer(address,address,uint256)";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let transfer_topic: B256 = keccak256(TRANSFER_TOPIC.as_bytes());
    let threshold_amount = U256::from(THRESHOLD) * U256::from(10u64).pow(U256::from(DECIMALS));
    let usdc_address = USDC_ADDRESS.parse::<Address>()?;

    let ws_rpc_url =
        std::env::var("WS_RPC_URL").map_err(|_| "Environment variable WS_RPC_URL is not set")?;

    let ws = WsConnect::new(&ws_rpc_url);
    let provider = ProviderBuilder::new().connect_ws(ws).await?;

    let subscription = provider.subscribe_blocks().await?;
    let mut stream = subscription.into_stream();

    while let Some(block) = stream.next().await {
        let block_number = block.number;
        println!("Block Number: {block_number}");

        let filter = Filter::new()
            .address(usdc_address)
            .event_signature(transfer_topic)
            .from_block(block_number)
            .to_block(block_number);

        let logs = provider.get_logs(&filter).await?;

        if logs.is_empty() {
            println!("No USDC transfers in block {block_number}!");
        }

        for log in logs {
            let event = log.into_inner();
            let value = U256::from_be_slice(&event.data.data[..32]);
            if value > threshold_amount {
                let parsed_value = u256_to_f64(value, DECIMALS);
                println!("More than a Million: {parsed_value} USDC \n");
            }
        }
    }

    Ok(())
}

fn u256_to_f64(value: U256, decimals: u32) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0) / 10f64.powi(decimals as i32)
}
