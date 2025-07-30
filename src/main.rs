use alloy::{
    primitives::{Address, B256, U256, keccak256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};
use dotenv::dotenv;
use sqlx::mysql::MySqlPoolOptions;
use std::env;
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

    let ws_rpc_url = env::var("WS_RPC_URL").expect("Environment variable WS_RPC_URL is not set");
    let database_url =
        env::var("DATABASE_URL").expect("Environment variable DATABASE_URL is not set");

    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

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
            if let Some(tx_hash) = log.transaction_hash {
                let event = log.into_inner();
                let value = U256::from_be_slice(&event.data.data[..32]);
                if value > threshold_amount {
                    let topics = event.data.topics();
                    let from_address = Address::from_slice(&topics[1][12..])
                        .to_string()
                        .to_lowercase();
                    let to_address = Address::from_slice(&topics[2][12..])
                        .to_string()
                        .to_lowercase();

                    sqlx::query(
                        r#"
                        INSERT INTO transfers (
                            tx_hash, block_number, from_address, to_address, amount, token_address
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    "#,
                    )
                    .bind(tx_hash.to_string().to_lowercase())
                    .bind(block_number)
                    .bind(&from_address)
                    .bind(&to_address)
                    .bind(value.to_string())
                    .bind(usdc_address.to_string().to_lowercase())
                    .execute(&pool)
                    .await?;

                    println!(
                        "High-value transfer: from {} to {} amount {} USDC (Tx: {})",
                        from_address,
                        to_address,
                        to_human_readable_amount(value, DECIMALS),
                        tx_hash
                    );
                }
            } else {
                println!("Log with missing transaction_hash skipped");
                continue;
            }
        }
    }

    Ok(())
}

fn to_human_readable_amount(amount: U256, decimals: u32) -> f64 {
    let scale = 10u64.pow(decimals) as f64;
    let amount_f64 = amount.to_string().parse::<f64>().unwrap_or(0.0);
    amount_f64 / scale
}
