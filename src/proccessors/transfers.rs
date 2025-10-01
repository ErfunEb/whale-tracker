use alloy::{
    primitives::{Address, U256},
    rpc::types::Log,
};
use log::{info, warn};
use std::error::Error;

use crate::{
    config::USDC_ADDRESS,
    repositories::{Transfer, TransferRepository},
};

pub async fn process_transfer_log(
    log: Log,
    threshold: U256,
    decimals: u32,
    repository: &TransferRepository,
) -> Result<(), Box<dyn Error>> {
    let tx_hash = match log.transaction_hash {
        Some(hash) => hash,
        None => {
            warn!("Skipping log with missing transaction hash");
            return Ok(());
        }
    };

    if tx_hash.to_string().len() != 66 {
        warn!("Invalid TX hash!");
        return Ok(());
    }

    let log_index = log.log_index.unwrap_or(0);
    let block_number = log.block_number.unwrap_or(0);
    let event = log.into_inner();
    let value = U256::from_be_slice(&event.data.data[..32]);

    if value <= threshold {
        return Ok(());
    }

    let topics = event.data.topics();
    if topics.len() < 3 {
        warn!("Invalid transfer event: not enough topics");
        return Ok(());
    }

    let from_address = Address::from_slice(&topics[1][12..])
        .to_string()
        .to_lowercase();
    let to_address = Address::from_slice(&topics[2][12..])
        .to_string()
        .to_lowercase();

    let transfer_tx = Transfer {
        tx_hash: tx_hash.to_string(),
        amount: value,
        token_address: USDC_ADDRESS.to_lowercase(),
        log_index,
        block_number,
        from_address,
        to_address,
    };

    repository.insert_transfer(&transfer_tx).await?;

    let human_amount = to_human_readable_amount(value, decimals);
    info!(
        "High-value transfer: from {} to {} amount {human_amount} USDC (Tx: {tx_hash}) in block {block_number}",
        transfer_tx.from_address, transfer_tx.to_address
    );

    Ok(())
}

fn to_human_readable_amount(amount: U256, decimals: u32) -> f64 {
    let scale = 10u64.pow(decimals) as f64;
    let amount_f64 = amount.to_string().parse::<f64>().unwrap_or(0.0);
    amount_f64 / scale
}
