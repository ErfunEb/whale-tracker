use alloy::primitives::U256;
use sqlx::mysql::MySqlPoolOptions;
use std::error::Error;

#[derive(Clone)]
pub struct TransferRepository {
    pool: sqlx::MySqlPool,
}

pub struct Transfer {
    pub tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub from_address: String,
    pub to_address: String,
    pub amount: U256,
    pub token_address: String,
}

impl TransferRepository {
    pub async fn new(database_url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    pub async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            r#"
            INSERT INTO transfers (
                tx_hash, log_index, block_number, from_address, to_address, amount, token_address
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(transfer.tx_hash.to_string().to_lowercase())
        .bind(transfer.log_index)
        .bind(transfer.block_number)
        .bind(transfer.from_address.as_str())
        .bind(transfer.to_address.as_str())
        .bind(transfer.amount.to_string())
        .bind(transfer.token_address.as_str())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
