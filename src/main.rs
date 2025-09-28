use alloy::{
    primitives::{Address, B256, U256, keccak256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{Filter, Log},
};
use dotenv::dotenv;
use log::{error, info, warn};
use sqlx::mysql::MySqlPoolOptions;
use std::{env, sync::Arc};
use std::{error::Error, time::Duration};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep},
};
use tokio_stream::StreamExt;

const USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const DECIMALS: u32 = 6;
const THRESHOLD: u64 = 1_000_000;
const TRANSFER_TOPIC: &str = "Transfer(address,address,uint256)";
const CHUNK_SIZE: u64 = 50;
const RPC_RETRY_DELAY_MS: u64 = 1000;

#[derive(Debug, Clone)]
struct Config {
    ws_rpc_url: String,
    database_url: String,
    usdc_address: Address,
    threshold: U256,
    decimals: u32,
    start_block: u64,
}

impl Config {
    fn load() -> Result<Self, Box<dyn Error>> {
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

#[derive(Clone)]
struct TransferRepository {
    pool: sqlx::MySqlPool,
}

struct Transfer {
    tx_hash: String,
    log_index: u64,
    block_number: u64,
    from_address: String,
    to_address: String,
    amount: U256,
    token_address: String,
}

impl TransferRepository {
    async fn new(database_url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), Box<dyn Error>> {
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

#[derive(Clone)]
struct IndexedBlocksRepository {
    pool: sqlx::MySqlPool,
}

impl IndexedBlocksRepository {
    async fn new(database_url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    async fn mark_block_indexed(&self, block_number: u64) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            "INSERT INTO indexed_blocks (block_number) VALUES (?)
            ON DUPLICATE KEY UPDATE indexed_at = CURRENT_TIMESTAMP",
        )
        .bind(block_number)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_first_last_block_indexed(
        &self,
    ) -> Result<(Option<u64>, Option<u64>), Box<dyn Error>> {
        let result: Option<(Option<i64>, Option<i64>)> =
            sqlx::query_as("SELECT MIN(block_number), MAX(block_number) from indexed_blocks")
                .fetch_optional(&self.pool)
                .await?;

        let (min, max) = result.map_or((None, None), |(min, max)| {
            (min.map(|v| v as u64), max.map(|v| v as u64))
        });

        Ok((min, max))
    }

    async fn find_missing_blocks(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<(u64, u64)>, Box<dyn Error>> {
        let indexed_blocks: Vec<i64> = sqlx::query_scalar(
            "SELECT block_number FROM indexed_blocks
                 WHERE block_number BETWEEN ? AND ?
                 ORDER BY block_number",
        )
        .bind(start_block)
        .bind(end_block)
        .fetch_all(&self.pool)
        .await?;

        let mut gaps = Vec::new();
        let mut previous_block = start_block;

        let mut range = vec![start_block as i64 - 1];
        range.extend(indexed_blocks);
        range.push(end_block as i64 + 1);

        for block in range {
            if block as u64 > previous_block + 1 {
                gaps.push((previous_block + 1, block as u64 - 1));
            }

            previous_block = block as u64;
        }

        Ok(gaps)
    }
}

async fn get_current_block_number(provider: &impl Provider) -> Result<u64, Box<dyn Error>> {
    let block_number = provider.get_block_number().await?;
    Ok(block_number)
}

async fn get_logs_with_retry(
    provider: &impl Provider,
    filter: &Filter,
    max_retries: u32,
) -> Result<Vec<Log>, Box<dyn Error>> {
    let mut attempts = 0;

    loop {
        match provider.get_logs(filter).await {
            Ok(logs) => return Ok(logs),
            Err(e) => {
                if attempts >= max_retries {
                    return Err(e.into());
                }

                attempts += 1;
                let delay =
                    std::time::Duration::from_millis(RPC_RETRY_DELAY_MS * 2u64.pow(attempts));

                error!("RPC error (retry {attempts} in {delay:?}): {e}");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn process_transfer_log(
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

async fn process_block_range(
    provider: &impl Provider,
    repository: &TransferRepository,
    indexed_repo: &IndexedBlocksRepository,
    config: &Config,
    start_block: u64,
    end_block: u64,
) -> Result<(), Box<dyn Error>> {
    let transfer_topic: B256 = keccak256(TRANSFER_TOPIC.as_bytes());

    let filter = Filter::new()
        .address(config.usdc_address)
        .event_signature(transfer_topic)
        .from_block(start_block)
        .to_block(end_block);

    let logs = get_logs_with_retry(provider, &filter, 5).await?;

    for log in logs {
        if let Err(e) =
            process_transfer_log(log, config.threshold, config.decimals, repository).await
        {
            error!("Error processing log: {e}");
        }
    }

    for block in start_block..=end_block {
        if let Err(e) = indexed_repo.mark_block_indexed(block).await {
            error!("Failed to mark block {block} as indexed: {e}");
        }
    }

    Ok(())
}

async fn real_time_subscription<P>(
    provider: P,
    config: Config,
    tx: mpsc::Sender<alloy::rpc::types::Log>,
) -> Result<(), Box<dyn Error>>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let transfer_topic: B256 = keccak256(TRANSFER_TOPIC.as_bytes());

    let filter = Filter::new()
        .address(config.usdc_address)
        .event_signature(transfer_topic);

    let subscription = provider.subscribe_logs(&filter).await?;
    let mut stream = subscription.into_stream();

    while let Some(log) = stream.next().await {
        if let Err(e) = tx.send(log).await {
            error!("Failed to send real-time log: {e}");
            break;
        }
    }

    Ok(())
}

async fn fill_missing_data(
    provider: &impl Provider,
    repository: &TransferRepository,
    index_repo: &IndexedBlocksRepository,
    config: &Config,
    start_block: u64,
) -> Result<(), Box<dyn Error>> {
    let current_block = get_current_block_number(&provider).await?;

    if current_block > start_block {
        let missing_blocks = index_repo
            .find_missing_blocks(start_block, current_block)
            .await?;

        if !missing_blocks.is_empty() {
            info!(
                "Found {} gaps in previously indexed blocks",
                missing_blocks.len()
            );

            for (gap_start, gap_end) in missing_blocks {
                info!("Filling gap: {gap_start} to {gap_end}");

                let mut start = gap_start;
                while start <= gap_end {
                    let end = (start + CHUNK_SIZE - 1).min(gap_end);
                    info!("Proccessing from: {start} to {end}");
                    process_block_range(&provider, repository, index_repo, config, start, end)
                        .await?;
                    start = end + 1;
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load()?;
    env_logger::init();
    info!("Starting whale tracker indexer...");
    info!("Configuration loaded!");

    let repository = TransferRepository::new(&config.database_url).await?;
    let index_repo = Arc::new(IndexedBlocksRepository::new(&config.database_url).await?);
    info!("Database connection established");

    let ws = WsConnect::new(&config.ws_rpc_url);
    let provider = Arc::new(ProviderBuilder::new().connect_ws(ws).await?);
    info!("Connected to Ethereum node");

    let current_block_provider = Arc::clone(&provider);
    let current_block = get_current_block_number(&current_block_provider).await?;
    info!("Current block: {current_block}");

    let block_fetch_repo = Arc::clone(&index_repo);
    let (first_indexed_block, _) = block_fetch_repo.get_first_last_block_indexed().await?;

    let start_block = if config.start_block > 0 {
        config.start_block
    } else if let Some(min) = first_indexed_block {
        min
    } else {
        current_block
    };
    info!("Starting from block: {start_block}");

    let (real_time_tx, mut real_time_rx) = mpsc::channel(100);
    let real_time_provider = provider.clone();
    let real_time_config = config.clone();

    tokio::spawn(async move {
        if let Err(e) =
            real_time_subscription(real_time_provider, real_time_config, real_time_tx).await
        {
            error!("Real-time subscription error: {e}");
        }
    });

    let backfill_repository = repository.clone();
    let backfill_config = config.clone();

    let backfill_indexing_repo = Arc::clone(&index_repo);
    tokio::spawn(async move {
        let missing_data_provider = Arc::clone(&provider);
        loop {
            let start = Instant::now();

            if let Err(e) = fill_missing_data(
                &missing_data_provider,
                &backfill_repository,
                &backfill_indexing_repo,
                &backfill_config,
                start_block,
            )
            .await
            {
                error!("Error filling the data: {e:?}");
            }

            let task_duration = start.elapsed();
            info!("Task completed in: {task_duration:?}");

            sleep(Duration::from_secs(60 * 60)).await; // An hour sleep
        }
    });

    let marking_block_repo = Arc::clone(&index_repo);
    while let Some(log) = real_time_rx.recv().await {
        if let Err(e) =
            process_transfer_log(log.clone(), config.threshold, config.decimals, &repository).await
        {
            error!("Error processing real-time transfer: {e}");
        }

        if let Some(block_number) = log.block_number {
            if let Err(e) = marking_block_repo.mark_block_indexed(block_number).await {
                error!("Failed to mark block {block_number} as indexed: {e}");
            }
        }
    }

    Ok(())
}
