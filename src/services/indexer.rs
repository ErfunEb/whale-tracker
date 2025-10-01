use crate::{
    config::{CHUNK_SIZE, Config, TRANSFER_TOPIC},
    proccessors::process_transfer_log,
    providers::EthereumProvider,
    repositories::{IndexedBlocksRepository, TransferRepository},
};
use alloy::{
    primitives::{B256, keccak256},
    rpc::types::{Filter, Log},
};
use log::{error, info};
use std::{error::Error, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep},
};
use tokio_stream::StreamExt;

pub struct IndexerService {
    pub provider: EthereumProvider,
    pub indexed_block_repo: IndexedBlocksRepository,
    pub transfers_repo: TransferRepository,
    pub config: Config,
}

impl IndexerService {
    pub fn new(
        provider: EthereumProvider,
        transfers_repo: TransferRepository,
        indexed_block_repo: IndexedBlocksRepository,
        config: Config,
    ) -> Arc<Self> {
        Arc::new(Self {
            provider,
            indexed_block_repo,
            transfers_repo,
            config,
        })
    }

    async fn process_block_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<(), Box<dyn Error>> {
        let transfer_topic: B256 = keccak256(TRANSFER_TOPIC.as_bytes());

        let filter = Filter::new()
            .address(self.config.usdc_address)
            .event_signature(transfer_topic)
            .from_block(start_block)
            .to_block(end_block);

        let logs = self.provider.get_logs_with_retry(&filter, 5).await?;

        for log in logs {
            if let Err(e) = process_transfer_log(
                log,
                self.config.threshold,
                self.config.decimals,
                &self.transfers_repo,
            )
            .await
            {
                error!("Error processing log: {e}");
            }
        }

        for block in start_block..=end_block {
            if let Err(e) = self.indexed_block_repo.mark_block_indexed(block).await {
                error!("Failed to mark block {block} as indexed: {e}");
            }
        }

        Ok(())
    }

    async fn fill_missing_data(&self, start_block: u64) -> Result<(), Box<dyn Error>> {
        let current_block = self.provider.get_current_block_number().await?;

        if current_block > start_block {
            let missing_blocks = self
                .indexed_block_repo
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
                        self.process_block_range(start, end).await?;
                        start = end + 1;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn Error>> {
        let current_block = self.provider.get_current_block_number().await?;
        info!("Current block: {current_block}");

        let (first_indexed_block, _) = self
            .indexed_block_repo
            .get_first_last_block_indexed()
            .await?;

        let start_block = if self.config.start_block > 0 {
            self.config.start_block
        } else if let Some(min) = first_indexed_block {
            min
        } else {
            current_block
        };
        info!("Starting from block: {start_block}");

        let (real_time_tx, mut real_time_rx) = mpsc::channel::<Log>(100);
        let real_time_service = Arc::clone(&self);
        tokio::spawn(async move {
            let transfer_topic: B256 = keccak256(TRANSFER_TOPIC.as_bytes());
            let filter = Filter::new()
                .address(real_time_service.config.usdc_address)
                .event_signature(transfer_topic);

            match real_time_service.provider.subscribe_logs(&filter).await {
                Ok(subscription) => {
                    let mut stream = subscription.into_stream();
                    while let Some(log) = stream.next().await {
                        if let Err(e) = real_time_tx.send(log).await {
                            error!("Failed to send real-time log: {e}");
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to subscribe to logs: {e}");
                }
            };
        });

        let backfill_service = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                let start = Instant::now();

                if let Err(e) = backfill_service.fill_missing_data(start_block).await {
                    error!("Error filling the data: {e:?}");
                }

                let task_duration = start.elapsed();
                info!("Task completed in: {task_duration:?}");

                sleep(Duration::from_secs(60 * 60)).await; // An hour sleep
            }
        });

        while let Some(log) = real_time_rx.recv().await {
            if let Err(e) = process_transfer_log(
                log.clone(),
                self.config.threshold,
                self.config.decimals,
                &self.transfers_repo,
            )
            .await
            {
                error!("Error processing real-time transfer: {e}");
            }

            if let Some(block_number) = log.block_number {
                if let Err(e) = self
                    .indexed_block_repo
                    .mark_block_indexed(block_number)
                    .await
                {
                    error!("Failed to mark block {block_number} as indexed: {e}");
                }
            }
        }

        Ok(())
    }
}
