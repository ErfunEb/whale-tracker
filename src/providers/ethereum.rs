use crate::config;

use alloy::{
    providers::{
        Identity, Provider, ProviderBuilder, RootProvider, WsConnect,
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
    },
    pubsub::Subscription,
    rpc::types::{Filter, Log},
};
use log::error;
use std::error::Error;

pub type EthereumProviderType = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

#[derive(Clone)]
pub struct EthereumProvider {
    pub provider: EthereumProviderType,
}

impl EthereumProvider {
    pub async fn new(ws_rpc_url: &str) -> Result<Self, Box<dyn Error>> {
        let z = WsConnect::new(ws_rpc_url);
        let provider = ProviderBuilder::new().connect_ws(z).await?;

        Ok(Self { provider })
    }

    pub async fn get_current_block_number(&self) -> Result<u64, Box<dyn Error>> {
        let block_number = self.provider.get_block_number().await?;
        Ok(block_number)
    }

    pub async fn get_logs_with_retry(
        &self,
        filter: &Filter,
        max_retries: u32,
    ) -> Result<Vec<Log>, Box<dyn Error>> {
        let mut attempts = 0;

        loop {
            match self.provider.get_logs(filter).await {
                Ok(logs) => return Ok(logs),
                Err(e) => {
                    if attempts >= max_retries {
                        return Err(e.into());
                    }

                    attempts += 1;
                    let delay = std::time::Duration::from_millis(
                        config::RPC_RETRY_DELAY_MS * 2u64.pow(attempts),
                    );

                    error!("RPC error (retry {attempts} in {delay:?}): {e}");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub async fn subscribe_logs(
        &self,
        filter: &Filter,
    ) -> Result<Subscription<Log>, Box<dyn Error + Send + Sync>> {
        let sub = self.provider.subscribe_logs(filter).await?;
        Ok(sub)
    }
}
