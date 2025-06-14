use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct Htx {
    config: ExchangeConfig,
    client: Client,
}

impl Htx {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for Htx {
    fn name(&self) -> ExchangeName {
        ExchangeName::Htx
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://api.hbdm.com/v2/linear-swap-ex/market/detail/batch_merged")
            .send()
            .await?;

        let data: Value = response.json().await?;

        // Check if the response is successful
        if data["status"].as_str().unwrap_or("error") != "ok" {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["err-msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let tickers = data["ticks"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["contract_code"].as_str()?;
                let best_bid = item["bid"][0].to_string().parse::<Decimal>().ok()?;
                let best_ask = item["ask"][0].to_string().parse::<Decimal>().ok()?;
                let volume_24h = item["vol"].as_str()?.parse::<Decimal>().ok()?;

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                })
            })
            .collect();

        Ok(tickers)
    }

    async fn subscribe_orderbook(
        &self,
        _config: SubscriptionConfig,
        _sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        todo!("Implement HTX orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement HTX place order")
    }

    async fn close_position(
        &self,
        _order_id: &str,
        _symbol: &str,
        _place_order_side: crate::exchanges::OrderSide,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement HTX close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement HTX get open positions")
    }
} 