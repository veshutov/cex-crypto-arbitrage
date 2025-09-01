use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct DeepcoinExchange {
    config: ExchangeConfig,
    client: Client,
}

impl DeepcoinExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for DeepcoinExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Deepcoin
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://api.deepcoin.com/deepcoin/market/tickers?instType=SWAP")
            .send()
            .await?;

        let data: Value = response.json().await?;

        // Check if the response is successful
        if data["code"].as_str().unwrap() != "0" {
            println!("Eror response from BitMart {}", data);
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let tickers = data["data"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["instId"].as_str().unwrap();
                let best_bid = item["bidPx"].as_str().unwrap().parse::<Decimal>().unwrap();
                let best_ask = item["askPx"].as_str().unwrap().parse::<Decimal>().unwrap();
                let volume_24h = item["volCcy24h"].as_str().unwrap().parse::<Decimal>().unwrap();

                if !symbol.contains("USDT") {
                    return None;
                }

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                    multiplier: Decimal::from(1),
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
        todo!("Implement BitMart orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement BitMart place order")
    }

    async fn close_position(
        &self,
        _position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement BitMart close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement BitMart get open positions")
    }
} 