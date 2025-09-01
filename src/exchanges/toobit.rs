use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct ToobitExchange {
    config: ExchangeConfig,
    client: Client,
}

impl ToobitExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for ToobitExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Toobit
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://api.toobit.com/quote/v1/ticker/bookTicker")
            .send()
            .await?;

        let data: Value = response.json().await?;

        let tickers = data
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["s"].as_str().unwrap();
                let best_bid = item["b"].as_str().unwrap().parse::<Decimal>().unwrap();
                let best_ask = item["a"].as_str().unwrap().parse::<Decimal>().unwrap();
                let volume_24h = Decimal::from(1);

                if best_ask == Decimal::ZERO {
                    return None;
                }

                if best_ask == Decimal::ZERO {
                    return None;
                }

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
        todo!("Implement Toobit orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement Toobit place order")
    }

    async fn close_position(
        &self,
        _position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement Toobit close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement Toobit get open positions")
    }
} 