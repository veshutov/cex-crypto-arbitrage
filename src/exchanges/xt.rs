use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct XtExchange {
    config: ExchangeConfig,
    client: Client,
}

impl XtExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for XtExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Xt
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://fapi.xt.com/future/market/v1/public/cg/contracts")
            .send()
            .await?;

            let data: Value = response.json().await?;

            // Check if the response is successful
            // if data["returnCode"].as_i64().unwrap() != 0 {
            //     println!("Eror response from XT {}", data);
            //     return Err(ExchangeError::InvalidResponse(format!(
            //         "API error: {}",
            //         data["msgInfo"].as_str().unwrap_or("Unknown error")
            //     )));
            // }
    
            let tickers = data
                .as_array()
                .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
                .iter()
                .filter_map(|item| {
                    let symbol = item["symbol"].as_str().unwrap().to_uppercase();
                    let best_bid = item["ask"].as_str().unwrap().parse::<Decimal>().unwrap();
                    let best_ask = item["bid"].as_str().unwrap().parse::<Decimal>().unwrap();
                    let volume_24h = Decimal::from(1);
    
                    if !symbol.ends_with("USDT") {
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
        todo!("Implement XT orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement XT place order")
    }

    async fn close_position(
        &self,
        _position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement XT close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement XT get open positions")
    }
} 