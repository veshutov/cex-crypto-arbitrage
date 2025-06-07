use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{Exchange, ExchangeError, ExchangeFee, OrderType, TickerData};

pub struct MexcExchange {
    client: Client,
    api_key: String,
    api_secret: String,
    taker_fee: f64,
    maker_fee: f64,
}

impl MexcExchange {
    pub fn new(api_key: String, api_secret: String, taker_fee: f64, maker_fee: f64) -> Self {
        Self {
            client: Client::new(),
            api_key,
            api_secret,
            taker_fee,
            maker_fee,
        }
    }
}

#[async_trait]
impl Exchange for MexcExchange {
    fn name(&self) -> &'static str {
        "mexc"
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let url = "https://contract.mexc.com/api/v1/contract/ticker";
        let response = self.client.get(url).send().await?;
        
        if !response.status().is_success() {
            return Err(ExchangeError::InvalidResponse(format!(
                "Failed to get tickers: {}",
                response.status()
            )));
        }

        let json: Value = response.json().await?;
        let data = json["data"].as_array().ok_or_else(|| {
            ExchangeError::InvalidResponse("Invalid response format".to_string())
        })?;

        let mut tickers = Vec::new();
        for item in data {
            let symbol = item["symbol"]
                .as_str()
                .ok_or_else(|| ExchangeError::InvalidResponse("Missing symbol".to_string()))?
                .to_string();

            let best_bid = item["bid1"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or_else(|| ExchangeError::InvalidResponse("Invalid bid price".to_string()))?;

            let best_ask = item["ask1"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or_else(|| ExchangeError::InvalidResponse("Invalid ask price".to_string()))?;

            tickers.push(TickerData {
                symbol,
                best_bid_price: best_bid,
                best_ask_price: best_ask,
            });
        }

        Ok(tickers)
    }

    fn get_fees(&self, order_type: OrderType) -> ExchangeFee {
        match order_type {
            OrderType::Limit => ExchangeFee {
                maker_fee: self.maker_fee,
                taker_fee: self.taker_fee,
            },
            OrderType::Market => ExchangeFee {
                maker_fee: self.maker_fee,
                taker_fee: self.taker_fee,
            },
        }
    }
} 