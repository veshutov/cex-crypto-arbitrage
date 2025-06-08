use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{Exchange, ExchangeError, ExchangeFee, TickerData, ExchangeName};

pub struct OkxExchange {
    client: Client,
    api_key: String,
    api_secret: String,
    taker_fee: f64,
    maker_fee: f64,
}

impl OkxExchange {
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
impl Exchange for OkxExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Okx
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP";
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
            let symbol = item["instId"]
                .as_str()
                .ok_or_else(|| ExchangeError::InvalidResponse("Missing symbol".to_string()))?
                .to_string();

            let best_bid = item["bidPx"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or_else(|| ExchangeError::InvalidResponse("Invalid bid price".to_string()))?;

            let best_ask = item["askPx"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or_else(|| ExchangeError::InvalidResponse("Invalid ask price".to_string()))?;

            tickers.push(TickerData {
                symbol,
                best_bid_price: best_bid,
                best_ask_price: best_ask,
                volume_24h: 1.0,
            });
        }

        Ok(tickers)
    }

    fn get_fees(&self) -> ExchangeFee {
        ExchangeFee {
            maker_fee: self.maker_fee,
            taker_fee: self.taker_fee,
        }
    }
} 