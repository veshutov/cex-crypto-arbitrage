use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{Exchange, ExchangeError, ExchangeFee, TickerData, ExchangeName};

pub struct GateExchange {
    client: Client,
    api_key: String,
    api_secret: String,
    taker_fee: f64,
    maker_fee: f64,
}

impl GateExchange {
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
impl Exchange for GateExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Gate
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let url = "https://api.gateio.ws/api/v4/futures/usdt/tickers";
        let response = self.client.get(url).send().await?;
        
        if !response.status().is_success() {
            return Err(ExchangeError::InvalidResponse(format!(
                "Failed to get tickers: {}",
                response.status()
            )));
        }

        let json: Value = response.json().await?;

        let tickers = json
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["contract"].as_str()?;
                
                // Parse bid and ask prices
                let best_bid = item["highest_bid"].as_str()?.parse::<f64>().ok()?;
                let best_ask = item["lowest_ask"].as_str()?.parse::<f64>().ok()?;
                let volume_24h = item["volume_24h"].as_str()?.parse::<f64>().ok()?;

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

    fn get_fees(&self) -> ExchangeFee {
        ExchangeFee {
            maker_fee: self.maker_fee,
            taker_fee: self.taker_fee,
        }
    }
} 