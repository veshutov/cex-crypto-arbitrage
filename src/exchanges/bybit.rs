use super::{Exchange, ExchangeError, ExchangeFee, OrderType};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;

pub struct BybitExchange {
    client: Client,
    api_key: String,
    api_secret: String,
    taker_fee: f64,
    maker_fee: f64,
}

impl BybitExchange {
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
impl Exchange for BybitExchange {
    fn name(&self) -> &'static str {
        "bybit"
    }

    async fn get_futures_tickers(&self) -> Result<Vec<String>, ExchangeError> {
        let response = self.client
            .get("https://api.bybit.com/v5/market/instruments-info?category=linear")
            .send()
            .await?;

        let data: Value = response.json().await?;
        
        let symbols = data["result"]["list"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                if item["status"].as_str()? == "Trading" {
                    Some(item["symbol"].as_str()?.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(symbols)
    }

    async fn get_ticker_price(&self, symbol: &str) -> Result<f64, ExchangeError> {
        let response = self.client
            .get(&format!("https://api.bybit.com/v5/market/tickers?category=linear&symbol={}", symbol))
            .header("api-key", self.api_key.clone())
            .header("api-secret", self.api_secret.clone())
            .send()
            .await?;

        let data: Value = response.json().await?;
        
        data["result"]["list"][0]["lastPrice"]
            .as_str()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid price format".to_string()))?
            .parse::<f64>()
            .map_err(|_| ExchangeError::InvalidResponse("Invalid price value".to_string()))
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