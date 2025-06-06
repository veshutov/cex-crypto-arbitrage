use super::{Exchange, ExchangeError, ExchangeFee, OrderType};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;

pub struct KuCoinExchange {
    client: Client,
    api_key: String,
    api_secret: String,
    taker_fee: f64,
    maker_fee: f64,
}

impl KuCoinExchange {
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
impl Exchange for KuCoinExchange {
    fn name(&self) -> &'static str {
        "kucoin"
    }

    async fn get_futures_tickers(&self) -> Result<Vec<String>, ExchangeError> {
        let response = self.client
            .get("https://api-futures.kucoin.com/api/v1/contracts/active")
            .send()
            .await?;

        let data: Value = response.json().await?;
        
        let symbols = data["data"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                if item["isActive"].as_bool()? {
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
            .get(&format!("https://api-futures.kucoin.com/api/v1/ticker?symbol={}", symbol))
            .header("KC-API-KEY", self.api_key.clone())
            .header("KC-API-SECRET", self.api_secret.clone())
            .send()
            .await?;

        let data: Value = response.json().await?;
        
        data["data"]["price"]
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