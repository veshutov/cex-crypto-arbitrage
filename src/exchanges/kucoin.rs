use super::{Exchange, ExchangeError, ExchangeFee, OrderType, TickerData};
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

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self.client
            .get("https://api-futures.kucoin.com/api/v1/allTickers")
            .send()
            .await?;

        let data: Value = response.json().await?;
        
        // Check if the response is successful
        if data["code"].as_str().unwrap_or("") != "200000" {
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
                let symbol = item["symbol"].as_str()?;
                
                // Parse bid and ask prices
                let best_bid = item["bestBidPrice"].as_str()?.parse::<f64>().ok()?;
                let best_ask = item["bestAskPrice"].as_str()?.parse::<f64>().ok()?;

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                })
            })
            .collect();

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