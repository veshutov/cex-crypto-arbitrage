// use async_trait::async_trait;
// use reqwest::Client;
// use serde_json::Value;

// use crate::exchanges::{Exchange, ExchangeError, ExchangeFee, TickerData, ExchangeName};

// pub struct HtxExchange {
//     client: Client,
//     api_key: String,
//     api_secret: String,
//     taker_fee: f64,
//     maker_fee: f64,
// }

// impl HtxExchange {
//     pub fn new(api_key: String, api_secret: String, taker_fee: f64, maker_fee: f64) -> Self {
//         Self {
//             client: Client::new(),
//             api_key,
//             api_secret,
//             taker_fee,
//             maker_fee,
//         }
//     }
// }

// #[async_trait]
// impl Exchange for HtxExchange {
//     fn name(&self) -> ExchangeName {
//         ExchangeName::Htx
//     }

//     async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
//         let url = "https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged?business_type=futures";
//         let response = self.client.get(url).send().await?;
        
//         if !response.status().is_success() {
//             return Err(ExchangeError::InvalidResponse(format!(
//                 "Failed to get tickers: {}",
//                 response.status()
//             )));
//         }

//         let json: Value = response.json().await?;
//         let data = json["ticks"].as_array().ok_or_else(|| {
//             ExchangeError::InvalidResponse("Invalid response format".to_string())
//         })?;

//         let mut tickers = Vec::new();
//         for item in data {
//             let symbol = item["contract_code"]
//                 .as_str()
//                 .ok_or_else(|| ExchangeError::InvalidResponse("Missing symbol".to_string()))?
//                 .to_string();

//             let best_bid = item["bid"][0]
//                 .as_f64()
//                 .ok_or_else(|| ExchangeError::InvalidResponse("Invalid bid price".to_string()))?;

//             let best_ask = item["ask"][0]
//                 .as_f64()
//                 .ok_or_else(|| ExchangeError::InvalidResponse("Invalid ask price".to_string()))?;

//             tickers.push(TickerData {
//                 symbol,
//                 best_bid_price: best_bid,
//                 best_ask_price: best_ask,
//                 volume_24h: 1.0,
//             });
//         }

//         Ok(tickers)
//     }

//     fn get_fees(&self) -> ExchangeFee {
//         ExchangeFee {
//             maker_fee: self.maker_fee,
//             taker_fee: self.taker_fee,
//         }
//     }
// } 