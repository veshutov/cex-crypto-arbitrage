use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::{
    Exchange, ExchangeError, ExchangeFee, ExchangeName, OrderBookData, OrderBookDataType,
    TickerData,
};

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

    async fn subscribe_orderbook<C, Fut>(
        &self,
        symbol: String,
        mut callback: C,
    ) -> Result<(), ExchangeError>
    where
        C: FnMut(OrderBookData) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let url = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to order book
        let subscribe_msg = serde_json::json!({
            "time": chrono::Utc::now().timestamp(),
            "channel": "futures.obu",
            "event": "subscribe",
            "payload": [format!("ob.{}.50", symbol)]
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .unwrap();

        tokio::spawn(async move {
            loop {
                if let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            let data: Value = serde_json::from_str(&text).unwrap();
                            if let Value::String(_) = data["result"]["s"] {
                                let asks: Vec<(f64, f64)> = data["result"]["a"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .filter_map(|item| {
                                        let price = item[0].as_str()?.parse::<f64>().ok()?;
                                        let size = item[1].as_str()?.parse::<f64>().ok()?;
                                        Some((price, size))
                                    })
                                    .collect();

                                let bids: Vec<(f64, f64)> = data["result"]["b"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .filter_map(|item| {
                                        let price = item[0].as_str()?.parse::<f64>().ok()?;
                                        let size = item[1].as_str()?.parse::<f64>().ok()?;
                                        Some((price, size))
                                    })
                                    .collect();

                                let s = data["result"]["s"]
                                    .as_str()
                                    .unwrap()
                                    .split(".")
                                    .collect::<Vec<_>>()[1]
                                    .to_string();

                                let orderbook: OrderBookData = OrderBookData {
                                    symbol: s,
                                    bids,
                                    asks,
                                    data_type: OrderBookDataType::Snapshot,
                                };
                                callback(orderbook).await;
                            }
                        }
                        Err(e) => {
                            println!("{:?}", e);
                            break;
                        }
                        _ => {
                            println!("ELSE");
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }
}
