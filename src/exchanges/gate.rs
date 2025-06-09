use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBookData, SubscriptionConfig,
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

    fn config(&self) -> ExchangeConfig {
        ExchangeConfig {
            maker_fee: self.maker_fee,
            taker_fee: self.taker_fee,
        }
    }

    async fn subscribe_orderbook(
        &mut self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBookData>,
    ) -> Result<(), ExchangeError> {
        let url = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        let symbol = config.symbols[0].to_owned();
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Subscribe to order book
        let subscribe_msg = serde_json::json!({
            "time": current_time,
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": [symbol.clone() + "_USDT"]
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
                                let best_ask_price = data["result"]["a"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<f64>()
                                    .unwrap();
                                let best_ask_amount = data["result"]["A"].as_f64().unwrap();

                                let best_bid_price = data["result"]["b"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<f64>()
                                    .unwrap();
                                let best_bid_amount = data["result"]["B"].as_f64().unwrap();

                                let timestamp = data["result"]["t"].as_i64().unwrap() as u64;

                                let orderbook: OrderBookData = OrderBookData {
                                    symbol: symbol.clone(),
                                    best_ask_amount,
                                    best_ask_price,
                                    best_bid_amount,
                                    best_bid_price,
                                    timestamp,
                                    exchange_name: ExchangeName::Gate,
                                };
                                if sender.send(orderbook).is_err() {
                                    break; // Receiver dropped
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error while recieving data from gate {:?}", e);
                            break;
                        }
                        _ => {
                            println!("Error while recieving data from gate");
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }
}
