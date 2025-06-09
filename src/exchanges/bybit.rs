use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use tokio::{
    sync::{mpsc::{self, Receiver}, Mutex},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::{self, Message}};

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBookData, SubscriptionConfig, TickerData,
};

pub struct BybitExchange {
    client: Client,
    api_key: String,
    api_secret: String,
    taker_fee: f64,
    maker_fee: f64,
    ws_state: Mutex<Option<(futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, tungstenite::Message>, futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>)>>,
}

impl BybitExchange {
    pub fn new(api_key: String, api_secret: String, taker_fee: f64, maker_fee: f64) -> Self {
        Self {
            client: Client::new(),
            api_key,
            api_secret,
            taker_fee,
            maker_fee,
            ws_state: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Exchange for BybitExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Bybit
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://api.bybit.com/v5/market/tickers?category=linear")
            .send()
            .await?;

        let data: Value = response.json().await?;

        // Check if the response is successful
        if data["retCode"].as_i64().unwrap_or(1) != 0 {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["retMsg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let tickers = data["result"]["list"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["symbol"].as_str()?;

                // Parse bid and ask prices
                let best_bid = item["bid1Price"].as_str()?.parse::<f64>().ok()?;
                let best_ask = item["ask1Price"].as_str()?.parse::<f64>().ok()?;
                let volume_24h = item["volume24h"].as_str()?.parse::<f64>().ok()?;

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
        let url = "wss://stream.bybit.com/v5/public/linear";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (write, read) = ws_stream.split();
        
        let mut ws_state = self.ws_state.lock().await;
        *ws_state = Some((write, read));
        let (mut write, mut read) = ws_state.take().unwrap();

        let symbol = config.symbols[0].to_owned();
        // Subscribe to order book
        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("orderbook.1.{}USDT", symbol)]
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .unwrap();
        let mut rx = ping().await;

        tokio::spawn(async move {
            loop {
                // Ping
                if let Ok(ping) = rx.try_recv() {
                    write.send(Message::Text(ping.into())).await.unwrap();
                }

                if let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            let data: Value = serde_json::from_str(&text).unwrap();

                            if let Some(asks) = data["data"]["a"].as_array() {
                                if let Some(bids) = data["data"]["b"].as_array() {
                                    let asks: Vec<(f64, f64)> = asks
                                        .iter()
                                        .filter_map(|item| {
                                            let price = item[0].as_str()?.parse::<f64>().ok()?;
                                            let size = item[1].as_str()?.parse::<f64>().ok()?;
                                            Some((price, size))
                                        })
                                        .collect();

                                    let bids: Vec<(f64, f64)> = bids
                                        .iter()
                                        .filter_map(|item| {
                                            let price = item[0].as_str()?.parse::<f64>().ok()?;
                                            let size = item[1].as_str()?.parse::<f64>().ok()?;
                                            Some((price, size))
                                        })
                                        .collect();

                                    let timestamp = data["ts"].as_i64().unwrap();

                                    let orderbook: OrderBookData = OrderBookData {
                                        symbol: symbol.clone(),
                                        best_ask_price: asks[0].0,
                                        best_ask_amount: asks[0].1,
                                        best_bid_price: bids[0].0,
                                        best_bid_amount: bids[0].1,
                                        timestamp: timestamp as u64,
                                        exchange_name: ExchangeName::Bybit,
                                    };
                                    if sender.send(orderbook).is_err() {
                                        break; // Receiver dropped
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error while recieving data from bybit {:?}", e);
                            break;
                        }
                        _ => {
                            println!("Error while recieving data from bybit");
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

async fn ping() -> Receiver<&'static str> {
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(async move {
        loop {
            let result = tx.send("{\"op\":\"ping\"}").await;
            if result.is_err() {
                break;
            }
            sleep(Duration::from_secs(20)).await;
        }
    });
    rx
}
