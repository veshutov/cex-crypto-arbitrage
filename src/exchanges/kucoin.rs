use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use tokio::{
    sync::mpsc::{self, Receiver},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::exchanges::{
    Exchange, ExchangeError, ExchangeFee, ExchangeName, OrderBookData, OrderBookDataType,
    TickerData,
};

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
    fn name(&self) -> ExchangeName {
        ExchangeName::Kucoin
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
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
                    volume_24h: 10_000_000.0,
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
        let token_url = "https://api-futures.kucoin.com/api/v1/bullet-public";
        let response = self.client.post(token_url).send().await?;
        let data: Value = response.json().await?;
        let token: &str = data["data"]["token"].as_str().unwrap();
        let ws_endpoint = data["data"]["instanceServers"][0]["endpoint"]
            .as_str()
            .unwrap();
        let ping_interval_ms = data["data"]["instanceServers"][0]["pingInterval"]
            .as_i64()
            .unwrap();

        let (ws_stream, _) = connect_async(format!("{}?token={}", ws_endpoint, token))
            .await
            .expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to order book
        let subscribe_msg = serde_json::json!({
          "id": 200,
          "type": "subscribe",
          "topic": format!("/contractMarket/tickerV2:{}", symbol),
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .unwrap();
        let mut rx = ping(ping_interval_ms as u64).await;

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
                            
                            if let Value::Object(_) = data["data"] {
                                let ask_price = data["data"]["bestAskPrice"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<f64>()
                                    .unwrap();
                                let ask_amount = data["data"]["bestAskSize"].as_f64().unwrap();

                                let bid_price = data["data"]["bestBidPrice"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<f64>()
                                    .unwrap();
                                let bid_amount = data["data"]["bestBidSize"].as_f64().unwrap();

                                let s = data["data"]["symbol"].as_str().unwrap().to_string();

                                let orderbook: OrderBookData = OrderBookData {
                                    symbol: s,
                                    bids: vec![(bid_price, bid_amount)],
                                    asks: vec![(ask_price, ask_amount)],
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
                        },
                    }
                }
            }
        });

        Ok(())
    }
}

pub async fn ping(ping_interval_ms: u64) -> Receiver<String> {
    let (tx, rx) = mpsc::channel(100);
    let mut id = 1;
    tokio::spawn(async move {
        loop {
            let result = tx.send(format!("{{\"id\":\"{}\",\"type\":\"ping\"}}", id)).await;
            if result.is_err() {
                break;
            }
            id += 1;
            sleep(Duration::from_millis(ping_interval_ms)).await;
        }
    });
    rx
}
