use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::Sha256;
use tokio::{
    sync::mpsc::{self, Receiver},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ulid::Ulid;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    OrderSide, SubscriptionConfig, TickerData,
};

pub struct KucoinExchange {
    client: Client,
    config: ExchangeConfig,
}

impl KucoinExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }

    fn generate_signature(
        &self,
        timestamp: u64,
        method: &str,
        endpoint: &str,
        body: &str,
    ) -> String {
        let message = format!("{}{}{}{}", timestamp, method, endpoint, body);
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        BASE64.encode(mac.finalize().into_bytes())
    }

    fn generate_passphrase(&self) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(self.config.api_passphrase.as_ref().unwrap().as_bytes());
        BASE64.encode(mac.finalize().into_bytes())
    }

    fn map_symbol(&self, symbol: &str) -> String {
        format!("{}USDTM", symbol)
    }
}

#[async_trait]
impl Exchange for KucoinExchange {
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
                let best_bid = item["bestBidPrice"].as_str()?.parse::<Decimal>().ok()?;
                let best_ask = item["bestAskPrice"].as_str()?.parse::<Decimal>().ok()?;

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h: Decimal::from(10_000_000),
                })
            })
            .collect();

        Ok(tickers)
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn subscribe_orderbook(
        &mut self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
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

        let (ws_stream, _) = connect_async(format!("{}?token={}", ws_endpoint, token)).await?;
        let (mut exchange_wr, mut exchange_rc) = ws_stream.split();

        // Subscribe to order book for each symbol with a unique ID
        for symbol in config.symbols.iter() {
            let subscribe_msg = serde_json::json!({
                "id": Ulid::new().to_string(),
                "type": "subscribe",
                "topic": format!("/contractMarket/tickerV2:{}", self.map_symbol(symbol))
            });

            exchange_wr
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await?;
        }

        let mut ping_rc = ping(ping_interval_ms as u64).await;

        tokio::spawn(async move {
            loop {
                // Ping
                if let Ok(ping) = ping_rc.try_recv() {
                    exchange_wr
                        .send(Message::Text(ping.into()))
                        .await
                        .expect("Error sending ping to kucoin");
                }

                if let Some(msg) = exchange_rc.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            let data: Value = serde_json::from_str(&text).unwrap();

                            if let Value::Object(_) = data["data"] {
                                let symbol = data["topic"]
                                    .as_str()
                                    .unwrap()
                                    .split(':')
                                    .nth(1)
                                    .unwrap()
                                    .replace("USDTM", "");

                                let best_ask_price = data["data"]["bestAskPrice"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_ask_amount =
                                    data["data"]["bestAskSize"].as_i64().unwrap().into();

                                let best_bid_price = data["data"]["bestBidPrice"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_bid_amount =
                                    data["data"]["bestBidSize"].as_i64().unwrap().into();

                                let timestamp =
                                    data["data"]["ts"].as_i64().unwrap() as u64 / 1_000_000;

                                let orderbook: OrderBook = OrderBook {
                                    symbol,
                                    best_ask_amount,
                                    best_ask_price,
                                    best_bid_amount,
                                    best_bid_price,
                                    timestamp,
                                    exchange_name: ExchangeName::Kucoin,
                                };
                                if sender.send(orderbook).is_err() {
                                    break; // Receiver dropped
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error while recieving data from kucoin {:?}", e);
                            ping_rc.close();
                            break;
                        }
                        _ => {
                            println!("Error while recieving data from kucoin");
                            ping_rc.close();
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn place_order(&self, order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api-futures.kucoin.com/api/v1/orders";
        let endpoint = "/api/v1/orders";

        let side = match order.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let params = serde_json::json!({
            "clientOid": order.id,
            "symbol": self.map_symbol(&order.symbol),
            "side": side,
            "type": "market",
            "size": order.quantity,
            "leverage": 1,
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let body = params.to_string();
        let signature = self.generate_signature(timestamp, "POST", endpoint, &body);
        let passphrase = self.generate_passphrase();

        let response = self
            .client
            .post(url)
            .header("KC-API-KEY", &self.config.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", timestamp.to_string())
            .header("KC-API-PASSPHRASE", passphrase)
            .header("KC-API-KEY-VERSION", "3")
            .json(&params)
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["code"].as_str().unwrap_or("") != "200000" {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        Ok(OrderResponse {
            id: data["data"]["clientOid"].as_str().unwrap().to_string(),
            exchange_order_id: data["data"]["orderId"].as_str().unwrap().to_string(),
        })
    }

    async fn close_position(
        &self,
        order_id: &str,
        symbol: &str,
        _side: OrderSide,
    ) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api-futures.kucoin.com/api/v1/orders";
        let endpoint = "/api/v1/orders";

        let params = serde_json::json!({
            "clientOid": order_id,
            "symbol": self.map_symbol(symbol),
            "type": "market",
            "closeOrder": true,
            "reduceOnly": true,
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let body = params.to_string();
        let signature = self.generate_signature(timestamp, "POST", endpoint, &body);
        let passphrase = self.generate_passphrase();

        let response = self
            .client
            .post(url)
            .header("KC-API-KEY", &self.config.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", timestamp.to_string())
            .header("KC-API-PASSPHRASE", passphrase)
            .header("KC-API-KEY-VERSION", "3")
            .json(&params)
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["code"].as_str().unwrap_or("") != "200000" {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        Ok(OrderResponse {
            id: data["data"]["clientOid"].as_str().unwrap().to_string(),
            exchange_order_id: data["data"]["orderId"].as_str().unwrap().to_string(),
        })
    }
}

async fn ping(ping_interval_ms: u64) -> Receiver<String> {
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(async move {
        loop {
            let result = tx
                .send(format!("{{\"id\":\"{}\",\"type\":\"ping\"}}", Ulid::new()))
                .await;
            if result.is_err() {
                println!("Kucoin ping error {}", result.err().unwrap());
                break;
            }
            sleep(Duration::from_millis(ping_interval_ms)).await;
        }
    });
    rx
}
