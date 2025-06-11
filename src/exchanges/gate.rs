use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use hmac::{Hmac, Mac};
use sha2::Sha512;
use hex;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, SubscriptionConfig,
    TickerData,
    OrderRequest, OrderResponse, OrderSide, OrderType,
};

pub struct GateExchange {
    client: Client,
    config: ExchangeConfig,
}

impl GateExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }

    fn generate_signature(&self, method: &str, url: &str, query_string: &str, payload: &str, timestamp: u64) -> String {
        let message = format!("{}\n{}\n{}\n{}\n{}", method, url, query_string, payload, timestamp);
        let mut mac = Hmac::<Sha512>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        hex::encode(mac.finalize().into_bytes())
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
                let best_bid = item["highest_bid"].as_str()?.parse::<Decimal>().ok()?;
                let best_ask = item["lowest_ask"].as_str()?.parse::<Decimal>().ok()?;
                let volume_24h = item["volume_24h"].as_str()?.parse::<Decimal>().ok()?;

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
        self.config.clone()
    }

    async fn subscribe_orderbook(
        &mut self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        let url = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Subscribe to order book for all symbols
        let subscribe_msg = serde_json::json!({
            "time": current_time,
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": config.symbols.iter().map(|symbol| symbol.clone() + "_USDT").collect::<Vec<String>>()
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
                                let symbol = data["result"]["s"]
                                    .as_str()
                                    .unwrap()
                                    .replace("_USDT", "");

                                let best_ask_price = data["result"]["a"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_ask_amount = data["result"]["A"].as_i64().unwrap().into();

                                let best_bid_price = data["result"]["b"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_bid_amount = data["result"]["B"].as_i64().unwrap().into();

                                let timestamp = data["result"]["t"].as_i64().unwrap() as u64;

                                let orderbook: OrderBook = OrderBook {
                                    symbol,
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

    async fn place_order(&self, order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        let url = "/api/v4/futures/usdt/orders";
        let full_url = format!("https://api.gateio.ws{}", url);
        
        let order_type = match order.order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
        };

        // Convert quantity to positive for buy orders and negative for sell orders
        let size = match order.side {
            OrderSide::Buy => order.quantity,
            OrderSide::Sell => -order.quantity,
        };

        let mut params = serde_json::json!({
            "contract": format!("{}_USDT", order.symbol),
            "size": size.to_string(),
            "type": order_type,
            "tif": "gtc",
        });

        if let Some(price) = order.price {
            params["price"] = serde_json::Value::String(price.to_string());
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let payload = params.to_string();
        let signature = self.generate_signature("POST", url, "", &payload, timestamp);

        let response = self.client
            .post(&full_url)
            .header("KEY", &self.config.api_key)
            .header("Timestamp", timestamp.to_string())
            .header("SIGN", signature)
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ExchangeError::InvalidResponse(format!(
                "Failed to place order: {}",
                response.status()
            )));
        }

        let data: Value = response.json().await?;

        Ok(OrderResponse {
            order_id: data["id"].as_str().unwrap_or_default().to_string(),
            symbol: order.symbol,
            side: order.side,
            order_type: order.order_type,
            quantity: order.quantity,
            price: order.price,
            status: data["status"].as_str().unwrap_or_default().to_string(),
        })
    }

    async fn close_position(&self, symbol: &str, side: OrderSide) -> Result<OrderResponse, ExchangeError> {
        let url = "/api/v4/futures/usdt/orders";
        let full_url = format!("https://api.gateio.ws{}", url);

        let params = serde_json::json!({
            "contract": format!("{}_USDT", symbol),
            "type": "market",
            "tif": "gtc",
            "close": true,
            "reduce_only": true,
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let payload = params.to_string();
        let signature = self.generate_signature("POST", url, "", &payload, timestamp);

        let response = self.client
            .post(&full_url)
            .header("KEY", &self.config.api_key)
            .header("Timestamp", timestamp.to_string())
            .header("SIGN", signature)
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ExchangeError::InvalidResponse(format!(
                "Failed to close position: {}",
                response.status()
            )));
        }

        let data: Value = response.json().await?;

        Ok(OrderResponse {
            order_id: data["id"].as_str().unwrap_or_default().to_string(),
            symbol: symbol.to_string(),
            side,
            order_type: OrderType::Market,
            quantity: Decimal::ZERO, // For close position, quantity is not relevant
            price: None,
            status: data["status"].as_str().unwrap_or_default().to_string(),
        })
    }
}
