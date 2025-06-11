use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::{
    sync::mpsc::{self, Receiver},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, SubscriptionConfig, TickerData,
    OrderRequest, OrderResponse, OrderSide, OrderType,
};

pub struct BybitExchange {
    client: Client,
    config: ExchangeConfig,
}

impl BybitExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }

    fn generate_signature(&self, timestamp: u64, recv_window: u64, params: &str) -> String {
        let message = format!("{}{}{}{}", timestamp, self.config.api_key, recv_window, params);
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        BASE64.encode(mac.finalize().into_bytes())
    }

    fn map_symbol(&self, symbol: &str) -> String {
        format!("{}USDT", symbol)
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
                let best_bid = item["bid1Price"].as_str()?.parse::<Decimal>().ok()?;
                let best_ask = item["ask1Price"].as_str()?.parse::<Decimal>().ok()?;
                let volume_24h = item["volume24h"].as_str()?.parse::<Decimal>().ok()?;

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
        let url = "wss://stream.bybit.com/v5/public/linear";
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to order book for all symbols
        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": config.symbols.iter().map(|symbol| format!("orderbook.1.{}", self.map_symbol(symbol))).collect::<Vec<String>>()
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;
        let mut rx = ping().await;

        tokio::spawn(async move {
            loop {
                // Ping
                if let Ok(ping) = rx.try_recv() {
                    write.send(Message::Text(ping.into())).await.expect("Error sending ping to bybit");
                }

                if let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            let data: Value = serde_json::from_str(&text).unwrap();

                            if let Some(asks) = data["data"]["a"].as_array() {
                                if let Some(bids) = data["data"]["b"].as_array() {
                                    let asks: Vec<(Decimal, Decimal)> = asks
                                        .iter()
                                        .filter_map(|item| {
                                            let price = item[0].as_str()?.parse::<Decimal>().ok()?;
                                            let size = item[1].as_str()?.parse::<Decimal>().ok()?;
                                            Some((price, size))
                                        })
                                        .collect();

                                    let bids: Vec<(Decimal, Decimal)> = bids
                                        .iter()
                                        .filter_map(|item| {
                                            let price = item[0].as_str()?.parse::<Decimal>().ok()?;
                                            let size = item[1].as_str()?.parse::<Decimal>().ok()?;
                                            Some((price, size))
                                        })
                                        .collect();

                                    let timestamp = data["ts"].as_i64().unwrap();
                                    let symbol = data["topic"]
                                        .as_str()
                                        .unwrap()
                                        .split('.')
                                        .nth(2)
                                        .unwrap()
                                        .replace("USDT", "");

                                    let orderbook: OrderBook = OrderBook {
                                        symbol,
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

    async fn place_order(&self, order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api.bybit.com/v5/order/create";
        
        let side = match order.side {
            OrderSide::Buy => "Buy",
            OrderSide::Sell => "Sell",
        };

        let order_type = match order.order_type {
            OrderType::Market => "Market",
            OrderType::Limit => "Limit",
        };

        let mut params = serde_json::json!({
            "category": "linear",
            "symbol": self.map_symbol(&order.symbol),
            "side": side,
            "orderType": order_type,
            "qty": order.quantity.to_string(),
        });

        if let Some(price) = order.price {
            params["price"] = serde_json::Value::String(price.to_string());
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let recv_window = 5000;
        let signature = self.generate_signature(timestamp, recv_window, &params.to_string());

        let response = self.client
            .post(url)
            .header("X-BAPI-API-KEY", &self.config.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-RECV-WINDOW", recv_window.to_string())
            .json(&params)
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["retCode"].as_i64().unwrap_or(1) != 0 {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["retMsg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let result = &data["result"];
        Ok(OrderResponse {
            order_id: result["orderId"].as_str().unwrap_or_default().to_string(),
            symbol: order.symbol,
            side: order.side,
            order_type: order.order_type,
            quantity: order.quantity,
            price: order.price,
            status: result["orderStatus"].as_str().unwrap_or_default().to_string(),
        })
    }

    async fn close_position(&self, symbol: &str, side: OrderSide) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api.bybit.com/v5/order/create";
        
        let order_side = match side {
            OrderSide::Buy => "Sell", // If we're long, we need to sell to close
            OrderSide::Sell => "Buy", // If we're short, we need to buy to close
        };

        let params = serde_json::json!({
            "category": "linear",
            "symbol": self.map_symbol(symbol),
            "side": order_side,
            "orderType": "Market",
            "positionIdx": 0, // 0: One-Way Mode, 1: Buy side, 2: Sell side
            "reduceOnly": true,
            "closePosition": true,
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let recv_window = 5000;
        let signature = self.generate_signature(timestamp, recv_window, &params.to_string());

        let response = self.client
            .post(url)
            .header("X-BAPI-API-KEY", &self.config.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-RECV-WINDOW", recv_window.to_string())
            .json(&params)
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["retCode"].as_i64().unwrap_or(1) != 0 {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["retMsg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let result = &data["result"];
        Ok(OrderResponse {
            order_id: result["orderId"].as_str().unwrap_or_default().to_string(),
            symbol: symbol.to_string(),
            side,
            order_type: OrderType::Market,
            quantity: Decimal::ZERO, // For close position, quantity is not relevant
            price: None,
            status: result["orderStatus"].as_str().unwrap_or_default().to_string(),
        })
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
