use std::time::Duration;

use async_trait::async_trait;
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

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    OrderSide, SubscriptionConfig, TickerData,
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
        let message = format!(
            "{}{}{}{}",
            timestamp, self.config.api_key, recv_window, params
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        hex::encode(mac.finalize().into_bytes())
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
        let (mut exchange_wr, mut exchange_rc) = ws_stream.split();

        // Subscribe to order book for all symbols
        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": config.symbols.iter().map(|symbol| format!("orderbook.1.{}", self.map_symbol(symbol))).collect::<Vec<String>>()
        });

        exchange_wr
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;
        let mut ping_rc: Receiver<&'static str> = ping().await;

        tokio::spawn(async move {
            loop {
                // Ping
                if let Ok(ping) = ping_rc.try_recv() {
                    exchange_wr
                        .send(Message::Text(ping.into()))
                        .await
                        .expect("Error sending ping to bybit");
                }

                if let Some(msg) = exchange_rc.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            let data: Value = serde_json::from_str(&text).unwrap();

                            if let Some(asks) = data["data"]["a"].as_array() {
                                if let Some(bids) = data["data"]["b"].as_array() {
                                    let asks: Vec<(Decimal, Decimal)> = asks
                                        .iter()
                                        .filter_map(|item| {
                                            let price =
                                                item[0].as_str()?.parse::<Decimal>().ok()?;
                                            let size = item[1].as_str()?.parse::<Decimal>().ok()?;
                                            Some((price, size))
                                        })
                                        .collect();

                                    let bids: Vec<(Decimal, Decimal)> = bids
                                        .iter()
                                        .filter_map(|item| {
                                            let price =
                                                item[0].as_str()?.parse::<Decimal>().ok()?;
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

        let params = serde_json::json!({
            "orderLinkId": order.id,
            "category": "linear",
            "symbol": self.map_symbol(&order.symbol),
            "side": side,
            "orderType": "Market",
            "qty": order.quantity.to_string(),
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let recv_window = 5000;
        let signature = self.generate_signature(timestamp, recv_window, &params.to_string());

        let response = self
            .client
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

        Ok(OrderResponse {
            id: order.id,
            exchange_order_id: data["result"]["orderId"].as_str().unwrap().to_string(),
        })
    }

    async fn close_position(
        &self,
        order_id: &str,
        symbol: &str,
        side: OrderSide,
    ) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api.bybit.com/v5/order/create";

        let order_side = match side {
            OrderSide::Buy => "Sell", // If we're long, we need to sell to close
            OrderSide::Sell => "Buy", // If we're short, we need to buy to close
        };

        let params = serde_json::json!({
            "orderLinkId": order_id,
            "category": "linear",
            "symbol": self.map_symbol(symbol),
            "side": order_side,
            "orderType": "Market",
            "qty": "0",
            "reduceOnly": true,
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let recv_window = 5000;
        let signature = self.generate_signature(timestamp, recv_window, &params.to_string());

        let response = self
            .client
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

        Ok(OrderResponse {
            id: data["result"]["orderLinkId"].as_str().unwrap().to_string(),
            exchange_order_id: data["result"]["orderId"].as_str().unwrap().to_string(),
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
            sleep(Duration::from_secs(15)).await;
        }
    });
    rx
}
