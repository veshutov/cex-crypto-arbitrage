use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tokio::{
    sync::mpsc::{self, Receiver},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ulid::Ulid;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    OrderSide, Position, SubscriptionConfig, TickerData,
};

pub struct KucoinExchange {
    client: Client,
    config: ExchangeConfig,
    symbol_multipliers: DashMap<String, Decimal>,
}

impl KucoinExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            client: Client::new(),
            config,
            symbol_multipliers: DashMap::new(),
        }
    }

    async fn upload_tickers(&self) -> Result<(), ExchangeError> {
        let tickers = self.get_multiplier().await?;

        for ticker in tickers {
            self.symbol_multipliers
                .insert(from_exchange_symbol(&ticker.symbol), ticker.multiplier);
        }
        Ok(())
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

    async fn get_multiplier(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://api-futures.kucoin.com/api/v1/contracts/active")
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
                let symbol = item["symbol"].as_str().unwrap();

                if !symbol.contains("USDT") {
                    return None;
                }

                // Parse bid and ask prices
                let best_bid = Decimal::try_from(item["lastTradePrice"].as_f64().unwrap()).unwrap();
                let best_ask = Decimal::try_from(item["lastTradePrice"].as_f64().unwrap()).unwrap();
                let volume_24h = item["volumeOf24h"].to_string().parse::<Decimal>().unwrap();
                let multiplier = item["multiplier"].to_string().parse::<Decimal>().unwrap();

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                    multiplier,
                })
            })
            .collect();

        Ok(tickers)
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
                let symbol = item["symbol"].as_str().unwrap();

                if !symbol.contains("USDT") {
                    return None;
                }

                // Parse bid and ask prices
                let best_bid = Decimal::try_from(item["bestBidPrice"].as_str().unwrap()).unwrap();
                let best_ask = Decimal::try_from(item["bestAskPrice"].as_str().unwrap()).unwrap();
                let volume_24h = Decimal::from_i32(1_000_000).unwrap();
                let multiplier = self.symbol_multipliers.get(&from_exchange_symbol(symbol)).map(|d| d.to_owned()).unwrap_or(Decimal::ONE);

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                    multiplier,
                })
            })
            .collect();

        Ok(tickers)
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn subscribe_orderbook(
        &self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        self.upload_tickers().await?;
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

        // Wait for welcome message
        while let Some(msg) = exchange_rc.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let data: Value = serde_json::from_str(&text).unwrap();
                    if data["type"].as_str() == Some("welcome") {
                        break;
                    }
                }
                Err(e) => {
                    return Err(ExchangeError::InvalidResponse(format!(
                        "Error while waiting for kucoin welcome message: {:?}",
                        e
                    )));
                }
                _ => {
                    return Err(ExchangeError::InvalidResponse(
                        "Unexpected message type while waiting kucoin for welcome".to_string(),
                    ));
                }
            }
        }

        // Subscribe to order book for each symbol with a unique ID
        let symbol_multipliers = self.symbol_multipliers.clone();
        let symbols = config
            .symbols
            .iter()
            .filter(|s| symbol_multipliers.contains_key(*s))
            .map(|symbol| to_exchange_symbol(symbol))
            .collect::<Vec<String>>();

        if symbols.is_empty() {
            return Ok(());
        }
        for symbol in symbols.iter() {
            let subscribe_msg = serde_json::json!({
                "id": Ulid::new().to_string(),
                "type": "subscribe",
                "topic": format!("/contractMarket/tickerV2:{}", symbol)
            });

            exchange_wr
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await?;
        }

        let mut ping_rc = ping(ping_interval_ms as u64).await;

        tokio::spawn(async move {
            'worker: loop {
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
                                let best_ask_amount = from_exchange_amount(
                                    &symbol_multipliers,
                                    &symbol,
                                    data["data"]["bestAskSize"].as_i64().unwrap(),
                                );

                                let best_bid_price = data["data"]["bestBidPrice"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_bid_amount = from_exchange_amount(
                                    &symbol_multipliers,
                                    &symbol,
                                    data["data"]["bestBidSize"].as_i64().unwrap(),
                                );

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
                                    println!("Rx dropped, exiting kucoin worker");
                                    break; // Receiver dropped
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error while receiving data from kucoin {:?}", e);
                            ping_rc.close();
                            break;
                        }
                        _ => {
                            println!("Error while receiving data from kucoin");
                            ping_rc.close();
                            break;
                        }
                    }
                } else {
                    println!("Exiting kucoin worker");
                    break 'worker;
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
            "symbol": to_exchange_symbol(&order.symbol),
            "side": side,
            "marginMode": "CROSS",
            "type": "market",
            "size": to_exchange_amount(&self.symbol_multipliers, &order.symbol, order.quantity),
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
            println!("Eror response from kucoin {}", data);
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

    async fn close_position(&self, position: &Position) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api-futures.kucoin.com/api/v1/orders";
        let endpoint = "/api/v1/orders";

        let params = serde_json::json!({
            "clientOid": Ulid::new().to_string(),
            "symbol": to_exchange_symbol(&position.symbol),
            "type": "market",
            "marginMode": "CROSS",
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

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        self.upload_tickers().await?;
        let url = "https://api-futures.kucoin.com/api/v1/positions";
        let endpoint = "/api/v1/positions";
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let signature = self.generate_signature(timestamp, "GET", endpoint, "");
        let passphrase = self.generate_passphrase();

        let response = self
            .client
            .get(url)
            .header("KC-API-KEY", &self.config.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", timestamp.to_string())
            .header("KC-API-PASSPHRASE", passphrase)
            .header("KC-API-KEY-VERSION", "3")
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["code"].as_str().unwrap_or("") != "200000" {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let positions = data["data"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .map(|item| {
                let qty = item["currentQty"].as_i64().unwrap();
                let side = if qty > 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                let symbol = from_exchange_symbol(item["symbol"].as_str().unwrap());
                Position {
                    size: from_exchange_amount(&self.symbol_multipliers, &symbol, qty.abs()),
                    symbol,
                    entry_price: Decimal::try_from(item["avgEntryPrice"].as_f64().unwrap())
                        .unwrap(),
                    entry_time: item["openingTimestamp"].as_i64().unwrap() as u64,
                    exchange_name: ExchangeName::Kucoin,
                    side,
                }
            })
            .collect();

        Ok(positions)
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

fn to_exchange_amount(
    symbol_multipliers: &DashMap<String, Decimal>,
    symbol: &str,
    amount: Decimal,
) -> i32 {
    let multiplier = *symbol_multipliers.get(symbol).unwrap();
    (amount / multiplier).trunc().to_i32().unwrap()
}

fn from_exchange_amount(
    symbol_multipliers: &DashMap<String, Decimal>,
    symbol: &str,
    amount: i64,
) -> Decimal {
    let multiplier = *symbol_multipliers.get(symbol).unwrap();
    Decimal::from(amount) * multiplier
}

fn to_exchange_symbol(symbol: &str) -> String {
    format!("{}USDTM", symbol)
}

fn from_exchange_symbol(symbol: &str) -> String {
    symbol.strip_suffix("USDTM").unwrap().to_string()
}
