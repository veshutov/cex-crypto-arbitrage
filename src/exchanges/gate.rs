use async_trait::async_trait;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use hex;
use hmac::{Hmac, Mac};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha512};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ulid::Ulid;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    OrderSide, Position, SubscriptionConfig, TickerData,
};

pub struct GateExchange {
    client: Client,
    config: ExchangeConfig,
    symbol_multipliers: DashMap<String, Decimal>,
}

impl GateExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            client: Client::new(),
            config,
            symbol_multipliers: DashMap::new(),
        }
    }

    async fn upload_tickers(&self) -> Result<(), ExchangeError> {
        let tickers: Vec<TickerData> = self.get_multiplier().await?;

        for ticker in tickers {
            self.symbol_multipliers.insert(from_exchange_symbol(&ticker.symbol), ticker.multiplier);
        }
        Ok(())
    }

    fn generate_signature(
        &self,
        method: &str,
        url: &str,
        query_string: &str,
        payload: &str,
        timestamp: u64,
    ) -> String {
        let payload_hash = hex::encode(Sha512::digest(payload));
        let message = format!(
            "{}\n{}\n{}\n{}\n{}",
            method, url, query_string, payload_hash, timestamp
        );
        let mut mac = Hmac::<Sha512>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    async fn get_multiplier(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let url = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
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
            .map(|item| {
                let symbol = item["name"].as_str().unwrap();

                // Parse bid and ask prices
                let best_bid = item["last_price"].as_str().unwrap().parse::<Decimal>().unwrap();
                let best_ask = item["last_price"].as_str().unwrap().parse::<Decimal>().unwrap();
                let multiplier = item["quanto_multiplier"].as_str().unwrap().parse::<Decimal>().unwrap();

                TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h: Decimal::from(1_000_000),
                    multiplier,
                }
            })
            .collect();

        Ok(tickers)
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
            .map(|item| {
                let symbol = item["contract"].as_str().unwrap();

                // Parse bid and ask prices
                let best_bid = item["highest_bid"].as_str().unwrap().parse::<Decimal>().unwrap();
                let best_ask = item["lowest_ask"].as_str().unwrap().parse::<Decimal>().unwrap();
                let volume_24h = item["volume_24h"].as_str().unwrap().parse::<Decimal>().unwrap();
                let multiplier = self.symbol_multipliers.get(&from_exchange_symbol(symbol)).map(|d| d.to_owned()).unwrap_or(Decimal::ONE);

                TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                    multiplier,
                }
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
        let url: &'static str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut exchange_wr, mut exchange_rc) = ws_stream.split();

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Subscribe to order book for all symbols
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
        let subscribe_msg = serde_json::json!({
            "time": current_time,
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": symbols
        });

        exchange_wr
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;

        

        tokio::spawn(async move {
            'worker: loop {
                if let Some(msg) = exchange_rc.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            let data: Value = serde_json::from_str(&text).unwrap();

                            if let Value::String(_) = data["result"]["s"] {
                                let symbol = from_exchange_symbol(data["result"]["s"].as_str().unwrap());

                                let best_ask_price = data["result"]["a"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_ask_amount = from_exchange_amount(&symbol_multipliers, &symbol, data["result"]["A"].as_i64().unwrap());

                                let best_bid_price = data["result"]["b"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_bid_amount = from_exchange_amount(&symbol_multipliers, &symbol, data["result"]["B"].as_i64().unwrap());

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
                                    println!("Rx dropped, exiting gate worker");
                                    break; // Receiver dropped
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error while receiving data from gate {:?}", e);
                            break;
                        }
                        r => {
                            println!("Error while receiving data from gate {:?}", r);
                            break;
                        }
                    }
                } else {
                    println!("Connection dropped, exiting gate worker");
                    break 'worker;
                }
            }
        });

        Ok(())
    }

    async fn place_order(&self, order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        let url = "/api/v4/futures/usdt/orders";
        let full_url = format!("https://api.gateio.ws{}", url);

        // Convert quantity to positive for buy orders and negative for sell orders
        let size = match order.side {
            OrderSide::Buy => to_exchange_amount(&self.symbol_multipliers, &order.symbol, order.quantity),
            OrderSide::Sell => -to_exchange_amount(&self.symbol_multipliers, &order.symbol, order.quantity),
        };

        let params = serde_json::json!({
            "text": format!("t-{}", order.id),
            "contract": to_exchange_symbol(&order.symbol),
            "size": size,
            "price": "0",
            "tif": "ioc",
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let payload = params.to_string();
        let signature = self.generate_signature("POST", url, "", &payload, timestamp);

        let response = self
            .client
            .post(&full_url)
            .header("KEY", &self.config.api_key)
            .header("Timestamp", timestamp.to_string())
            .header("SIGN", signature)
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let data: Value = response.json().await?;
            println!("Eror response from gate {} {}", status, data);
            return Err(ExchangeError::InvalidResponse(format!(
                "Failed to place order on gate: {}",
                status
            )));
        }

        let data: Value = response.json().await?;

        Ok(OrderResponse {
            id: data["text"]
                .as_str()
                .unwrap()
                .trim_start_matches("t-")
                .to_string(),
            exchange_order_id: data["id"].as_f64().unwrap().to_string(),
        })
    }

    async fn close_position(
        &self,
        position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        let url = "/api/v4/futures/usdt/orders";
        let full_url = format!("https://api.gateio.ws{}", url);

        let params = serde_json::json!({
            "text": format!("t-{}", Ulid::new().to_string()),
            "contract": to_exchange_symbol(&position.symbol),
            "price": "0",
            "size": 0,
            "tif": "ioc",
            "close": true,
            "reduce_only": true,
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let payload = params.to_string();
        let signature = self.generate_signature("POST", url, "", &payload, timestamp);

        let response = self
            .client
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
            id: data["text"]
                .as_str()
                .unwrap()
                .trim_start_matches("t-")
                .to_string(),
            exchange_order_id: data["id"].as_f64().unwrap().to_string(),
        })
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        self.upload_tickers().await?;
        let url = "/api/v4/futures/usdt/positions";
        let params = "holding=true";
        let full_url = format!("https://api.gateio.ws{}?{}", url, params);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let signature = self.generate_signature("GET", url, params, "", timestamp);

        let response = self
            .client
            .get(&full_url)
            .header("KEY", &self.config.api_key)
            .header("Timestamp", timestamp.to_string())
            .header("SIGN", signature)
            .send()
            .await?;

        if !response.status().is_success() {
            let data: Value = response.json().await?;
            println!("Eror response from gate {}", data);
            return Err(ExchangeError::InvalidResponse(format!(
                "Failed to get positions: {}",
                data["message"].as_str().unwrap_or("Unknown error")
            )));
        }

        let data: Value = response.json().await?;

        let positions = data
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .map(|item| {
                let qty = item["size"].as_i64().unwrap();
                let side = if qty > 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                let symbol = from_exchange_symbol(item["contract"].as_str().unwrap());
                Position {
                    size: from_exchange_amount(&self.symbol_multipliers, &symbol, qty.abs()),
                    symbol,
                    entry_price: item["entry_price"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap(),
                    entry_time: item["open_time"].as_i64().unwrap() as u64 * 1000,
                    exchange_name: ExchangeName::Gate,
                    side,
                }
            })
            .collect();

        Ok(positions)
    }
}

fn to_exchange_symbol(symbol: &str) -> String {
    format!("{}_USDT", symbol)
}

fn from_exchange_symbol(symbol: &str) -> String {
    symbol.strip_suffix("_USDT").unwrap().to_string()
}

fn to_exchange_amount(symbol_multipliers: &DashMap<String, Decimal>, symbol: &str, amount: Decimal) -> i32 {
    let multiplier = *symbol_multipliers.get(symbol).unwrap();
    (amount / multiplier).trunc().to_i32().unwrap()
}

fn from_exchange_amount(symbol_multipliers: &DashMap<String, Decimal>, symbol: &str, amount: i64) -> Decimal {
    let multiplier = *symbol_multipliers.get(symbol).unwrap();
    Decimal::from(amount) * multiplier
}
