use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures::{SinkExt, StreamExt};
use hex;
use hmac::{Hmac, Mac};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ulid::Ulid;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    OrderSide, Position, SubscriptionConfig, TickerData,
};

pub struct BingxExchange {
    config: ExchangeConfig,
    client: Client,
}

impl BingxExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    fn generate_signature(&self, params: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(params.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn map_to_exchange_symbol(&self, symbol: &str) -> String {
        format!("{}-USDT", symbol)
    }

    fn map_from_exchange_symbol(&self, symbol: &str) -> String {
        symbol.strip_suffix("-USDT").unwrap().to_string()
    }

    fn prepare_params(&self, mut params: HashMap<String, String>) -> String {
        // Add timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        params.insert("timestamp".to_string(), timestamp);

        // Sort parameters alphabetically
        let mut sorted_keys: Vec<_> = params.keys().collect();
        sorted_keys.sort();

        // Join parameters with &
        let params_str = sorted_keys
            .iter()
            .map(|key| format!("{}={}", key, params.get(*key).unwrap()))
            .collect::<Vec<String>>()
            .join("&");

        params_str
    }

    async fn send_request(
        &self,
        method: &str,
        path: &str,
        params: &str,
    ) -> Result<Value, ExchangeError> {
        let signature = self.generate_signature(params);
        let url = format!(
            "https://open-api.bingx.com{}?{}&signature={}",
            path, params, signature
        );

        let response = self
            .client
            .request(
                match method {
                    "GET" => reqwest::Method::GET,
                    "POST" => reqwest::Method::POST,
                    _ => reqwest::Method::GET,
                },
                &url,
            )
            .header("X-BX-APIKEY", &self.config.api_key)
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["code"].as_i64().unwrap() != 0 {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        Ok(data)
    }
}

#[async_trait]
impl Exchange for BingxExchange {
    fn name(&self) -> ExchangeName {
        ExchangeName::Bingx
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let params = HashMap::new();
        let params_str = self.prepare_params(params);
        let data = self
            .send_request("GET", "/openApi/swap/v1/ticker/price", &params_str)
            .await?;

        let tickers = data["data"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["symbol"].as_str()?;
                let best_bid = item["price"].as_str()?.parse::<Decimal>().ok()?;
                let best_ask = item["price"].as_str()?.parse::<Decimal>().ok()?;
                let volume_24h = Decimal::from(1000001);

                if !symbol.contains("USDT") {
                    return None;
                }

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

    async fn subscribe_orderbook(
        &self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        let url = "wss://open-api-swap.bingx.com/swap-market";
        let (ws_stream, _) = connect_async(url).await?;
        let (mut exchange_wr, mut exchange_rc) = ws_stream.split();

        // Subscribe to order book for each symbol
        let symbol_whitelist = get_symbols_whitelist();
        let symbols = config
            .symbols
            .iter()
            .map(|symbol| self.map_to_exchange_symbol(symbol))
            .filter(|s| symbol_whitelist.contains(s))
            .collect::<Vec<String>>();

        if symbols.is_empty() {
            return Ok(());
        }

        for symbol in symbols {
            let subscribe_msg = serde_json::json!({
                "id": Ulid::new().to_string(),
                "reqType": "sub",
                "dataType": format!("{}@depth5@100ms", symbol),
            });

            exchange_wr
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await?;
        }

        tokio::spawn(async move {
            'worker: loop {
                if let Some(msg) = exchange_rc.next().await {
                    match msg {
                        Ok(Message::Binary(data)) => {
                            // Decompress GZIP data
                            let mut decoder = GzDecoder::new(&data[..]);
                            let mut decompressed = String::new();
                            if let Err(e) = decoder.read_to_string(&mut decompressed) {
                                println!("Error decompressing data: {:?}", e);
                                break 'worker;
                            }

                            if decompressed == "Ping" {
                                match exchange_wr.send(Message::Text("Pong".into())).await {
                                    Err(e) => {
                                        println!("Error while sending pong to BingX {:?}", e);
                                        break 'worker;
                                    }
                                    _ => continue,
                                }
                            }

                            let data: Value = match serde_json::from_str(&decompressed) {
                                Ok(d) => d,
                                Err(e) => {
                                    println!("Error parsing BingX JSON {}: {:?}", &decompressed, e);
                                    break 'worker;
                                }
                            };

                            if let Some(asks) = data["data"]["asks"].as_array() {
                                if let Some(bids) = data["data"]["bids"].as_array() {
                                    let symbol = data["dataType"]
                                        .as_str()
                                        .unwrap()
                                        .split('-')
                                        .nth(0)
                                        .unwrap()
                                        .to_string();
                                    let best_bid = bids.first().unwrap();
                                    let best_bid_price =
                                        best_bid[0].as_str().unwrap().parse::<Decimal>().unwrap();
                                    let best_bid_amount =
                                        best_bid[1].as_str().unwrap().parse::<Decimal>().unwrap();

                                    let best_ask = asks.first().unwrap();
                                    let best_ask_price =
                                        best_ask[0].as_str().unwrap().parse::<Decimal>().unwrap();
                                    let best_ask_amount =
                                        best_ask[1].as_str().unwrap().parse::<Decimal>().unwrap();

                                    let timestamp = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis()
                                        as u64;

                                    let orderbook = OrderBook {
                                        symbol,
                                        best_bid_amount,
                                        best_bid_price,
                                        best_ask_price,
                                        best_ask_amount,
                                        timestamp,
                                        exchange_name: ExchangeName::Bingx,
                                    };

                                    if sender.send(orderbook).is_err() {
                                        break 'worker;
                                    }
                                }
                            }
                        }
                        Ok(Message::Text(text)) => {
                            println!("Received text message: {}", text);
                        }
                        Err(e) => {
                            println!("Error while receiving data from BingX {:?}", e);
                            break 'worker;
                        }
                        r => {
                            println!("Error while receiving data from BingX {:?}", r);
                            break 'worker;
                        }
                    }
                } else {
                    println!("Exiting bingx worker");
                    break 'worker;
                }
            }
        });

        Ok(())
    }

    async fn place_order(&self, order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        let side = match order.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let mut params = HashMap::new();
        params.insert("clientOrderId".to_string(), order.id.to_string());
        params.insert(
            "symbol".to_string(),
            self.map_to_exchange_symbol(&order.symbol),
        );
        params.insert("positionSide".to_string(), "BOTH".to_string());
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), "MARKET".to_string());
        params.insert("quantity".to_string(), order.quantity.to_string());

        let params_str = self.prepare_params(params);
        let data = self
            .send_request("POST", "/openApi/swap/v2/trade/order", &params_str)
            .await?;

        println!("response - {:?}", data);

        Ok(OrderResponse {
            id: order.id,
            exchange_order_id: data["data"]["order"]["orderId"]
                .as_i64()
                .unwrap()
                .to_string(),
        })
    }

    async fn close_position(&self, position: &Position) -> Result<OrderResponse, ExchangeError> {
        let side = match position.side {
            OrderSide::Buy => "SELL",
            OrderSide::Sell => "BUY",
        };

        let mut params = HashMap::new();
        params.insert("clientOrderId".to_string(), Ulid::new().to_string());
        params.insert(
            "symbol".to_string(),
            self.map_to_exchange_symbol(&position.symbol),
        );
        params.insert("positionSide".to_string(), "BOTH".to_string());
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), "MARKET".to_string());
        params.insert("reduceOnly".to_string(), "true".to_string());
        params.insert("quantity".to_string(), position.size.to_string());
        params.insert("closePosition".to_string(), "true".to_string());

        let params_str = self.prepare_params(params);
        let data = self
            .send_request("POST", "/openApi/swap/v2/trade/order", &params_str)
            .await?;

        println!("response - {:?}", data);

        Ok(OrderResponse {
            id: data["data"]["order"]["clientOrderID"]
                .as_str()
                .unwrap()
                .to_string(),
            exchange_order_id: data["data"]["order"]["orderId"]
                .as_i64()
                .unwrap()
                .to_string(),
        })
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        let params = self.prepare_params(HashMap::new());
        let data = self
            .send_request("GET", "/openApi/swap/v2/user/positions", &params)
            .await?;

        let positions = data["data"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .map(|item| {
                let symbol = item["symbol"].as_str().unwrap();
                let size = item["positionAmt"]
                    .as_str()
                    .unwrap()
                    .parse::<f32>()
                    .unwrap() as i32;
                let position_side = item["positionSide"].as_str().unwrap();
                let side = if position_side == "LONG" {
                    OrderSide::Buy
                } else if position_side == "SHORT" {
                    OrderSide::Sell
                } else {
                    panic!("uknown position side");
                };

                Position {
                    symbol: self.map_from_exchange_symbol(symbol),
                    size,
                    entry_price: item["avgPrice"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap(),
                    entry_time: item["updateTime"].as_i64().unwrap() as u64,
                    exchange_name: ExchangeName::Bingx,
                    side,
                }
            })
            .collect();

        Ok(positions)
    }
}

fn get_symbols_whitelist() -> HashSet<String> {
    HashSet::from([
        "RDO-USDT".to_string(),
        "AXL-USDT".to_string(),
        "XEM-USDT".to_string(),
        "TGT-USDT".to_string(),
    ])
}
