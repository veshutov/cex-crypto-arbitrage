use std::{collections::HashSet, time::Duration};

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
    OrderSide, Position, SubscriptionConfig, TickerData,
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

    fn map_to_exchange_symbol(&self, symbol: &str) -> String {
        format!("{}USDTM", symbol)
    }

    fn map_from_exchange_symbol(&self, symbol: &str) -> String {
        symbol.strip_suffix("USDTM").unwrap().to_string()
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
        &self,
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
                if let Some(ping) = ping_rc.recv().await {
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
            "symbol": self.map_to_exchange_symbol(&order.symbol),
            "side": side,
            "marginMode": "CROSS",
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

    async fn close_position(
        &self,
        position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        let url = "https://api-futures.kucoin.com/api/v1/orders";
        let endpoint = "/api/v1/orders";

        let params = serde_json::json!({
            "clientOid": Ulid::new().to_string(),
            "symbol": self.map_to_exchange_symbol(&position.symbol),
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

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
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
                let qty = item["currentQty"].as_i64().unwrap() as i32;
                let side = if qty > 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                let symbol = item["symbol"].as_str().unwrap();
                Position {
                    symbol: self.map_from_exchange_symbol(symbol),
                    size: qty.abs(),
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

fn get_symbols_whitelist() -> HashSet<String> {
    HashSet::from([
        "ANIMEUSDTM".to_string(),
        "SNXUSDTM".to_string(),
        "OMUSDTM".to_string(),
        "BRETTUSDTM".to_string(),
        "TOKENUSDTM".to_string(),
        "BIGTIMEUSDTM".to_string(),
        "AWEUSDTM".to_string(),
        "ZKUSDTM".to_string(),
        "STEEMUSDTM".to_string(),
        "ULTIUSDTM".to_string(),
        "BRUSDTM".to_string(),
        "FXSUSDTM".to_string(),
        "MEUSDTM".to_string(),
        "FLOWUSDTM".to_string(),
        "HBARUSDTM".to_string(),
        "YGGUSDTM".to_string(),
        "VVVUSDTM".to_string(),
        "SUSHIUSDTM".to_string(),
        "BDXNUSDTM".to_string(),
        "STOUSDTM".to_string(),
        "CYBERUSDTM".to_string(),
        "JASMYUSDTM".to_string(),
        "MLNUSDTM".to_string(),
        "HEIUSDTM".to_string(),
        "GOATUSDTM".to_string(),
        "DUCKUSDTM".to_string(),
        "DUSDTM".to_string(),
        "KSMUSDTM".to_string(),
        "MBOXUSDTM".to_string(),
        "DOGEUSDCM".to_string(),
        "SUPERUSDTM".to_string(),
        "HYPEUSDTM".to_string(),
        "RONUSDTM".to_string(),
        "MYROUSDTM".to_string(),
        "ALGOUSDTM".to_string(),
        "WIFUSDTM".to_string(),
        "IMXUSDTM".to_string(),
        "PUFFERUSDTM".to_string(),
        "SXTUSDTM".to_string(),
        "PHAUSDTM".to_string(),
        "ASRUSDTM".to_string(),
        "MOVRUSDTM".to_string(),
        "SHIBUSDTM".to_string(),
        "OMGUSDTM".to_string(),
        "1000BONKUSDTM".to_string(),
        "OXTUSDTM".to_string(),
        "CETUSUSDTM".to_string(),
        "ORBSUSDTM".to_string(),
        "ORDIUSDTM".to_string(),
        "WOOUSDTM".to_string(),
        "PEPEUSDCM".to_string(),
        "GLMRUSDTM".to_string(),
        "CHRUSDTM".to_string(),
        "NCUSDTM".to_string(),
        "PONKEUSDTM".to_string(),
        "ZECUSDTM".to_string(),
        "DENTUSDTM".to_string(),
        "LAUNCHCOINUSDTM".to_string(),
        "CGPTUSDTM".to_string(),
        "HAEDALUSDTM".to_string(),
        "RAREUSDTM".to_string(),
        "NEIROCTOUSDTM".to_string(),
        "APEUSDTM".to_string(),
        "SCRUSDTM".to_string(),
        "ETHUSDCM".to_string(),
        "ZENUSDTM".to_string(),
        "PEOPLEUSDTM".to_string(),
        "YFIUSDTM".to_string(),
        "IDUSDTM".to_string(),
        "1MBABYDOGEUSDTM".to_string(),
        "AEVOUSDTM".to_string(),
        "COOKIEUSDTM".to_string(),
        "REZUSDTM".to_string(),
        "GALAUSDTM".to_string(),
        "METISUSDTM".to_string(),
        "ANKRUSDTM".to_string(),
        "AERGOUSDTM".to_string(),
        "DODOUSDTM".to_string(),
        "INITUSDTM".to_string(),
        "1000000MOGUSDTM".to_string(),
        "STORJUSDTM".to_string(),
        "MANTAUSDTM".to_string(),
        "UXLINKUSDTM".to_string(),
        "SXPUSDTM".to_string(),
        "TURBOUSDTM".to_string(),
        "SUNDOGUSDTM".to_string(),
        "ICXUSDTM".to_string(),
        "ZILUSDTM".to_string(),
        "LINKUSDTM".to_string(),
        "ACEUSDTM".to_string(),
        "SOLUSDM".to_string(),
        "HOTUSDTM".to_string(),
        "FLOKIUSDTM".to_string(),
        "STRKUSDTM".to_string(),
        "AIUSDTM".to_string(),
        "GUNUSDTM".to_string(),
        "ONEUSDTM".to_string(),
        "DOTUSDM".to_string(),
        "KOMAUSDTM".to_string(),
        "XEMUSDTM".to_string(),
        "DOGSUSDTM".to_string(),
        "KCSUSDTM".to_string(),
        "THETAUSDTM".to_string(),
        "NEARUSDTM".to_string(),
        "AUSDTM".to_string(),
        "BNBUSDTM".to_string(),
        "GTCUSDTM".to_string(),
        "MELANIAUSDTM".to_string(),
        "XRPUSDCM".to_string(),
        "XBTMM25".to_string(),
        "AIXBTUSDTM".to_string(),
        "WLDUSDTM".to_string(),
        "CTCUSDTM".to_string(),
        "BSVUSDTM".to_string(),
        "CKBUSDTM".to_string(),
        "LUNAUSDTM".to_string(),
        "XBTUSDTM".to_string(),
        "GRASSUSDTM".to_string(),
        "PRCLUSDTM".to_string(),
        "KASUSDTM".to_string(),
        "WUSDTM".to_string(),
        "ETHUSDM".to_string(),
        "AXSUSDTM".to_string(),
        "BATUSDTM".to_string(),
        "ATOMUSDTM".to_string(),
        "10000COQUSDTM".to_string(),
        "BOMEUSDTM".to_string(),
        "KAVAUSDTM".to_string(),
        "NFPUSDTM".to_string(),
        "MTLUSDTM".to_string(),
        "ENAUSDTM".to_string(),
        "FORTHUSDTM".to_string(),
        "NKNUSDTM".to_string(),
        "CATSUSDTM".to_string(),
        "BERAUSDTM".to_string(),
        "EPTUSDTM".to_string(),
        "SANDUSDTM".to_string(),
        "LDOUSDTM".to_string(),
        "SUIUSDCM".to_string(),
        "SPXUSDTM".to_string(),
        "AXLUSDTM".to_string(),
        "1000XUSDTM".to_string(),
        "PENDLEUSDTM".to_string(),
        "ICPUSDTM".to_string(),
        "IOTAUSDTM".to_string(),
        "HOMEUSDTM".to_string(),
        "ONDOUSDTM".to_string(),
        "ALUUSDTM".to_string(),
        "FLOCKUSDTM".to_string(),
        "MEMEFIUSDTM".to_string(),
        "OPUSDTM".to_string(),
        "SOLUSDTM".to_string(),
        "HMSTRUSDTM".to_string(),
        "ASTRUSDTM".to_string(),
        "LEVERUSDTM".to_string(),
        "AAVEUSDTM".to_string(),
        "ILVUSDTM".to_string(),
        "ZETAUSDTM".to_string(),
        "CELOUSDTM".to_string(),
        "PROMPTUSDTM".to_string(),
        "NAKAUSDTM".to_string(),
        "ARBUSDTM".to_string(),
        "FUNUSDTM".to_string(),
        "HIFIUSDTM".to_string(),
        "UMAUSDTM".to_string(),
        "1000RATSUSDTM".to_string(),
        "SPELLUSDTM".to_string(),
        "THEUSDTM".to_string(),
        "HUMAUSDTM".to_string(),
        "BANDUSDTM".to_string(),
        "SOLVUSDTM".to_string(),
        "WCTUSDTM".to_string(),
        "ACHUSDTM".to_string(),
        "B2USDTM".to_string(),
        "ADAUSDTM".to_string(),
        "IOUSDTM".to_string(),
        "DEEPUSDTM".to_string(),
        "BLASTUSDTM".to_string(),
        "CRVUSDTM".to_string(),
        "AIOTUSDTM".to_string(),
        "FETUSDTM".to_string(),
        "API3USDTM".to_string(),
        "SUPRAUSDTM".to_string(),
        "GPSUSDTM".to_string(),
        "NILUSDTM".to_string(),
        "VIRTUALUSDTM".to_string(),
        "INJUSDTM".to_string(),
        "CROUSDTM".to_string(),
        "RESOLVUSDTM".to_string(),
        "DOTUSDTM".to_string(),
        "DEGENUSDTM".to_string(),
        "GORKUSDTM".to_string(),
        "XBTUSDM".to_string(),
        "SOPHUSDTM".to_string(),
        "RAYUSDTM".to_string(),
        "ZEREBROUSDTM".to_string(),
        "ONTUSDTM".to_string(),
        "USDCUSDTM".to_string(),
        "RDNTUSDTM".to_string(),
        "CATIUSDTM".to_string(),
        "SIGNUSDTM".to_string(),
        "CHZUSDTM".to_string(),
        "GRIFFAINUSDTM".to_string(),
        "JUPUSDTM".to_string(),
        "NEIROUSDTM".to_string(),
        "SEIUSDTM".to_string(),
        "AGTUSDTM".to_string(),
        "JOEUSDTM".to_string(),
        "LTCUSDTM".to_string(),
        "ETHFIUSDTM".to_string(),
        "PARTIUSDTM".to_string(),
        "RENDERUSDTM".to_string(),
        "AUDIOUSDTM".to_string(),
        "MEWUSDTM".to_string(),
        "SKLUSDTM".to_string(),
        "LAUSDTM".to_string(),
        "ALPINEUSDTM".to_string(),
        "SSVUSDTM".to_string(),
        "SKYAIUSDTM".to_string(),
        "EGLDUSDTM".to_string(),
        "LOOKSUSDTM".to_string(),
        "VINEUSDTM".to_string(),
        "VICUSDTM".to_string(),
        "DOGUSDTM".to_string(),
        "NXPCUSDTM".to_string(),
        "STXUSDTM".to_string(),
        "VETUSDTM".to_string(),
        "ZROUSDTM".to_string(),
        "POLYXUSDTM".to_string(),
        "BUSDTM".to_string(),
        "DOODUSDTM".to_string(),
        "GMTUSDTM".to_string(),
        "ACTSOLUSDTM".to_string(),
        "DUSKUSDTM".to_string(),
        "EDUUSDTM".to_string(),
        "AGIUSDTM".to_string(),
        "HYPERUSDTM".to_string(),
        "ALPHAUSDTM".to_string(),
        "MAJORUSDTM".to_string(),
        "TIAUSDTM".to_string(),
        "CVCUSDTM".to_string(),
        "DOGEUSDTM".to_string(),
        "LISTAUSDTM".to_string(),
        "ORCAUSDTM".to_string(),
        "SAGAUSDTM".to_string(),
        "MAVIAUSDTM".to_string(),
        "COTIUSDTM".to_string(),
        "COMPUSDTM".to_string(),
        "TRBUSDTM".to_string(),
        "XVGUSDTM".to_string(),
        "WAVESUSDTM".to_string(),
        "FLMUSDTM".to_string(),
        "WALUSDTM".to_string(),
        "ZEUSUSDTM".to_string(),
        "NOTUSDTM".to_string(),
        "ATHUSDTM".to_string(),
        "ETHUSDTM".to_string(),
        "GRTUSDTM".to_string(),
        "SIRENUSDTM".to_string(),
        "PEPEUSDTM".to_string(),
        "SFPUSDTM".to_string(),
        "AVAUSDTM".to_string(),
        "QNTUSDTM".to_string(),
        "MORPHOUSDTM".to_string(),
        "TWTUSDTM".to_string(),
        "OMNIUSDTM".to_string(),
        "HOOKUSDTM".to_string(),
        "MKRUSDTM".to_string(),
        "KDAUSDTM".to_string(),
        "VANAUSDTM".to_string(),
        "ARPAUSDTM".to_string(),
        "BCHUSDTM".to_string(),
        "MAVUSDTM".to_string(),
        "BANANAUSDTM".to_string(),
        "XAIUSDTM".to_string(),
        "XRPUSDM".to_string(),
        "TUSDTM".to_string(),
        "AVAAIUSDTM".to_string(),
        "ALCHUSDTM".to_string(),
        "PENGUUSDTM".to_string(),
        "BABYUSDTM".to_string(),
        "TRUUSDTM".to_string(),
        "HIVEUSDTM".to_string(),
        "10000WENUSDTM".to_string(),
        "DASHUSDTM".to_string(),
        "MINAUSDTM".to_string(),
        "LRCUSDTM".to_string(),
        "MASKUSDTM".to_string(),
        "ALTUSDTM".to_string(),
        "AVAXUSDTM".to_string(),
        "CFXUSDTM".to_string(),
        "ENJUSDTM".to_string(),
        "TUTUSDTM".to_string(),
        "BBUSDTM".to_string(),
        "MUBARAKUSDTM".to_string(),
        "ARKUSDTM".to_string(),
        "GMXUSDTM".to_string(),
        "MANAUSDTM".to_string(),
        "RIFUSDTM".to_string(),
        "UNIUSDTM".to_string(),
        "XCNUSDTM".to_string(),
        "OGNUSDTM".to_string(),
        "USUALUSDTM".to_string(),
        "MDTUSDTM".to_string(),
        "10000SATSUSDTM".to_string(),
        "KAITOUSDTM".to_string(),
        "SWELLUSDTM".to_string(),
        "ALICEUSDTM".to_string(),
        "IOTXUSDTM".to_string(),
        "XBTUSDCM".to_string(),
        "AGLDUSDTM".to_string(),
        "AVAILUSDTM".to_string(),
        "RFCUSDTM".to_string(),
        "RUNEUSDTM".to_string(),
        "ETCUSDTM".to_string(),
        "PAXGUSDTM".to_string(),
        "VANRYUSDTM".to_string(),
        "DYMUSDTM".to_string(),
        "CARVUSDTM".to_string(),
        "PLUMEUSDTM".to_string(),
        "FLUXUSDTM".to_string(),
        "SWARMSUSDTM".to_string(),
        "GASUSDTM".to_string(),
        "LQTYUSDTM".to_string(),
        "XTZUSDTM".to_string(),
        "XBTMU25".to_string(),
        "BIOUSDTM".to_string(),
        "ZRCUSDTM".to_string(),
        "APTUSDTM".to_string(),
        "FORMUSDTM".to_string(),
        "STAKEUSDTM".to_string(),
        "BELUSDTM".to_string(),
        "MEMEUSDTM".to_string(),
        "CHILLGUYUSDTM".to_string(),
        "CAKEUSDTM".to_string(),
        "SUIUSDTM".to_string(),
        "CELRUSDTM".to_string(),
        "LUNCUSDTM".to_string(),
        "TRUMPUSDTM".to_string(),
        "MERLUSDTM".to_string(),
        "MAGICUSDTM".to_string(),
        "RSRUSDTM".to_string(),
        "JUSDTM".to_string(),
        "NMRUSDTM".to_string(),
        "KNCUSDTM".to_string(),
        "SOLAYERUSDTM".to_string(),
        "VRAUSDTM".to_string(),
        "POWRUSDTM".to_string(),
        "XVSUSDTM".to_string(),
        "1INCHUSDTM".to_string(),
        "FUELUSDTM".to_string(),
        "IPUSDTM".to_string(),
        "PERPUSDTM".to_string(),
        "C98USDTM".to_string(),
        "TSTBSCUSDTM".to_string(),
        "EIGENUSDTM".to_string(),
        "HIPPOUSDTM".to_string(),
        "QTUMUSDTM".to_string(),
        "10000LADYSUSDTM".to_string(),
        "SOLUSDCM".to_string(),
        "JELLYJELLYUSDTM".to_string(),
        "RVNUSDTM".to_string(),
        "10000CATUSDTM".to_string(),
        "HSKUSDTM".to_string(),
        "NTRNUSDTM".to_string(),
        "REDSTONEUSDTM".to_string(),
        "HFTUSDTM".to_string(),
        "CVXUSDTM".to_string(),
        "JTOUSDTM".to_string(),
        "1000CHEEMSUSDTM".to_string(),
        "GUSDTM".to_string(),
        "BNTUSDTM".to_string(),
        "POPCATUSDTM".to_string(),
        "DYDXUSDTM".to_string(),
        "SONICUSDTM".to_string(),
        "FTTUSDTM".to_string(),
        "STGUSDTM".to_string(),
        "COWUSDTM".to_string(),
        "FARTCOINUSDTM".to_string(),
        "ENSUSDTM".to_string(),
        "PORTALUSDTM".to_string(),
        "SUSDTM".to_string(),
        "USTCUSDTM".to_string(),
        "BANUSDTM".to_string(),
        "SKATEUSDTM".to_string(),
        "SOONUSDTM".to_string(),
        "TRXUSDTM".to_string(),
        "KERNELUSDTM".to_string(),
        "FILUSDTM".to_string(),
        "NEOUSDTM".to_string(),
        "PYTHUSDTM".to_string(),
        "KAIAUSDTM".to_string(),
        "DRIFTUSDTM".to_string(),
        "WAXPUSDTM".to_string(),
        "XECUSDTM".to_string(),
        "TAIKOUSDTM".to_string(),
        "POLUSDTM".to_string(),
        "VOXELUSDTM".to_string(),
        "ARKMUSDTM".to_string(),
        "ROAMUSDTM".to_string(),
        "PHBUSDTM".to_string(),
        "SLERFUSDTM".to_string(),
        "PIXELUSDTM".to_string(),
        "MOCAUSDTM".to_string(),
        "MILKUSDTM".to_string(),
        "ROSEUSDTM".to_string(),
        "SHELLUSDTM".to_string(),
        "TNSRUSDTM".to_string(),
        "TONUSDTM".to_string(),
        "SUNUSDTM".to_string(),
        "MOVEUSDTM".to_string(),
        "OBOLUSDTM".to_string(),
        "ZRXUSDTM".to_string(),
        "XLMUSDTM".to_string(),
        "TAOUSDTM".to_string(),
        "BLURUSDTM".to_string(),
        "XMRUSDTM".to_string(),
        "AEROUSDTM".to_string(),
        "AI16ZUSDTM".to_string(),
        "BMTUSDTM".to_string(),
        "CTSIUSDTM".to_string(),
        "BAKEUSDTM".to_string(),
        "AUCTIONUSDTM".to_string(),
        "MOODENGUSDTM".to_string(),
        "XRPUSDTM".to_string(),
        "LPTUSDTM".to_string(),
        "ARUSDTM".to_string(),
        "B3USDTM".to_string(),
        "PNUTUSDTM".to_string(),
        "SYRUPUSDTM".to_string(),
        "HIGHUSDTM".to_string(),
    ])
}
