use std::{collections::HashSet, time::Duration};

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
    OrderSide, Position, SubscriptionConfig, TickerData,
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

    fn map_to_exchange_symbol(&self, symbol: &str) -> String {
        format!("{}USDT", symbol)
    }

    fn map_from_exchange_symbol(&self, symbol: &str) -> String {
        symbol.strip_suffix("USDT").unwrap().to_string()
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
            println!("Eror response from bybit {}", data);
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
        &self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        let url = "wss://stream.bybit.com/v5/public/linear";
        let (ws_stream, _) = connect_async(url).await?;
        let (mut exchange_wr, mut exchange_rc) = ws_stream.split();

        // Subscribe to order book for all symbols
        let whitelist = get_symbols_whitelist();
        let symbols = config
            .symbols
            .iter()
            .map(|symbol| self.map_to_exchange_symbol(symbol))
            .filter(|s| whitelist.contains(s))
            .map(|symbol| format!("orderbook.1.{}", symbol))
            .collect::<Vec<String>>();
        if symbols.is_empty() {
            return Ok(());
        }
        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": symbols
        });

        exchange_wr
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;
        let mut ping_rc: Receiver<&'static str> = ping().await;

        tokio::spawn(async move {
            'worker: loop {
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
                            ping_rc.close();
                            break;
                        }
                        r => {
                            println!("Error while recieving data from bybit {:?}", r);
                            ping_rc.close();
                            break;
                        }
                    }
                } else {
                    println!("Exiting bybit worker");
                    break 'worker;
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
            "symbol": self.map_to_exchange_symbol(&order.symbol),
            "side": side,
            "orderType": "Market",
            "qty": order.quantity.to_string(),
        });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let recv_window = 2000;
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
            println!("Eror response from bybit {}", data);
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
            "symbol": self.map_to_exchange_symbol(symbol),
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

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        let url = "https://api.bybit.com/v5/position/list";
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let recv_window = 5000;
        let params = "category=linear&settleCoin=USDT";
        let signature = self.generate_signature(timestamp, recv_window, params);

        let response = self
            .client
            .get(format!("{}?{}", url, params))
            .header("X-BAPI-API-KEY", &self.config.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-RECV-WINDOW", recv_window.to_string())
            .send()
            .await?;

        let data: Value = response.json().await?;

        if data["retCode"].as_i64().unwrap_or(1) != 0 {
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["retMsg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let positions = data["result"]["list"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .map(|item| {
                let side = item["side"].as_str().unwrap();
                let side = if side == "Sell" {
                    OrderSide::Sell
                } else if side == "Buy" {
                    OrderSide::Buy
                } else {
                    panic!("Unknown side")
                };
                let symbol = item["symbol"].as_str().unwrap();
                Position {
                    symbol: self.map_from_exchange_symbol(symbol),
                    size: item["size"].as_str().unwrap().parse::<i32>().unwrap(),
                    entry_price: item["avgPrice"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap(),
                    entry_time: item["createdTime"]
                        .as_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap(),
                    exchange_name: ExchangeName::Bybit,
                    side,
                }
            })
            .collect();

        Ok(positions)
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

fn get_symbols_whitelist() -> HashSet<String> {
    HashSet::from([
        "1000000BABYDOGEUSDT".to_string(),
        "1000000CHEEMSUSDT".to_string(),
        "1000000MOGUSDT".to_string(),
        "1000000PEIPEIUSDT".to_string(),
        "10000COQUSDT".to_string(),
        "10000ELONUSDT".to_string(),
        "10000LADYSUSDT".to_string(),
        "10000QUBICUSDT".to_string(),
        "10000SATSUSDT".to_string(),
        "10000WENUSDT".to_string(),
        "10000WHYUSDT".to_string(),
        "1000APUUSDT".to_string(),
        "1000BONKPERP".to_string(),
        "1000BONKUSDT".to_string(),
        "1000BTTUSDT".to_string(),
        "1000CATSUSDT".to_string(),
        "1000CATUSDT".to_string(),
        "1000FLOKIUSDT".to_string(),
        "1000LUNCUSDT".to_string(),
        "1000NEIROCTOUSDT".to_string(),
        "1000PEPEPERP".to_string(),
        "1000PEPEUSDT".to_string(),
        "1000RATSUSDT".to_string(),
        "1000TOSHIUSDT".to_string(),
        "1000TURBOUSDT".to_string(),
        "1000XECUSDT".to_string(),
        "1000XUSDT".to_string(),
        "1INCHUSDT".to_string(),
        "A8USDT".to_string(),
        "AAVEPERP".to_string(),
        "AAVEUSDT".to_string(),
        "ACEUSDT".to_string(),
        "ACHUSDT".to_string(),
        "ACTUSDT".to_string(),
        "ACXUSDT".to_string(),
        "ADAUSDT".to_string(),
        "AERGOUSDT".to_string(),
        "AEROUSDT".to_string(),
        "AEVOPERP".to_string(),
        "AEVOUSDT".to_string(),
        "AGIUSDT".to_string(),
        "AGLDUSDT".to_string(),
        "AGTUSDT".to_string(),
        "AI16ZUSDT".to_string(),
        "AIOZUSDT".to_string(),
        "AIUSDT".to_string(),
        "AIXBTUSDT".to_string(),
        "AKTUSDT".to_string(),
        "ALCHUSDT".to_string(),
        "ALEOUSDT".to_string(),
        "ALGOUSDT".to_string(),
        "ALICEUSDT".to_string(),
        "ALPHAUSDT".to_string(),
        "ALTUSDT".to_string(),
        "ALUUSDT".to_string(),
        "ANIMEUSDT".to_string(),
        "ANKRUSDT".to_string(),
        "APEUSDT".to_string(),
        "API3USDT".to_string(),
        "APTUSDT".to_string(),
        "ARBPERP".to_string(),
        "ARBUSDT".to_string(),
        "ARCUSDT".to_string(),
        "ARKMUSDT".to_string(),
        "ARKUSDT".to_string(),
        "ARPAUSDT".to_string(),
        "ARUSDT".to_string(),
        "ASTRUSDT".to_string(),
        "ATAUSDT".to_string(),
        "ATHUSDT".to_string(),
        "ATOMUSDT".to_string(),
        "AUCTIONUSDT".to_string(),
        "AUDIOUSDT".to_string(),
        "AUSDT".to_string(),
        "AVAAIUSDT".to_string(),
        "AVAILUSDT".to_string(),
        "AVAUSDT".to_string(),
        "AVAXUSDT".to_string(),
        "AVLUSDT".to_string(),
        "AWEUSDT".to_string(),
        "AXLUSDT".to_string(),
        "AXSUSDT".to_string(),
        "B2USDT".to_string(),
        "B3USDT".to_string(),
        "BABYUSDT".to_string(),
        "BADGERUSDT".to_string(),
        "BAKEUSDT".to_string(),
        "BALUSDT".to_string(),
        "BANANAS31USDT".to_string(),
        "BANANAUSDT".to_string(),
        "BANDUSDT".to_string(),
        "BANKUSDT".to_string(),
        "BANUSDT".to_string(),
        "BATUSDT".to_string(),
        "BBUSDT".to_string(),
        "BCHPERP".to_string(),
        "BCHUSDT".to_string(),
        "BDXNUSDT".to_string(),
        "BEAMUSDT".to_string(),
        "BELUSDT".to_string(),
        "BERAUSDT".to_string(),
        "BICOUSDT".to_string(),
        "BIGTIMEUSDT".to_string(),
        "BIOUSDT".to_string(),
        "BLASTUSDT".to_string(),
        "BLURUSDT".to_string(),
        "BMTUSDT".to_string(),
        "BNBPERP".to_string(),
        "BNBUSDT".to_string(),
        "BNTUSDT".to_string(),
        "BOBAUSDT".to_string(),
        "BOMEUSDT".to_string(),
        "BRETTUSDT".to_string(),
        "BROCCOLIUSDT".to_string(),
        "BRUSDT".to_string(),
        "BSVUSDT".to_string(),
        "BSWUSDT".to_string(),
        "BTC-26DEC25".to_string(),
        "BTC-26SEP25".to_string(),
        "BTC-27JUN25".to_string(),
        "BTCPERP".to_string(),
        "BTCUSDT".to_string(),
        "BTCUSDT-04JUL25".to_string(),
        "BTCUSDT-20JUN25".to_string(),
        "BTCUSDT-25JUL25".to_string(),
        "BTCUSDT-26DEC25".to_string(),
        "BTCUSDT-26SEP25".to_string(),
        "BTCUSDT-27JUN25".to_string(),
        "BTCUSDT-27MAR26".to_string(),
        "BTCUSDT-29AUG25".to_string(),
        "BUSDT".to_string(),
        "C98USDT".to_string(),
        "CAKEUSDT".to_string(),
        "CARVUSDT".to_string(),
        "CATIUSDT".to_string(),
        "CELOUSDT".to_string(),
        "CELRUSDT".to_string(),
        "CETUSUSDT".to_string(),
        "CFXUSDT".to_string(),
        "CGPTUSDT".to_string(),
        "CHESSUSDT".to_string(),
        "CHILLGUYUSDT".to_string(),
        "CHRUSDT".to_string(),
        "CHZUSDT".to_string(),
        "CKBUSDT".to_string(),
        "CLANKERUSDT".to_string(),
        "CLOUDUSDT".to_string(),
        "COMPUSDT".to_string(),
        "COOKIEUSDT".to_string(),
        "COOKUSDT".to_string(),
        "COREUSDT".to_string(),
        "COSUSDT".to_string(),
        "COTIUSDT".to_string(),
        "COWUSDT".to_string(),
        "CPOOLUSDT".to_string(),
        "CROUSDT".to_string(),
        "CRVPERP".to_string(),
        "CRVUSDT".to_string(),
        "CTCUSDT".to_string(),
        "CTKUSDT".to_string(),
        "CTSIUSDT".to_string(),
        "CUDISUSDT".to_string(),
        "CVCUSDT".to_string(),
        "CVXUSDT".to_string(),
        "CYBERUSDT".to_string(),
        "DARKUSDT".to_string(),
        "DASHUSDT".to_string(),
        "DBRUSDT".to_string(),
        "DEEPUSDT".to_string(),
        "DEGENUSDT".to_string(),
        "DENTUSDT".to_string(),
        "DEXEUSDT".to_string(),
        "DGBUSDT".to_string(),
        "DODOUSDT".to_string(),
        "DOGEPERP".to_string(),
        "DOGEUSDT".to_string(),
        "DOGSUSDT".to_string(),
        "DOGUSDT".to_string(),
        "DOODUSDT".to_string(),
        "DOTPERP".to_string(),
        "DOTUSDT".to_string(),
        "DRIFTUSDT".to_string(),
        "DUCKUSDT".to_string(),
        "DUSKUSDT".to_string(),
        "DYDXUSDT".to_string(),
        "DYMUSDT".to_string(),
        "EDUUSDT".to_string(),
        "EGLDUSDT".to_string(),
        "EIGENUSDT".to_string(),
        "ELXUSDT".to_string(),
        "ENAPERP".to_string(),
        "ENAUSDT".to_string(),
        "ENJUSDT".to_string(),
        "ENSUSDT".to_string(),
        "EPICUSDT".to_string(),
        "EPTUSDT".to_string(),
        "ETCPERP".to_string(),
        "ETCUSDT".to_string(),
        "ETH-26DEC25".to_string(),
        "ETH-26SEP25".to_string(),
        "ETH-27JUN25".to_string(),
        "ETHBTCUSDT".to_string(),
        "ETHFIPERP".to_string(),
        "ETHFIUSDT".to_string(),
        "ETHPERP".to_string(),
        "ETHUSDT".to_string(),
        "ETHUSDT-04JUL25".to_string(),
        "ETHUSDT-20JUN25".to_string(),
        "ETHUSDT-25JUL25".to_string(),
        "ETHUSDT-26DEC25".to_string(),
        "ETHUSDT-26SEP25".to_string(),
        "ETHUSDT-27JUN25".to_string(),
        "ETHUSDT-27MAR26".to_string(),
        "ETHUSDT-29AUG25".to_string(),
        "ETHWUSDT".to_string(),
        "FARTCOINUSDT".to_string(),
        "FBUSDT".to_string(),
        "FHEUSDT".to_string(),
        "FIDAUSDT".to_string(),
        "FILUSDT".to_string(),
        "FIOUSDT".to_string(),
        "FLMUSDT".to_string(),
        "FLOCKUSDT".to_string(),
        "FLOWUSDT".to_string(),
        "FLRUSDT".to_string(),
        "FLUXUSDT".to_string(),
        "FORMUSDT".to_string(),
        "FORTHUSDT".to_string(),
        "FTNUSDT".to_string(),
        "FUELUSDT".to_string(),
        "FUSDT".to_string(),
        "FWOGUSDT".to_string(),
        "FXSUSDT".to_string(),
        "GALAUSDT".to_string(),
        "GASUSDT".to_string(),
        "GIGAUSDT".to_string(),
        "GLMRUSDT".to_string(),
        "GLMUSDT".to_string(),
        "GMTUSDT".to_string(),
        "GMXUSDT".to_string(),
        "GNOUSDT".to_string(),
        "GOATUSDT".to_string(),
        "GODSUSDT".to_string(),
        "GORKUSDT".to_string(),
        "GPSUSDT".to_string(),
        "GRASSUSDT".to_string(),
        "GRIFFAINUSDT".to_string(),
        "GRTUSDT".to_string(),
        "GTCUSDT".to_string(),
        "GUNUSDT".to_string(),
        "GUSDT".to_string(),
        "HAEDALUSDT".to_string(),
        "HBARUSDT".to_string(),
        "HEIUSDT".to_string(),
        "HFTUSDT".to_string(),
        "HIFIUSDT".to_string(),
        "HIGHUSDT".to_string(),
        "HIPPOUSDT".to_string(),
        "HIVEUSDT".to_string(),
        "HMSTRUSDT".to_string(),
        "HNTUSDT".to_string(),
        "HOMEUSDT".to_string(),
        "HOOKUSDT".to_string(),
        "HOTUSDT".to_string(),
        "HPOS10IUSDT".to_string(),
        "HUMAUSDT".to_string(),
        "HYPEPERP".to_string(),
        "HYPERUSDT".to_string(),
        "HYPEUSDT".to_string(),
        "ICPUSDT".to_string(),
        "ICXUSDT".to_string(),
        "IDEXUSDT".to_string(),
        "IDUSDT".to_string(),
        "ILVUSDT".to_string(),
        "IMXUSDT".to_string(),
        "INITUSDT".to_string(),
        "INJUSDT".to_string(),
        "IOSTUSDT".to_string(),
        "IOTAUSDT".to_string(),
        "IOTXUSDT".to_string(),
        "IOUSDT".to_string(),
        "IPUSDT".to_string(),
        "JASMYUSDT".to_string(),
        "JELLYJELLYUSDT".to_string(),
        "JOEUSDT".to_string(),
        "JSTUSDT".to_string(),
        "JTOUSDT".to_string(),
        "JUPUSDT".to_string(),
        "JUSDT".to_string(),
        "KAIAUSDT".to_string(),
        "KAITOUSDT".to_string(),
        "KASUSDT".to_string(),
        "KAVAUSDT".to_string(),
        "KDAUSDT".to_string(),
        "KERNELUSDT".to_string(),
        "KMNOUSDT".to_string(),
        "KNCUSDT".to_string(),
        "KOMAUSDT".to_string(),
        "KSMUSDT".to_string(),
        "L3USDT".to_string(),
        "LAUNCHCOINUSDT".to_string(),
        "LAUSDT".to_string(),
        "LDOUSDT".to_string(),
        "LEVERUSDT".to_string(),
        "LINKPERP".to_string(),
        "LINKUSDT".to_string(),
        "LISTAUSDT".to_string(),
        "LOOKSUSDT".to_string(),
        "LPTUSDT".to_string(),
        "LQTYUSDT".to_string(),
        "LRCUSDT".to_string(),
        "LSKUSDT".to_string(),
        "LTCPERP".to_string(),
        "LTCUSDT".to_string(),
        "LUMIAUSDT".to_string(),
        "LUNA2USDT".to_string(),
        "MAGICUSDT".to_string(),
        "MAJORUSDT".to_string(),
        "MANAUSDT".to_string(),
        "MANTAUSDT".to_string(),
        "MASAUSDT".to_string(),
        "MASKUSDT".to_string(),
        "MAVIAUSDT".to_string(),
        "MAVUSDT".to_string(),
        "MBLUSDT".to_string(),
        "MBOXUSDT".to_string(),
        "MDTUSDT".to_string(),
        "MELANIAUSDT".to_string(),
        "MEMEUSDT".to_string(),
        "MERLUSDT".to_string(),
        "METISUSDT".to_string(),
        "MEUSDT".to_string(),
        "MEWUSDT".to_string(),
        "MICHIUSDT".to_string(),
        "MILKUSDT".to_string(),
        "MINAUSDT".to_string(),
        "MKRUSDT".to_string(),
        "MLNUSDT".to_string(),
        "MNTPERP".to_string(),
        "MNTUSDT".to_string(),
        "MOBILEUSDT".to_string(),
        "MOCAUSDT".to_string(),
        "MOODENGUSDT".to_string(),
        "MORPHOUSDT".to_string(),
        "MOVEUSDT".to_string(),
        "MOVRUSDT".to_string(),
        "MTLUSDT".to_string(),
        "MUBARAKUSDT".to_string(),
        "MVLUSDT".to_string(),
        "MYRIAUSDT".to_string(),
        "MYROUSDT".to_string(),
        "NCUSDT".to_string(),
        "NEARUSDT".to_string(),
        "NEIROETHUSDT".to_string(),
        "NEOUSDT".to_string(),
        "NFPUSDT".to_string(),
        "NILUSDT".to_string(),
        "NKNUSDT".to_string(),
        "NMRUSDT".to_string(),
        "NOTPERP".to_string(),
        "NOTUSDT".to_string(),
        "NSUSDT".to_string(),
        "NTRNUSDT".to_string(),
        "NXPCUSDT".to_string(),
        "OBOLUSDT".to_string(),
        "OBTUSDT".to_string(),
        "OGNUSDT".to_string(),
        "OGUSDT".to_string(),
        "OLUSDT".to_string(),
        "OMNIUSDT".to_string(),
        "OMUSDT".to_string(),
        "ONDOPERP".to_string(),
        "ONDOUSDT".to_string(),
        "ONEUSDT".to_string(),
        "ONGUSDT".to_string(),
        "ONTUSDT".to_string(),
        "OPPERP".to_string(),
        "OPUSDT".to_string(),
        "ORBSUSDT".to_string(),
        "ORCAUSDT".to_string(),
        "ORDERUSDT".to_string(),
        "ORDIPERP".to_string(),
        "ORDIUSDT".to_string(),
        "OSMOUSDT".to_string(),
        "OXTUSDT".to_string(),
        "PARTIUSDT".to_string(),
        "PAXGUSDT".to_string(),
        "PEAQUSDT".to_string(),
        "PENDLEUSDT".to_string(),
        "PENGUUSDT".to_string(),
        "PEOPLEUSDT".to_string(),
        "PERPUSDT".to_string(),
        "PHAUSDT".to_string(),
        "PHBUSDT".to_string(),
        "PIPPINUSDT".to_string(),
        "PIXELUSDT".to_string(),
        "PLUMEUSDT".to_string(),
        "PNUTUSDT".to_string(),
        "POLPERP".to_string(),
        "POLUSDT".to_string(),
        "POLYXUSDT".to_string(),
        "PONKEUSDT".to_string(),
        "POPCATPERP".to_string(),
        "POPCATUSDT".to_string(),
        "PORTALUSDT".to_string(),
        "POWRUSDT".to_string(),
        "PRAIUSDT".to_string(),
        "PRCLUSDT".to_string(),
        "PRIMEUSDT".to_string(),
        "PROMPTUSDT".to_string(),
        "PROMUSDT".to_string(),
        "PUFFERUSDT".to_string(),
        "PUMPBTCUSDT".to_string(),
        "PUNDIXUSDT".to_string(),
        "PYRUSDT".to_string(),
        "PYTHUSDT".to_string(),
        "QIUSDT".to_string(),
        "QNTUSDT".to_string(),
        "QTUMUSDT".to_string(),
        "QUICKUSDT".to_string(),
        "RADUSDT".to_string(),
        "RAREUSDT".to_string(),
        "RAYDIUMUSDT".to_string(),
        "RDNTUSDT".to_string(),
        "REDUSDT".to_string(),
        "RENDERUSDT".to_string(),
        "REQUSDT".to_string(),
        "RESOLVUSDT".to_string(),
        "REXUSDT".to_string(),
        "REZUSDT".to_string(),
        "RFCUSDT".to_string(),
        "RIFUSDT".to_string(),
        "RLCUSDT".to_string(),
        "ROAMUSDT".to_string(),
        "RONINUSDT".to_string(),
        "ROSEUSDT".to_string(),
        "RPLUSDT".to_string(),
        "RSRUSDT".to_string(),
        "RSS3USDT".to_string(),
        "RUNEUSDT".to_string(),
        "RVNUSDT".to_string(),
        "SAFEUSDT".to_string(),
        "SAGAUSDT".to_string(),
        "SANDUSDT".to_string(),
        "SAROSUSDT".to_string(),
        "SCAUSDT".to_string(),
        "SCRTUSDT".to_string(),
        "SCRUSDT".to_string(),
        "SCUSDT".to_string(),
        "SDUSDT".to_string(),
        "SEIUSDT".to_string(),
        "SENDUSDT".to_string(),
        "SERAPHUSDT".to_string(),
        "SFPUSDT".to_string(),
        "SHELLUSDT".to_string(),
        "SHIB1000PERP".to_string(),
        "SHIB1000USDT".to_string(),
        "SIGNUSDT".to_string(),
        "SIRENUSDT".to_string(),
        "SKATEUSDT".to_string(),
        "SKLUSDT".to_string(),
        "SKYAIUSDT".to_string(),
        "SLERFUSDT".to_string(),
        "SLFUSDT".to_string(),
        "SLPUSDT".to_string(),
        "SNTUSDT".to_string(),
        "SNXUSDT".to_string(),
        "SOLAYERUSDT".to_string(),
        "SOLOUSDT".to_string(),
        "SOLPERP".to_string(),
        "SOLUSDT".to_string(),
        "SOLUSDT-04JUL25".to_string(),
        "SOLUSDT-20JUN25".to_string(),
        "SOLUSDT-25JUL25".to_string(),
        "SOLUSDT-27JUN25".to_string(),
        "SOLVUSDT".to_string(),
        "SONICUSDT".to_string(),
        "SOONUSDT".to_string(),
        "SOPHUSDT".to_string(),
        "SPECUSDT".to_string(),
        "SPELLUSDT".to_string(),
        "SPXUSDT".to_string(),
        "SQDUSDT".to_string(),
        "SSVUSDT".to_string(),
        "STEEMUSDT".to_string(),
        "STGUSDT".to_string(),
        "STORJUSDT".to_string(),
        "STOUSDT".to_string(),
        "STRKPERP".to_string(),
        "STRKUSDT".to_string(),
        "STXUSDT".to_string(),
        "SUIPERP".to_string(),
        "SUIUSDT".to_string(),
        "SUNDOGUSDT".to_string(),
        "SUNUSDT".to_string(),
        "SUPERUSDT".to_string(),
        "SUSDT".to_string(),
        "SUSHIUSDT".to_string(),
        "SWARMSUSDT".to_string(),
        "SWEATUSDT".to_string(),
        "SWELLUSDT".to_string(),
        "SXPUSDT".to_string(),
        "SXTUSDT".to_string(),
        "SYNUSDT".to_string(),
        "SYRUPUSDT".to_string(),
        "SYSUSDT".to_string(),
        "TAIKOUSDT".to_string(),
        "TAIUSDT".to_string(),
        "TAOUSDT".to_string(),
        "THETAUSDT".to_string(),
        "THEUSDT".to_string(),
        "TIAPERP".to_string(),
        "TIAUSDT".to_string(),
        "TLMUSDT".to_string(),
        "TNSRUSDT".to_string(),
        "TOKENUSDT".to_string(),
        "TONPERP".to_string(),
        "TONUSDT".to_string(),
        "TRBUSDT".to_string(),
        "TRUMPPERP".to_string(),
        "TRUMPUSDT".to_string(),
        "TRUUSDT".to_string(),
        "TRXUSDT".to_string(),
        "TSTBSCUSDT".to_string(),
        "TUSDT".to_string(),
        "TUTUSDT".to_string(),
        "TWTUSDT".to_string(),
        "UMAUSDT".to_string(),
        "UNIPERP".to_string(),
        "UNIUSDT".to_string(),
        "USDCUSDT".to_string(),
        "USDEUSDT".to_string(),
        "USTCUSDT".to_string(),
        "USUALUSDT".to_string(),
        "UXLINKUSDT".to_string(),
        "VANAUSDT".to_string(),
        "VANRYUSDT".to_string(),
        "VELODROMEUSDT".to_string(),
        "VELOUSDT".to_string(),
        "VETUSDT".to_string(),
        "VICUSDT".to_string(),
        "VINEUSDT".to_string(),
        "VIRTUALUSDT".to_string(),
        "VOXELUSDT".to_string(),
        "VRUSDT".to_string(),
        "VTHOUSDT".to_string(),
        "VVVUSDT".to_string(),
        "WALUSDT".to_string(),
        "WAVESUSDT".to_string(),
        "WAXPUSDT".to_string(),
        "WCTUSDT".to_string(),
        "WIFPERP".to_string(),
        "WIFUSDT".to_string(),
        "WLDPERP".to_string(),
        "WLDUSDT".to_string(),
        "WOOUSDT".to_string(),
        "WUSDT".to_string(),
        "XAIUSDT".to_string(),
        "XAUTUSDT".to_string(),
        "XCHUSDT".to_string(),
        "XCNUSDT".to_string(),
        "XDCUSDT".to_string(),
        "XEMUSDT".to_string(),
        "XIONUSDT".to_string(),
        "XLMPERP".to_string(),
        "XLMUSDT".to_string(),
        "XMRUSDT".to_string(),
        "XNOUSDT".to_string(),
        "XRDUSDT".to_string(),
        "XRPPERP".to_string(),
        "XRPUSDT".to_string(),
        "XTERUSDT".to_string(),
        "XTZUSDT".to_string(),
        "XVGUSDT".to_string(),
        "XVSUSDT".to_string(),
        "YFIUSDT".to_string(),
        "YGGUSDT".to_string(),
        "ZBCNUSDT".to_string(),
        "ZECUSDT".to_string(),
        "ZENTUSDT".to_string(),
        "ZENUSDT".to_string(),
        "ZEREBROUSDT".to_string(),
        "ZETAUSDT".to_string(),
        "ZEUSUSDT".to_string(),
        "ZILUSDT".to_string(),
        "ZKJUSDT".to_string(),
        "ZKUSDT".to_string(),
        "ZORAUSDT".to_string(),
        "ZRCUSDT".to_string(),
        "ZROUSDT".to_string(),
        "ZRXUSDT".to_string(),
    ])
}
