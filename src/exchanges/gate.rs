use core::panic;
use std::collections::HashSet;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use hex;
use hmac::{Hmac, Mac};
use reqwest::Client;
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
}

impl GateExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
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
        &self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        let url = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut exchange_wr, mut exchange_rc) = ws_stream.split();

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Subscribe to order book for all symbols
        let symbol_whitelist = get_symbols_whitelist();
        let symbols = config
            .symbols
            .iter()
            .map(|symbol| to_exchange_symbol(symbol))
            .filter(|s| symbol_whitelist.contains(s))
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
                                let best_ask_amount = from_exchange_amount(&symbol, data["result"]["A"].as_i64().unwrap()).into();

                                let best_bid_price = data["result"]["b"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<Decimal>()
                                    .unwrap();
                                let best_bid_amount = from_exchange_amount(&symbol, data["result"]["B"].as_i64().unwrap()).into();

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
                            println!("Error while receiving data from gate {:?}", e);
                            break;
                        }
                        r => {
                            println!("Error while receiving data from gate {:?}", r);
                            break;
                        }
                    }
                } else {
                    println!("Rx dropped, exiting gate worker");
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
            OrderSide::Buy => to_exchange_amount(&order.symbol, order.quantity),
            OrderSide::Sell => -to_exchange_amount(&order.symbol, order.quantity),
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
                    size: from_exchange_amount(&symbol, qty.abs()),
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

fn to_exchange_amount(symbol: &str, amount: i32) -> i32 {
    match symbol {
        "RDO" => amount / 100,
        "T" => amount / 100,
        "REX" => amount / 100,
        "AIOT" => amount / 10,
        "TGT" => amount / 10,
        "OMG" => amount,
        "XEM" => amount,
        "ZKJ" => amount,
        "SCA" => amount,
        _ => panic!("Unknown gate token {}", symbol)
    }
}

fn from_exchange_amount(symbol: &str, amount: i64) -> i32 {
    let a = match symbol {
        "RDO" => amount * 100,
        "T" => amount * 100,
        "REX" => amount * 100,
        "AIOT" => amount * 10,
        "TGT" => amount * 10,
        "OMG" => amount,
        "XEM" => amount,
        "ZKJ" => amount,
        "SCA" => amount,
        _ => panic!("Unknown gate token {}", symbol)
    };
    a as i32
}

fn get_symbols_whitelist() -> HashSet<String> {
    HashSet::from([
        "SONIC_USDT".to_string(),
        "VTHO_USDT".to_string(),
        "FHE_USDT".to_string(),
        "WEN_USDT".to_string(),
        "EDGE_USDT".to_string(),
        "KDA_USDT".to_string(),
        "ONE_USDT".to_string(),
        "NS_USDT".to_string(),
        "FLOCK_USDT".to_string(),
        "CHR_USDT".to_string(),
        "CTK_USDT".to_string(),
        "HOME_USDT".to_string(),
        "XRD_USDT".to_string(),
        "RIF_USDT".to_string(),
        "MYRIA_USDT".to_string(),
        "XCN_USDT".to_string(),
        "XRP_USDT".to_string(),
        "DYDX_USDT".to_string(),
        "CARV_USDT".to_string(),
        "DEXE_USDT".to_string(),
        "TOKEN_USDT".to_string(),
        "ARK_USDT".to_string(),
        "ALEO_USDT".to_string(),
        "KAS_USDT".to_string(),
        "OMNI_USDT".to_string(),
        "WAL_USDT".to_string(),
        "ACT_USDT".to_string(),
        "INIT_USDT".to_string(),
        "STRK_USDT".to_string(),
        "BERA_USDT".to_string(),
        "XEC_USDT".to_string(),
        "CELO_USDT".to_string(),
        "DOGE_USDT".to_string(),
        "ARPA_USDT".to_string(),
        "SWARMS_USDT".to_string(),
        "EGLD_USDT".to_string(),
        "FUEL_USDT".to_string(),
        "XION_USDT".to_string(),
        "DEGO_USDT".to_string(),
        "COOKIE_USDT".to_string(),
        "LEVER_USDT".to_string(),
        "PHB_USDT".to_string(),
        "DBR_USDT".to_string(),
        "BEL_USDT".to_string(),
        "BROCCOLIF3B_USDT".to_string(),
        "AIOT_USDT".to_string(),
        "ALICE_USDT".to_string(),
        "SAGA_USDT".to_string(),
        "AIOZ_USDT".to_string(),
        "VIC_USDT".to_string(),
        "PROMPT_USDT".to_string(),
        "BDXN_USDT".to_string(),
        "PFVS_USDT".to_string(),
        "BAKE_USDT".to_string(),
        "CTC_USDT".to_string(),
        "SSV_USDT".to_string(),
        "FLM_USDT".to_string(),
        "TLM_USDT".to_string(),
        "HIPPO_USDT".to_string(),
        "MOCA_USDT".to_string(),
        "SEND_USDT".to_string(),
        "ALT_USDT".to_string(),
        "ZETA_USDT".to_string(),
        "BAL_USDT".to_string(),
        "AVA_USDT".to_string(),
        "FWOG_USDT".to_string(),
        "AVAAI_USDT".to_string(),
        "MEMEFI_USDT".to_string(),
        "MANTA_USDT".to_string(),
        "MERL_USDT".to_string(),
        "ALU_USDT".to_string(),
        "REQ_USDT".to_string(),
        "CAT_USDT".to_string(),
        "PONKE_USDT".to_string(),
        "B3_USDT".to_string(),
        "METIS_USDT".to_string(),
        "GLMR_USDT".to_string(),
        "PAWS_USDT".to_string(),
        "MLN_USDT".to_string(),
        "SYN_USDT".to_string(),
        "IMT_USDT".to_string(),
        "CSPR_USDT".to_string(),
        "F_USDT".to_string(),
        "XLM_USDT".to_string(),
        "SXT_USDT".to_string(),
        "SKL_USDT".to_string(),
        "ALPACA_USDT".to_string(),
        "CAKE_USDT".to_string(),
        "ROAM_USDT".to_string(),
        "AVAX_USDT".to_string(),
        "JASMY_USDT".to_string(),
        "BSW_USDT".to_string(),
        "PERP_USDT".to_string(),
        "COMP_USDT".to_string(),
        "JTO_USDT".to_string(),
        "HUMA_USDT".to_string(),
        "DODO_USDT".to_string(),
        "CHZ_USDT".to_string(),
        "CATI_USDT".to_string(),
        "ZK_USDT".to_string(),
        "SAFE_USDT".to_string(),
        "JST_USDT".to_string(),
        "TGT_USDT".to_string(),
        "TAIKO_USDT".to_string(),
        "OG_USDT".to_string(),
        "FDUSD_USDT".to_string(),
        "LUNA_USDT".to_string(),
        "YFI_USDT".to_string(),
        "SCRT_USDT".to_string(),
        "HOOK_USDT".to_string(),
        "BAIDOGE_USDT".to_string(),
        "PORT3_USDT".to_string(),
        "THE_USDT".to_string(),
        "PI_USDT".to_string(),
        "BR_USDT".to_string(),
        "POWR_USDT".to_string(),
        "NXPC_USDT".to_string(),
        "SQD_USDT".to_string(),
        "ELON_USDT".to_string(),
        "LPT_USDT".to_string(),
        "VANRY_USDT".to_string(),
        "NULS_USDT".to_string(),
        "AEVO_USDT".to_string(),
        "MUBARAK_USDT".to_string(),
        "HNT_USDT".to_string(),
        "J_USDT".to_string(),
        "PRAI_USDT".to_string(),
        "EDGEN_USDT".to_string(),
        "W_USDT".to_string(),
        "ACH_USDT".to_string(),
        "POL_USDT".to_string(),
        "LTC_USDT".to_string(),
        "SOLV_USDT".to_string(),
        "NEIRO_USDT".to_string(),
        "TRU_USDT".to_string(),
        "REI_USDT".to_string(),
        "SLERF_USDT".to_string(),
        "ZRX_USDT".to_string(),
        "HBAR_USDT".to_string(),
        "XAI_USDT".to_string(),
        "ZEREBRO_USDT".to_string(),
        "UNFI_USDT".to_string(),
        "STO_USDT".to_string(),
        "PIPPIN_USDT".to_string(),
        "STX_USDT".to_string(),
        "MEME_USDT".to_string(),
        "BENQI_USDT".to_string(),
        "SPELL_USDT".to_string(),
        "RESOLV_USDT".to_string(),
        "ALPINE_USDT".to_string(),
        "BAND_USDT".to_string(),
        "BIGTIME_USDT".to_string(),
        "PORTAL_USDT".to_string(),
        "HOT_USDT".to_string(),
        "MYRO_USDT".to_string(),
        "SWEAT_USDT".to_string(),
        "STG_USDT".to_string(),
        "BADGER_USDT".to_string(),
        "ATOM_USDT".to_string(),
        "RPL_USDT".to_string(),
        "ASTR_USDT".to_string(),
        "GHST_USDT".to_string(),
        "BAT_USDT".to_string(),
        "BANANA_USDT".to_string(),
        "HIGH_USDT".to_string(),
        "ACE_USDT".to_string(),
        "AI_USDT".to_string(),
        "BAN_USDT".to_string(),
        "WOO_USDT".to_string(),
        "ETHFI_USDT".to_string(),
        "GHIBLI_USDT".to_string(),
        "APU_USDT".to_string(),
        "KAVA_USDT".to_string(),
        "BEAMX_USDT".to_string(),
        "KERNEL_USDT".to_string(),
        "LRC_USDT".to_string(),
        "AUDIO_USDT".to_string(),
        "VANA_USDT".to_string(),
        "VVV_USDT".to_string(),
        "ONT_USDT".to_string(),
        "ROSE_USDT".to_string(),
        "JOE_USDT".to_string(),
        "ETC_USDT".to_string(),
        "MOVE_USDT".to_string(),
        "KOMA_USDT".to_string(),
        "APE_USDT".to_string(),
        "BLUR_USDT".to_string(),
        "TRB_USDT".to_string(),
        "BICO_USDT".to_string(),
        "BRETT_USDT".to_string(),
        "BOBA_USDT".to_string(),
        "BLUE_USDT".to_string(),
        "CYBER_USDT".to_string(),
        "XTZ_USDT".to_string(),
        "IDOL_USDT".to_string(),
        "SXP_USDT".to_string(),
        "MASK_USDT".to_string(),
        "CKB_USDT".to_string(),
        "HAEDAL_USDT".to_string(),
        "OM_USDT".to_string(),
        "RAY_USDT".to_string(),
        "PHA_USDT".to_string(),
        "CVC_USDT".to_string(),
        "MBOX_USDT".to_string(),
        "RSS3_USDT".to_string(),
        "TON_USDT".to_string(),
        "PROM_USDT".to_string(),
        "XNO_USDT".to_string(),
        "PEIPEI_USDT".to_string(),
        "EPT_USDT".to_string(),
        "FLOKI_USDT".to_string(),
        "ANKR_USDT".to_string(),
        "USDC_USDT".to_string(),
        "ICP_USDT".to_string(),
        "CGPT_USDT".to_string(),
        "WAVES_USDT".to_string(),
        "SUI_USDT".to_string(),
        "WEMIX_USDT".to_string(),
        "ZEUS_USDT".to_string(),
        "LUCE_USDT".to_string(),
        "BONK_USDT".to_string(),
        "SHM_USDT".to_string(),
        "RDNT_USDT".to_string(),
        "COS_USDT".to_string(),
        "SOL_USDT".to_string(),
        "POLYX_USDT".to_string(),
        "RVN_USDT".to_string(),
        "RATS_USDT".to_string(),
        "B2_USDT".to_string(),
        "KNC_USDT".to_string(),
        "RON_USDT".to_string(),
        "VIRTUAL_USDT".to_string(),
        "LDO_USDT".to_string(),
        "PRCL_USDT".to_string(),
        "POKT_USDT".to_string(),
        "FB_USDT".to_string(),
        "ZRO_USDT".to_string(),
        "MOVR_USDT".to_string(),
        "SPX_USDT".to_string(),
        "ELDE_USDT".to_string(),
        "WIF_USDT".to_string(),
        "MOG_USDT".to_string(),
        "LOOKS_USDT".to_string(),
        "DEGEN_USDT".to_string(),
        "KAITO_USDT".to_string(),
        "AMB_USDT".to_string(),
        "JELLYJELLY_USDT".to_string(),
        "VET_USDT".to_string(),
        "LISTA_USDT".to_string(),
        "KAIA_USDT".to_string(),
        "REN_USDT".to_string(),
        "TAI_USDT".to_string(),
        "BCH_USDT".to_string(),
        "QNT_USDT".to_string(),
        "PAAL_USDT".to_string(),
        "QUICK_USDT".to_string(),
        "FORTH_USDT".to_string(),
        "COTI_USDT".to_string(),
        "QUBIC_USDT".to_string(),
        "SIREN_USDT".to_string(),
        "KSM_USDT".to_string(),
        "FET_USDT".to_string(),
        "DIA_USDT".to_string(),
        "TIA_USDT".to_string(),
        "ULTI_USDT".to_string(),
        "CRV_USDT".to_string(),
        "NOT_USDT".to_string(),
        "OBT_USDT".to_string(),
        "IOST_USDT".to_string(),
        "BANANAS31_USDT".to_string(),
        "LSK_USDT".to_string(),
        "NEIROETH_USDT".to_string(),
        "PUFFER_USDT".to_string(),
        "GRIFFAIN_USDT".to_string(),
        "SYS_USDT".to_string(),
        "SD_USDT".to_string(),
        "UMA_USDT".to_string(),
        "INJ_USDT".to_string(),
        "DOT_USDT".to_string(),
        "MKR_USDT".to_string(),
        "ICE_USDT".to_string(),
        "NEAR_USDT".to_string(),
        "BNB_USDT".to_string(),
        "RLC_USDT".to_string(),
        "PEPE_USDT".to_string(),
        "AUCTION_USDT".to_string(),
        "DRIFT_USDT".to_string(),
        "FLY_USDT".to_string(),
        "SFP_USDT".to_string(),
        "MBL_USDT".to_string(),
        "CETUS_USDT".to_string(),
        "LOOM_USDT".to_string(),
        "ZIL_USDT".to_string(),
        "NC_USDT".to_string(),
        "BABY_USDT".to_string(),
        "OXT_USDT".to_string(),
        "ONG_USDT".to_string(),
        "BNT_USDT".to_string(),
        "POPCAT_USDT".to_string(),
        "BTT_USDT".to_string(),
        "RARE_USDT".to_string(),
        "OL_USDT".to_string(),
        "NAVX_USDT".to_string(),
        "BROCCOLI_USDT".to_string(),
        "WLD_USDT".to_string(),
        "SKATE_USDT".to_string(),
        "IOTA_USDT".to_string(),
        "RENDER_USDT".to_string(),
        "ATH_USDT".to_string(),
        "PAXG_USDT".to_string(),
        "JAILSTOOL_USDT".to_string(),
        "1INCH_USDT".to_string(),
        "NIL_USDT".to_string(),
        "OBOL_USDT".to_string(),
        "RDO_USDT".to_string(),
        "DOG_USDT".to_string(),
        "ETH_USDT".to_string(),
        "SYRUP_USDT".to_string(),
        "PLUME_USDT".to_string(),
        "BANK_USDT".to_string(),
        "MBABYDOGE_USDT".to_string(),
        "T_USDT".to_string(),
        "A_USDT".to_string(),
        "SOPH_USDT".to_string(),
        "HMSTR_USDT".to_string(),
        "FTN_USDT".to_string(),
        "REZ_USDT".to_string(),
        "DOGS_USDT".to_string(),
        "TAO_USDT".to_string(),
        "LQTY_USDT".to_string(),
        "GRASS_USDT".to_string(),
        "HYPER_USDT".to_string(),
        "ADA_USDT".to_string(),
        "FOXY_USDT".to_string(),
        "BGSC_USDT".to_string(),
        "SNT_USDT".to_string(),
        "MOODENG_USDT".to_string(),
        "DGB_USDT".to_string(),
        "SLP_USDT".to_string(),
        "GALA_USDT".to_string(),
        "VINE_USDT".to_string(),
        "BOND_USDT".to_string(),
        "OMG_USDT".to_string(),
        "MILK_USDT".to_string(),
        "AMP_USDT".to_string(),
        "ALCH_USDT".to_string(),
        "FLUX_USDT".to_string(),
        "BUZZ_USDT".to_string(),
        "L3_USDT".to_string(),
        "AIXBT_USDT".to_string(),
        "BSV_USDT".to_string(),
        "NKN_USDT".to_string(),
        "GPS_USDT".to_string(),
        "WHY_USDT".to_string(),
        "DARK_USDT".to_string(),
        "FTT_USDT".to_string(),
        "TNSR_USDT".to_string(),
        "ALGO_USDT".to_string(),
        "KEKIUS_USDT".to_string(),
        "FARTCOIN_USDT".to_string(),
        "X_USDT".to_string(),
        "VOXEL_USDT".to_string(),
        "SOON_USDT".to_string(),
        "PRIME_USDT".to_string(),
        "XDC_USDT".to_string(),
        "AR_USDT".to_string(),
        "AI16Z_USDT".to_string(),
        "ME_USDT".to_string(),
        "ENJ_USDT".to_string(),
        "SUNDOG_USDT".to_string(),
        "LA_USDT".to_string(),
        "CHESS_USDT".to_string(),
        "NEO_USDT".to_string(),
        "ICX_USDT".to_string(),
        "LADYS_USDT".to_string(),
        "D_USDT".to_string(),
        "PYR_USDT".to_string(),
        "LINK_USDT".to_string(),
        "LOKA_USDT".to_string(),
        "TRX_USDT".to_string(),
        "SEI_USDT".to_string(),
        "BOME_USDT".to_string(),
        "VRA_USDT".to_string(),
        "AAVE_USDT".to_string(),
        "RAD_USDT".to_string(),
        "ANIME_USDT".to_string(),
        "HYPE_USDT".to_string(),
        "ENA_USDT".to_string(),
        "TRUMP_USDT".to_string(),
        "XEM_USDT".to_string(),
        "MOODENGETH_USDT".to_string(),
        "SKYAI_USDT".to_string(),
        "FLOW_USDT".to_string(),
        "APT_USDT".to_string(),
        "LAYER_USDT".to_string(),
        "NEIROCTO_USDT".to_string(),
        "RWA_USDT".to_string(),
        "CRO_USDT".to_string(),
        "AERGO_USDT".to_string(),
        "PELL_USDT".to_string(),
        "MAV_USDT".to_string(),
        "SWELL_USDT".to_string(),
        "GROK_USDT".to_string(),
        "MANA_USDT".to_string(),
        "SUPRA_USDT".to_string(),
        "COW_USDT".to_string(),
        "FIDA_USDT".to_string(),
        "IOTX_USDT".to_string(),
        "MAVIA_USDT".to_string(),
        "MAGIC_USDT".to_string(),
        "HAPPY_USDT".to_string(),
        "SERAPH_USDT".to_string(),
        "ATA_USDT".to_string(),
        "ELX_USDT".to_string(),
        "VELODROME_USDT".to_string(),
        "YGG_USDT".to_string(),
        "MORPHO_USDT".to_string(),
        "HIFI_USDT".to_string(),
        "CLOUD_USDT".to_string(),
        "CORE_USDT".to_string(),
        "PENDLE_USDT".to_string(),
        "TURBO_USDT".to_string(),
        "ZKJ_USDT".to_string(),
        "LUMIA_USDT".to_string(),
        "SATS_USDT".to_string(),
        "VELO_USDT".to_string(),
        "FIS_USDT".to_string(),
        "STMX_USDT".to_string(),
        "USUAL_USDT".to_string(),
        "MEW_USDT".to_string(),
        "G_USDT".to_string(),
        "IMX_USDT".to_string(),
        "MICHI_USDT".to_string(),
        "ZRC_USDT".to_string(),
        "DOLO_USDT".to_string(),
        "WAXL_USDT".to_string(),
        "SCA_USDT".to_string(),
        "AXS_USDT".to_string(),
        "AVAIL_USDT".to_string(),
        "DUSK_USDT".to_string(),
        "MAJOR_USDT".to_string(),
        "MDT_USDT".to_string(),
        "PEOPLE_USDT".to_string(),
        "THETA_USDT".to_string(),
        "HEI_USDT".to_string(),
        "DOOD_USDT".to_string(),
        "GMX_USDT".to_string(),
        "AGLD_USDT".to_string(),
        "KMNO_USDT".to_string(),
        "BID_USDT".to_string(),
        "CHEEMS_USDT".to_string(),
        "PIXEL_USDT".to_string(),
        "XCH_USDT".to_string(),
        "USTC_USDT".to_string(),
        "SANTOS_USDT".to_string(),
        "B_USDT".to_string(),
        "ILV_USDT".to_string(),
        "PEAQ_USDT".to_string(),
        "DF_USDT".to_string(),
        "RED_USDT".to_string(),
        "UNI_USDT".to_string(),
        "SUSHI_USDT".to_string(),
        "SHELL_USDT".to_string(),
        "FORM_USDT".to_string(),
        "BMT_USDT".to_string(),
        "XAUT_USDT".to_string(),
        "ETHW_USDT".to_string(),
        "GUN_USDT".to_string(),
        "WCT_USDT".to_string(),
        "AGI_USDT".to_string(),
        "SIGN_USDT".to_string(),
        "CVX_USDT".to_string(),
        "HIVE_USDT".to_string(),
        "ORCA_USDT".to_string(),
        "MOBILE_USDT".to_string(),
        "SAND_USDT".to_string(),
        "GOAT_USDT".to_string(),
        "DYM_USDT".to_string(),
        "A8_USDT".to_string(),
        "RUNE_USDT".to_string(),
        "AWE_USDT".to_string(),
        "GM_USDT".to_string(),
        "UXLINK_USDT".to_string(),
        "PENGU_USDT".to_string(),
        "AKT_USDT".to_string(),
        "PYTH_USDT".to_string(),
        "EPIC_USDT".to_string(),
        "JUP_USDT".to_string(),
        "LAUNCHCOIN_USDT".to_string(),
        "NTRN_USDT".to_string(),
        "BLZ_USDT".to_string(),
        "BIO_USDT".to_string(),
        "SHIB_USDT".to_string(),
        "AVL_USDT".to_string(),
        "PNUT_USDT".to_string(),
        "TSTBSC_USDT".to_string(),
        "PARTI_USDT".to_string(),
        "QTUM_USDT".to_string(),
        "ORBS_USDT".to_string(),
        "SC_USDT".to_string(),
        "MTL_USDT".to_string(),
        "MINA_USDT".to_string(),
        "DUCK_USDT".to_string(),
        "GMT_USDT".to_string(),
        "GAS_USDT".to_string(),
        "ARC_USDT".to_string(),
        "OGN_USDT".to_string(),
        "ZORA_USDT".to_string(),
        "MASA_USDT".to_string(),
        "EIGEN_USDT".to_string(),
        "ORDER_USDT".to_string(),
        "TUT_USDT".to_string(),
        "NMR_USDT".to_string(),
        "GT_USDT".to_string(),
        "GLM_USDT".to_string(),
        "RDAC_USDT".to_string(),
        "DEEP_USDT".to_string(),
        "IO_USDT".to_string(),
        "BB_USDT".to_string(),
        "GIGA_USDT".to_string(),
        "IP_USDT".to_string(),
        "DENT_USDT".to_string(),
        "RSR_USDT".to_string(),
        "CFX_USDT".to_string(),
        "SCR_USDT".to_string(),
        "KILO_USDT".to_string(),
        "FIO_USDT".to_string(),
        "ASR_USDT".to_string(),
        "PUNDIX_USDT".to_string(),
        "AGT_USDT".to_string(),
        "STEEM_USDT".to_string(),
        "S_USDT".to_string(),
        "GRT_USDT".to_string(),
        "REX_USDT".to_string(),
        "IDEX_USDT".to_string(),
        "CATS_USDT".to_string(),
        "FUN_USDT".to_string(),
        "HOUSE_USDT".to_string(),
        "API3_USDT".to_string(),
        "ALPHA_USDT".to_string(),
        "NFP_USDT".to_string(),
        "XVS_USDT".to_string(),
        "SOLO_USDT".to_string(),
        "SUN_USDT".to_string(),
        "EDU_USDT".to_string(),
        "OP_USDT".to_string(),
        "ACX_USDT".to_string(),
        "ZBCN_USDT".to_string(),
        "AERO_USDT".to_string(),
        "BTC_USDT".to_string(),
        "BLAST_USDT".to_string(),
        "ENS_USDT".to_string(),
        "TOSHI_USDT".to_string(),
        "ONDO_USDT".to_string(),
        "MELANIA_USDT".to_string(),
        "CPOOL_USDT".to_string(),
        "KEY_USDT".to_string(),
        "FLR_USDT".to_string(),
        "HSK_USDT".to_string(),
        "GORK_USDT".to_string(),
        "SUPER_USDT".to_string(),
        "ARKM_USDT".to_string(),
        "CHILLGUY_USDT".to_string(),
        "WAXP_USDT".to_string(),
        "SNX_USDT".to_string(),
        "ID_USDT".to_string(),
        "ARB_USDT".to_string(),
        "TWT_USDT".to_string(),
        "CTSI_USDT".to_string(),
        "HFT_USDT".to_string(),
        "LUNC_USDT".to_string(),
        "CELR_USDT".to_string(),
        "ORDI_USDT".to_string(),
        "GNO_USDT".to_string(),
        "RFC_USDT".to_string(),
        "FIL_USDT".to_string(),
        "RIFSOL_USDT".to_string(),
        "ZCX_USDT".to_string(),
        "C98_USDT".to_string(),
        "STORJ_USDT".to_string(),
    ])
}
