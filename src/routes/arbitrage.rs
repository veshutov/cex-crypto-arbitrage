use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::{uuid, Uuid};
use std::{collections::HashMap, sync::Arc};

use crate::{exchanges::{ArbitrageOpportunity, Exchange, OrderType, TickerData, ExchangeFee}, AppState};

#[derive(Debug, Deserialize)]
pub struct ArbitrageQuery {
    exchange1_order_type: OrderType,
    exchange2_order_type: OrderType,
}

#[derive(Debug, Serialize)]
pub struct ArbitrageResponse {
    opportunities: Vec<ArbitrageOpportunity>,
}

pub async fn get_arbitrage_opportunities(
    State(state): State<AppState>,
    Query(query): Query<ArbitrageQuery>,
) -> Json<ArbitrageResponse> {
    let mut opportunities = Vec::new();
    let exchanges = state.exchanges;

    // Get all tickers with prices from both exchanges
    let mut all_tickers_map: HashMap<String, Vec<(String, TickerData, ExchangeFee)>> = HashMap::new();
    
    for exchange in exchanges.iter() {
        if let Ok(tickers) = exchange.get_futures_tickers().await {
            println!("tickers size on {} exchange: {:?}", exchange.name(), tickers.len());
            for ticker in tickers {
                let fee = exchange.get_fees(if exchange.name() == "bybit" {
                    query.exchange1_order_type
                } else {
                    query.exchange2_order_type
                });
                
                let symbol = convert_symbol(ticker.symbol.as_str(), exchange.name());
                
                // Group tickers by symbol
                if let Some((_, tickers)) = all_tickers_map.iter_mut().find(|(s, _)| s.to_string() == symbol) {
                    tickers.push((exchange.name().to_string(), ticker, fee));
                } else {
                    all_tickers_map.insert(symbol, vec![(exchange.name().to_string(), ticker, fee)]);
                }
            }
        }
    }

    println!("tickers on both exchanges: {:?}", all_tickers_map.iter().filter(|(_, tickers)| tickers.len() == 2).count());

    // Check each symbol for arbitrage opportunities
    for (symbol, tickers) in all_tickers_map {
        // Compare prices between exchanges
        for i in 0..tickers.len() {
            for j in (i + 1)..tickers.len() {
                let (exchange1, ticker1, fee1) = &tickers[i];
                let (exchange2, ticker2, fee2) = &tickers[j];

                // Check if we can buy on exchange1 and sell on exchange2
                let buy_on_1_sell_on_2 = ticker1.best_ask_price < ticker2.best_bid_price;
                let total_fee1 = (ticker1.best_ask_price * fee1.taker_fee + ticker2.best_bid_price * fee2.taker_fee) * 2.0;
                let profit1 = ticker2.best_bid_price - ticker1.best_ask_price - total_fee1;

                // Check if we can buy on exchange2 and sell on exchange1
                let buy_on_2_sell_on_1 = ticker2.best_ask_price < ticker1.best_bid_price;
                let total_fee2 = (ticker2.best_ask_price * fee2.taker_fee + ticker1.best_bid_price * fee1.taker_fee) * 2.0;
                let profit2 = ticker1.best_bid_price - ticker2.best_ask_price - total_fee2;

                if buy_on_1_sell_on_2 && profit1 > 0.0 {
                    opportunities.push(ArbitrageOpportunity {
                        symbol: symbol.clone(),
                        buy_exchange: exchange1.clone(),
                        sell_exchange: exchange2.clone(),
                        buy_price: ticker1.best_ask_price,
                        sell_price: ticker2.best_bid_price,
                        potential_profit: profit1,
                        total_fees: total_fee1,
                    });
                }

                if buy_on_2_sell_on_1 && profit2 > 0.0 {
                    opportunities.push(ArbitrageOpportunity {
                        symbol: symbol.clone(),
                        buy_exchange: exchange2.clone(),
                        sell_exchange: exchange1.clone(),
                        buy_price: ticker2.best_ask_price,
                        sell_price: ticker1.best_bid_price,
                        potential_profit: profit2,
                        total_fees: total_fee2,
                    });
                }
            }
        }
    }

    opportunities.sort_by(|a, b| b.potential_profit.total_cmp(&a.potential_profit));

    opportunities.iter().for_each(|o| {
        println!("symbol: {}, profit: {}, buy: {}, sell: {}", o.symbol, o.potential_profit, o.buy_exchange, o.sell_exchange);
    });

    Json(ArbitrageResponse { opportunities })
} 

fn convert_symbol(symbol: &str, exchange: &str) -> String {
    match exchange {
        "bybit" => convert_bybit_symbol(symbol),
        "kucoin" => convert_kucoin_symbol(symbol),
        "okx" => convert_okx_symbol(symbol),
        "bitget" => convert_bitget_symbol(symbol),
        "htx" => convert_htx_symbol(symbol),
        "gate" => convert_gate_symbol(symbol),
        "mexc" => convert_mexc_symbol(symbol),
        "bingx" => convert_bingx_symbol(symbol),
        _ => symbol.to_string(),
    }
}

fn convert_bybit_symbol(symbol: &str) -> String {
    // Handle special cases first
    match symbol {
        "1000000BABYDOGEUSDT" => "1MBABYDOGEUSDTM".to_string(),
        "1000000MOGUSDT" => "1000000MOGUSDTM".to_string(),
        "10000COQUSDT" => "10000COQUSDTM".to_string(),
        "10000LADYSUSDT" => "10000LADYSUSDTM".to_string(),
        "10000SATSUSDT" => "10000SATSUSDTM".to_string(),
        "10000WENUSDT" => "10000WENUSDTM".to_string(),
        "1000BONKUSDT" => "1000BONKUSDTM".to_string(),
        "1000RATSUSDT" => "1000RATSUSDTM".to_string(),
        "1000XUSDT" => "1000XUSDTM".to_string(),
        "1INCHUSDT" => "1INCHUSDTM".to_string(),
        "BTCUSDT" => "XBTUSDTM".to_string(),
        "ETHUSDT" => "ETHUSDTM".to_string(),
        "SOLUSDT" => "SOLUSDTM".to_string(),
        "DOTUSDT" => "DOTUSDTM".to_string(),
        "LTCUSDT" => "LTCUSDTM".to_string(),
        "BCHUSDT" => "BCHUSDTM".to_string(),
        "XRPUSDT" => "XRPUSDTM".to_string(),
        "LUNA2USDT" => "LUNAUSDTM".to_string(),
        "NEIROUSDT" => "NEIROUSDTM".to_string(),
        "NEIROETHUSDT" => "NEIROETHUSDTM".to_string(),
        "RAYDIUMUSDT" => "RAYUSDTM".to_string(),
        // "SHIB1000USDT" => "SHIBUSDTM".to_string(),
        "WLDUSDT" => "WLDUSDTM".to_string(),
        "RONINUSDT" => "RONUSDTM".to_string(),
        "OMGUSDT" => "OMGUSDTM".to_string(),
        _ => {
            if symbol.ends_with("USDT") && !symbol.contains("PERP") {
                format!("{}M", symbol)
            } else {
                Uuid::now_v7().to_string()
            }
        }
    }
}

fn convert_kucoin_symbol(symbol: &str) -> String {
    // KuCoin symbols are already in the standard format
    symbol.to_string()
}

fn convert_okx_symbol(symbol: &str) -> String {
    // Remove the -SWAP suffix and add M suffix for USDT pairs
    if symbol.ends_with("-SWAP") {
        let base = symbol.trim_end_matches("-SWAP");
        if base.ends_with("USDT") {
            format!("{}M", base)
        } else {
            base.to_string()
        }
    } else {
        symbol.to_string()
    }
}

fn convert_bitget_symbol(symbol: &str) -> String {
    // Remove the _UMCBL suffix and add M suffix for USDT pairs
    if symbol.ends_with("_UMCBL") {
        let base = symbol.trim_end_matches("_UMCBL");
        if base.ends_with("USDT") {
            format!("{}M", base)
        } else {
            base.to_string()
        }
    } else {
        symbol.to_string()
    }
}

fn convert_htx_symbol(symbol: &str) -> String {
    // Remove the -USDT suffix and add M suffix
    if symbol.ends_with("-USDT") {
        let base = symbol.trim_end_matches("-USDT");
        format!("{}USDTM", base)
    } else {
        symbol.to_string()
    }
}

fn convert_gate_symbol(symbol: &str) -> String {
    // Remove the _USDT suffix and add M suffix
    if symbol.ends_with("_USDT") {
        let base = symbol.trim_end_matches("_USDT");
        format!("{}USDTM", base)
    } else {
        symbol.to_string()
    }
}

fn convert_mexc_symbol(symbol: &str) -> String {
    // Remove the _USDT suffix and add M suffix
    if symbol.ends_with("_USDT") {
        let base = symbol.trim_end_matches("_USDT");
        format!("{}USDTM", base)
    } else {
        symbol.to_string()
    }
}

fn convert_bingx_symbol(symbol: &str) -> String {
    // Remove the -USDT suffix and add M suffix
    if symbol.ends_with("-USDT") {
        let base = symbol.trim_end_matches("-USDT");
        format!("{}USDTM", base)
    } else {
        symbol.to_string()
    }
}