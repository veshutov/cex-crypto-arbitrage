use futures::future::join_all;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

use crate::exchanges::Exchange;
use crate::{
    exchanges::{ArbitrageOpportunity, ExchangeFee, ExchangeName, OrderBookData, TickerData},
    AppState,
};

struct OwnedOrderBookItem(String, String);

pub async fn start_arbitrage_checker_ws(state: AppState) {
    let symbols = vec!["BTCUSDT"];

    let stdout = io::stdout();
    let mut handle = io::BufWriter::new(stdout);

    // Get exchanges
    let exchanges = state.exchanges;
    let (bybit, kucoin, gate) = exchanges.as_ref();

    // Subscribe to order books for each symbol on each exchange
    let mut orderbook_receivers = Vec::new();
    for symbol in symbols {
        if let Ok(rx) = bybit.subscribe_orderbook(symbol.to_string(), async |orderbook| {
          println!("bybit Order book {:?}", orderbook)
        }).await {
            orderbook_receivers.push((ExchangeName::Bybit, symbol, rx));
        }
        if let Ok(rx) = kucoin.subscribe_orderbook("ETHUSDTM".to_string(), async |orderbook| {
          println!("kucoin Order book {:?}", orderbook)
        }).await {
            orderbook_receivers.push((ExchangeName::Kucoin, "ETHUSDTM", rx));
        }
        // if let Ok(rx) = gate.subscribe_orderbook("BTC_USDT".to_string(), async |orderbook| {
        //   println!("gate Order book {:?}", orderbook)
        // }).await {
        //     orderbook_receivers.push((ExchangeName::Gate, "BTC_USDT", rx));
        // }
    }

    // // Store latest order book data
    // let mut orderbooks: HashMap<(ExchangeName, String), OrderBookData> = HashMap::new();

    // // Process any new order book updates
    // for (exchange, symbol, mut rx) in orderbook_receivers {
    //     tokio::spawn(async move {
    //         while let Ok(orderbook) = rx.try_recv() {
    //           println!("Order book {:?}", orderbook)
    //             // orderbooks.insert((*exchange, symbol.clone()), orderbook);
    //         }
    //     });
    // }

    loop {
      sleep(Duration::from_secs(5)).await;
    }
    // Main loop
    // loop {
    //     // Check for arbitrage opportunities
    //     let opportunities = check_arbitrage_opportunities_ws(&orderbooks, exchanges);

    //     // Display results
    //     write!(handle, "\x1B[2J\x1B[1;1H").unwrap();
    //     writeln!(handle, "Arbitrage Opportunities:").unwrap();
    //     writeln!(handle, "----------------------").unwrap();

    //     for opp in opportunities.iter().take(10) {
    //         writeln!(
    //             handle,
    //             "Symbol: {}, Buy: {:?} @ {}, Sell: {:?} @ {}, Profit: {:.8}",
    //             opp.symbol,
    //             opp.buy_exchange,
    //             opp.buy_price,
    //             opp.sell_exchange,
    //             opp.sell_price,
    //             opp.potential_profit
    //         )
    //         .unwrap();
    //     }
    //     handle.flush().unwrap();

    //     sleep(Duration::from_secs(5)).await;
    // }
}

// fn check_arbitrage_opportunities_ws(
//     orderbooks: &HashMap<(ExchangeName, String), OrderBookData>,
//     exchanges: Arc<Vec<Box<dyn Exchange>>>,
// ) -> Vec<ArbitrageOpportunity> {
//     let mut opportunities = Vec::new();

//     // Group orderbooks by symbol
//     let mut symbol_orderbooks: HashMap<String, Vec<(ExchangeName, &OrderBookData)>> =
//         HashMap::new();
//     for ((exchange, symbol), orderbook) in orderbooks {
//         symbol_orderbooks
//             .entry(symbol.clone())
//             .or_default()
//             .push((*exchange, orderbook));
//     }

//     // Check each symbol for arbitrage opportunities
//     for (symbol, orderbooks) in symbol_orderbooks {
//         if orderbooks.len() < 2 {
//             continue;
//         }

//         // Compare prices between exchanges
//         for i in 0..orderbooks.len() {
//             for j in (i + 1)..orderbooks.len() {
//                 let (exchange1, orderbook1) = orderbooks[i];
//                 let (exchange2, orderbook2) = orderbooks[j];

//                 // Get fees for both exchanges
//                 let fee1 = exchanges
//                     .iter()
//                     .find(|e| e == exchange1)
//                     .map(|e| e.get_fees())
//                     .unwrap_or(ExchangeFee {
//                         maker_fee: 0.0,
//                         taker_fee: 0.0,
//                     });
//                 let fee2 = exchanges
//                     .iter()
//                     .find(|e| e == exchange2)
//                     .map(|e| e.get_fees())
//                     .unwrap_or(ExchangeFee {
//                         maker_fee: 0.0,
//                         taker_fee: 0.0,
//                     });

//                 // Get best bid and ask prices
//                 let best_ask1 = orderbook1
//                     .asks
//                     .first()
//                     .map(|(price, _)| *price)
//                     .unwrap_or(f64::MAX);
//                 let best_bid1 = orderbook1
//                     .bids
//                     .first()
//                     .map(|(price, _)| *price)
//                     .unwrap_or(0.0);
//                 let best_ask2 = orderbook2
//                     .asks
//                     .first()
//                     .map(|(price, _)| *price)
//                     .unwrap_or(f64::MAX);
//                 let best_bid2 = orderbook2
//                     .bids
//                     .first()
//                     .map(|(price, _)| *price)
//                     .unwrap_or(0.0);

//                 // Check if we can buy on exchange1 and sell on exchange2
//                 let buy_on_1_sell_on_2 = best_ask1 < best_bid2;
//                 let total_fee1 = (best_ask1 * fee1.maker_fee + best_bid2 * fee2.maker_fee) * 2.0;
//                 let profit1 = best_bid2 - best_ask1 - total_fee1;

//                 // Check if we can buy on exchange2 and sell on exchange1
//                 let buy_on_2_sell_on_1 = best_ask2 < best_bid1;
//                 let total_fee2 = (best_ask2 * fee2.maker_fee + best_bid1 * fee1.maker_fee) * 2.0;
//                 let profit2 = best_bid1 - best_ask2 - total_fee2;

//                 if buy_on_1_sell_on_2 && profit1 > 0.0 {
//                     opportunities.push(ArbitrageOpportunity {
//                         symbol: symbol.clone(),
//                         buy_exchange: exchange1.clone(),
//                         sell_exchange: exchange2.clone(),
//                         buy_price: best_ask1,
//                         sell_price: best_bid2,
//                         potential_profit: profit1,
//                         total_fees: total_fee1,
//                     });
//                 }

//                 if buy_on_2_sell_on_1 && profit2 > 0.0 {
//                     opportunities.push(ArbitrageOpportunity {
//                         symbol: symbol.clone(),
//                         buy_exchange: exchange2.clone(),
//                         sell_exchange: exchange1.clone(),
//                         buy_price: best_ask2,
//                         sell_price: best_bid1,
//                         potential_profit: profit2,
//                         total_fees: total_fee2,
//                     });
//                 }
//             }
//         }
//     }

//     opportunities.sort_by(|a, b| b.potential_profit.total_cmp(&a.potential_profit));
//     opportunities
// }

// pub async fn start_arbitrage_checker(state: AppState) {
//     loop {
//         let now: Instant = Instant::now();
//         let _opportunities = check_arbitrage_opportunities(&state).await;
//         let elapsed = now.elapsed();
//         println!("Check duration: {:.2?}", elapsed);
//         sleep(Duration::from_secs(5)).await;
//     }
// }

// async fn check_arbitrage_opportunities(state: &AppState) -> Vec<ArbitrageOpportunity> {
//     let mut opportunities = Vec::new();
//     let exchanges = state.exchanges.clone();

//     // Get all tickers with prices from both exchanges
//     let mut all_tickers_map: HashMap<String, Vec<(ExchangeName, TickerData, ExchangeFee)>> =
//         HashMap::new();

//     // Run exchange requests in parallel, but measure each exchange call duration individually
//     let ticker_futures: Vec<_> = exchanges
//         .iter()
//         .map(|exchange| {
//             let name = exchange.name();
//             async move {
//                 let start = Instant::now();
//                 let result = exchange.get_futures_tickers().await;
//                 let duration = start.elapsed();
//                 println!("Exchange {:?} request duration: {:.2?}", name, duration);
//                 result
//             }
//         })
//         .collect();

//     let now: Instant = Instant::now();
//     let ticker_results = join_all(ticker_futures).await;
//     let elapsed = now.elapsed();
//     println!("Exchanges requests duration: {:.2?}", elapsed);

//     for (exchange, result) in exchanges.iter().zip(ticker_results) {
//         if let Ok(tickers) = result {
//             println!(
//                 "tickers size on {:?} exchange: {:?}",
//                 exchange.name(),
//                 tickers.len()
//             );
//             for ticker in tickers {
//                 if ticker.volume_24h < state.cfg.min_volume_24h {
//                     continue;
//                 }
//                 let fee = exchange.get_fees();
//                 let symbol = convert_symbol(ticker.symbol.clone(), exchange.name());

//                 // Group tickers by symbol
//                 if let Some((_, tickers)) = all_tickers_map
//                     .iter_mut()
//                     .find(|(s, _)| s.to_string() == symbol)
//                 {
//                     tickers.push((exchange.name(), ticker, fee));
//                 } else {
//                     all_tickers_map.insert(symbol, vec![(exchange.name(), ticker, fee)]);
//                 }
//             }
//         }
//     }

//     println!(
//         "tickers on both exchanges: {:?}",
//         all_tickers_map
//             .iter()
//             .filter(|(_, tickers)| tickers.len() > 1)
//             .count()
//     );

//     // Check each symbol for arbitrage opportunities
//     for (symbol, tickers) in all_tickers_map {
//         // Compare prices between exchanges
//         for i in 0..tickers.len() {
//             for j in (i + 1)..tickers.len() {
//                 let (exchange1, ticker1, fee1) = &tickers[i];
//                 let (exchange2, ticker2, fee2) = &tickers[j];

//                 // Check if we can buy on exchange1 and sell on exchange2
//                 let buy_on_1_sell_on_2 = ticker1.best_ask_price < ticker2.best_bid_price;
//                 let total_fee1 = (ticker1.best_ask_price * fee1.maker_fee
//                     + ticker2.best_bid_price * fee2.maker_fee)
//                     * 2.0;
//                 let profit1 = ticker2.best_bid_price - ticker1.best_ask_price - total_fee1;

//                 // Check if we can buy on exchange2 and sell on exchange1
//                 let buy_on_2_sell_on_1 = ticker2.best_ask_price < ticker1.best_bid_price;
//                 let total_fee2 = (ticker2.best_ask_price * fee2.maker_fee
//                     + ticker1.best_bid_price * fee1.maker_fee)
//                     * 2.0;
//                 let profit2 = ticker1.best_bid_price - ticker2.best_ask_price - total_fee2;

//                 if buy_on_1_sell_on_2 && profit1 > 0.0 {
//                     opportunities.push(ArbitrageOpportunity {
//                         symbol: symbol.clone(),
//                         buy_exchange: exchange1.clone(),
//                         sell_exchange: exchange2.clone(),
//                         buy_price: ticker1.best_ask_price,
//                         sell_price: ticker2.best_bid_price,
//                         potential_profit: profit1,
//                         total_fees: total_fee1,
//                     });
//                 }

//                 if buy_on_2_sell_on_1 && profit2 > 0.0 {
//                     opportunities.push(ArbitrageOpportunity {
//                         symbol: symbol.clone(),
//                         buy_exchange: exchange2.clone(),
//                         sell_exchange: exchange1.clone(),
//                         buy_price: ticker2.best_ask_price,
//                         sell_price: ticker1.best_bid_price,
//                         potential_profit: profit2,
//                         total_fees: total_fee2,
//                     });
//                 }
//             }
//         }
//     }

//     opportunities.sort_by(|a, b| b.potential_profit.total_cmp(&a.potential_profit));

//     opportunities.iter().take(10).for_each(|o| {
//         println!(
//             "symbol: {}, profit: {}, buy: {:?}, sell: {:?}",
//             o.symbol, o.potential_profit, o.buy_exchange, o.sell_exchange
//         );
//     });

//     println!("------------------------------------------------------------------");

//     opportunities
// }

fn convert_symbol(symbol: String, exchange: ExchangeName) -> String {
    match exchange {
        ExchangeName::Bybit => convert_bybit_symbol(symbol),
        ExchangeName::Kucoin => convert_kucoin_symbol(symbol),
        ExchangeName::Gate => convert_gate_symbol(symbol),

        ExchangeName::Bingx => todo!(),
        ExchangeName::BitGet => todo!(),
        ExchangeName::Htx => todo!(),
        ExchangeName::Mexc => todo!(),
        ExchangeName::Okx => todo!(),
    }
}

fn convert_bybit_symbol(symbol: String) -> String {
    if let Some(s) = symbol.strip_suffix("USDT") {
        s.to_string()
    } else {
        symbol
    }
}

fn convert_kucoin_symbol(symbol: String) -> String {
    if let Some(s) = symbol.strip_suffix("USDTM") {
        s.to_string()
    } else {
        symbol
    }
}

fn convert_gate_symbol(symbol: String) -> String {
    if let Some(s) = symbol.strip_suffix("_USDT") {
        s.to_string()
    } else {
        symbol
    }
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
