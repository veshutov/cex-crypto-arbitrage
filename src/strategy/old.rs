use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tokio::time::sleep;

use crate::exchanges::bybit::BybitExchange;
use crate::exchanges::gate::GateExchange;
use crate::exchanges::kucoin::KucoinExchange;
use crate::exchanges::{Exchange, ExchangeConfig, TickerData};
use crate::{exchanges::ExchangeName, Config};

pub async fn start_arbitrage_checker(cfg: Config) {
    loop {
        let now: Instant = Instant::now();
        let _opportunities = check_arbitrage_opportunities(&cfg).await;
        let elapsed = now.elapsed();
        println!("Check duration: {:.2?}", elapsed);
        sleep(Duration::from_secs(5)).await;
    }
}

async fn check_arbitrage_opportunities(cfg: &Config) -> Vec<RestArbitrageOpportunity> {
    let mut opportunities = Vec::new();
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(
            cfg.bybit_api_key.clone(),
            cfg.bybit_api_secret.clone(),
            cfg.bybit_taker_fee,
            cfg.bybit_maker_fee,
        )),
        Box::new(KucoinExchange::new(
            cfg.kucoin_api_key.clone(),
            cfg.kucoin_api_secret.clone(),
            cfg.kucoin_taker_fee,
            cfg.kucoin_maker_fee,
        )),
        Box::new(GateExchange::new(
            cfg.gate_api_key.clone(),
            cfg.gate_api_secret.clone(),
            cfg.gate_taker_fee,
            cfg.gate_maker_fee,
        )),
    ];

    // Get all tickers with prices from both exchanges
    let mut all_tickers_map: HashMap<String, Vec<(ExchangeName, TickerData, ExchangeConfig)>> =
        HashMap::new();

    // Run exchange requests in parallel, but measure each exchange call duration individually
    let ticker_futures: Vec<_> = exchanges
        .iter()
        .map(|exchange| async move { exchange.get_futures_tickers().await })
        .collect();

    let now: Instant = Instant::now();
    let ticker_results = join_all(ticker_futures).await;
    let elapsed = now.elapsed();
    println!("Exchanges requests duration: {:.2?}", elapsed);

    for (exchange, result) in exchanges.iter().zip(ticker_results) {
        if let Ok(tickers) = result {
            // println!(
            //     "tickers size on {:?} exchange: {:?}",
            //     exchange.name(),
            //     tickers.len()
            // );
            for ticker in tickers {
                if ticker.volume_24h < cfg.min_volume_24h {
                    continue;
                }
                let fee = exchange.config();
                let symbol = convert_symbol(ticker.symbol.clone(), exchange.name());

                // Group tickers by symbol
                if let Some((_, tickers)) = all_tickers_map
                    .iter_mut()
                    .find(|(s, _)| s.to_string() == symbol)
                {
                    tickers.push((exchange.name(), ticker, fee));
                } else {
                    all_tickers_map.insert(symbol, vec![(exchange.name(), ticker, fee)]);
                }
            }
        }
    }

    println!(
        "tickers on both exchanges: {:?}",
        all_tickers_map
            .iter()
            .filter(|(_, tickers)| tickers.len() > 1)
            .count()
    );

    // Check each symbol for arbitrage opportunities
    for (symbol, tickers) in all_tickers_map {
        // Compare prices between exchanges
        for i in 0..tickers.len() {
            for j in (i + 1)..tickers.len() {
                let (exchange1, ticker1, fee1) = &tickers[i];
                let (exchange2, ticker2, fee2) = &tickers[j];

                // Check if we can buy on exchange1 and sell on exchange2
                let buy_on_1_sell_on_2 = ticker1.best_ask_price < ticker2.best_bid_price;
                let total_fee1 = (ticker1.best_ask_price * fee1.taker_fee
                    + ticker2.best_bid_price * fee2.taker_fee)
                    * Decimal::from(2);
                let profit1 = ticker2.best_bid_price - ticker1.best_ask_price - total_fee1;

                // Check if we can buy on exchange2 and sell on exchange1
                let buy_on_2_sell_on_1 = ticker2.best_ask_price < ticker1.best_bid_price;
                let total_fee2 = (ticker2.best_ask_price * fee2.taker_fee
                    + ticker1.best_bid_price * fee1.taker_fee)
                    * Decimal::from(2);
                let profit2 = ticker1.best_bid_price - ticker2.best_ask_price - total_fee2;

                if buy_on_1_sell_on_2 && profit1 > Decimal::ZERO {
                    opportunities.push(RestArbitrageOpportunity {
                        symbol: symbol.clone(),
                        buy_exchange: exchange1.clone(),
                        sell_exchange: exchange2.clone(),
                        buy_price: ticker1.best_ask_price,
                        sell_price: ticker2.best_bid_price,
                        potential_profit: profit1,
                        total_fees: total_fee1,
                    });
                }

                if buy_on_2_sell_on_1 && profit2 > Decimal::ZERO {
                    opportunities.push(RestArbitrageOpportunity {
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

    opportunities.sort_by(|a, b| b.potential_profit.cmp(&a.potential_profit));

    opportunities.iter().take(10).for_each(|o| {
        println!(
            "rest: symbol: {}, profit: {}, buy: {:?}, sell: {:?}",
            o.symbol, o.potential_profit, o.buy_exchange, o.sell_exchange
        );
    });

    println!("------------------------------------------------------------------");

    opportunities
}

fn convert_symbol(symbol: String, exchange: ExchangeName) -> String {
    match exchange {
        ExchangeName::Bybit => convert_bybit_symbol(symbol),
        ExchangeName::Kucoin => convert_kucoin_symbol(symbol),
        ExchangeName::Gate => convert_gate_symbol(symbol),
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

pub struct RestArbitrageOpportunity {
    pub symbol: String,
    pub buy_exchange: ExchangeName,
    pub sell_exchange: ExchangeName,
    pub buy_price: Decimal,
    pub sell_price: Decimal,
    pub potential_profit: Decimal,
    pub total_fees: Decimal,
}
