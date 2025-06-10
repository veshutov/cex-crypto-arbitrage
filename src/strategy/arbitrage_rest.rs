use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tokio::time::sleep;

use crate::engine::ArbitrageOpportunity;
use crate::{
    exchanges::bybit::BybitExchange,
    exchanges::gate::GateExchange,
    exchanges::kucoin::KucoinExchange,
    exchanges::{Exchange, ExchangeConfig, ExchangeName, TickerData},
    Config, Result,
};

pub async fn start_arbitrage_checker_rest(cfg: Config) -> Result<()> {
    loop {
        let now: Instant = Instant::now();
        let _opportunities = check_arbitrage_opportunities(&cfg).await?;
        let elapsed = now.elapsed();
        println!("Check duration: {:.2?}", elapsed);
        sleep(Duration::from_secs(5)).await;
    }
}

async fn check_arbitrage_opportunities(cfg: &Config) -> Result<Vec<ArbitrageOpportunity>> {
    let mut opportunities = Vec::with_capacity(1000);
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(cfg.bybit.clone())),
        Box::new(KucoinExchange::new(cfg.kucoin.clone())),
        Box::new(GateExchange::new(cfg.gate.clone())),
    ];

    // Get all tickers with prices from both exchanges
    let mut all_tickers_map: HashMap<String, Vec<(ExchangeName, TickerData, ExchangeConfig)>> =
        HashMap::with_capacity(1000); // Pre-allocate with reasonable capacity

    let ticker_futures: Vec<_> = exchanges
        .iter()
        .map(|exchange| async { exchange.get_futures_tickers().await })
        .collect();

    let now: Instant = Instant::now();
    let ticker_results = join_all(ticker_futures).await;
    let elapsed = now.elapsed();
    println!("Exchanges requests duration: {:.2?}", elapsed);

    for (exchange, result) in exchanges.iter().zip(ticker_results) {
        let tickers = result?;
        for ticker in tickers {
            if ticker.volume_24h < cfg.min_volume_24h {
                continue;
            }
            let fee = exchange.config();
            let symbol = convert_symbol(ticker.symbol.clone(), exchange.name());

            // Use direct HashMap entry API for more efficient insertion
            all_tickers_map
                .entry(symbol)
                .or_insert_with(|| Vec::with_capacity(3)) // Pre-allocate for max 3 exchanges
                .push((exchange.name(), ticker, fee));
        }
    }

    println!(
        "tickers on both exchanges: {:?}",
        all_tickers_map
            .iter()
            .filter(|(_, tickers)| tickers.len() > 1)
            .count()
    );

    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let hundred = Decimal::from(100);
    let two = Decimal::from(2);

    // Check each symbol for arbitrage opportunities
    for (symbol, tickers) in all_tickers_map {
        // Compare prices between exchanges
        for i in 0..tickers.len() {
            for j in (i + 1)..tickers.len() {
                let (exchange1, ticker1, fee1) = &tickers[i];
                let (exchange2, ticker2, fee2) = &tickers[j];

                let buy_price1 = ticker1.best_ask_price;
                let sell_price1 = ticker2.best_bid_price;
                let buy_price2 = ticker2.best_ask_price;
                let sell_price2 = ticker1.best_bid_price;

                // Check if we can buy on exchange1 and sell on exchange2
                let buy_on_1_sell_on_2 = buy_price1 < sell_price1;
                if buy_on_1_sell_on_2 {
                    let gross_spread1 = sell_price1 - buy_price1;
                    let total_fee1 = (buy_price1 * fee1.taker_fee + sell_price1 * fee2.taker_fee) * two;
                    let net_profit1 = gross_spread1 - total_fee1;

                    if net_profit1 > Decimal::ZERO {
                        let gross_spread_percentage1 = (gross_spread1 / buy_price1) * hundred;
                        let net_spread_percentage1 = (net_profit1 / buy_price1) * hundred;

                        opportunities.push(ArbitrageOpportunity {
                            symbol: symbol.clone(),
                            buy_exchange: *exchange1,
                            sell_exchange: *exchange2,
                            buy_price: buy_price1,
                            sell_price: sell_price1,
                            gross_spread_percentage: gross_spread_percentage1,
                            net_spread_percentage: net_spread_percentage1,
                            estimated_profit_per_unit: net_profit1,
                            max_volume: Decimal::ZERO,
                            timestamp: current_time,
                        });
                    }
                }

                // Check if we can buy on exchange2 and sell on exchange1
                let buy_on_2_sell_on_1 = buy_price2 < sell_price2;
                if buy_on_2_sell_on_1 {
                    let gross_spread2 = sell_price2 - buy_price2;
                    let total_fee2 = (buy_price2 * fee2.taker_fee + sell_price2 * fee1.taker_fee) * two;
                    let net_profit2 = gross_spread2 - total_fee2;

                    if net_profit2 > Decimal::ZERO {
                        let gross_spread_percentage2 = (gross_spread2 / buy_price2) * hundred;
                        let net_spread_percentage2 = (net_profit2 / buy_price2) * hundred;

                        opportunities.push(ArbitrageOpportunity {
                            symbol: symbol.clone(),
                            buy_exchange: *exchange2,
                            sell_exchange: *exchange1,
                            buy_price: buy_price2,
                            sell_price: sell_price2,
                            gross_spread_percentage: gross_spread_percentage2,
                            net_spread_percentage: net_spread_percentage2,
                            estimated_profit_per_unit: net_profit2,
                            max_volume: Decimal::ZERO,
                            timestamp: current_time,
                        });
                    }
                }
            }
        }
    }

    opportunities.sort_by(|a, b| {
        b.net_spread_percentage
            .partial_cmp(&a.net_spread_percentage)
            .unwrap()
    });

    opportunities.iter().take(10).for_each(|o| {
        println!(
            "rest: {} – {} ({:.2}), buy: {:?}, sell: {:?}",
            o.symbol,
            o.estimated_profit_per_unit,
            o.net_spread_percentage,
            o.buy_exchange,
            o.sell_exchange
        );
    });

    println!("------------------------------------------------------------------");

    Ok(opportunities)
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
