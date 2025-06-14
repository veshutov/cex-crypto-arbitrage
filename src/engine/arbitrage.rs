use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::{
    exchanges::{ExchangeConfig, ExchangeName, OrderBook},
    Result,
};

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub symbol: String,
    pub buy_exchange: ExchangeName,
    pub sell_exchange: ExchangeName,
    pub buy_price: Decimal,
    pub sell_price: Decimal,
    pub gross_profit_percentage: Decimal,
    pub net_profit_percentage: Decimal,
    pub profit_per_unit: Decimal,
    pub max_quantity: Decimal,
    pub timestamp: u64,
}

pub async fn find_arbitrage_opportunity(
    exchange_configs: &HashMap<ExchangeName, ExchangeConfig>,
    buy_order_book: &OrderBook,
    sell_order_book: &OrderBook,
    order_book_max_age_ms: u64,
) -> Option<ArbitrageOpportunity> {
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Skip stale quotes
    if current_time.saturating_sub(buy_order_book.timestamp) > order_book_max_age_ms
        || current_time.saturating_sub(sell_order_book.timestamp) > order_book_max_age_ms
    {
        return None;
    }

    Some(calculate_arbitrage(
        exchange_configs,
        buy_order_book,
        sell_order_book,
    ))
}

pub async fn find_arbitrage_opportunities(
    market_data: &crate::engine::market_data::MarketData,
    exchange_configs: &HashMap<ExchangeName, ExchangeConfig>,
    symbol: &str,
    order_book_max_age_ms: u64,
) -> Result<Vec<ArbitrageOpportunity>> {
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let order_books = market_data.get_all_order_book_for_symbol(symbol);
    let mut opportunities = Vec::with_capacity(10);

    let order_books_count = order_books.len();

    for i in 0..order_books_count {
        for j in (i + 1)..order_books_count {
            let buy_order_book = &order_books[i];
            let sell_order_book = &order_books[j];

            // Skip stale quotes
            if current_time.saturating_sub(buy_order_book.timestamp) > order_book_max_age_ms
                || current_time.saturating_sub(sell_order_book.timestamp) > order_book_max_age_ms
            {
                continue;
            }

            // Check both directions
            let opp1 = calculate_arbitrage(exchange_configs, buy_order_book, sell_order_book);
            opportunities.push(opp1);

            let opp2 = calculate_arbitrage(exchange_configs, sell_order_book, buy_order_book);
            opportunities.push(opp2);
        }
    }

    // Sort by profitability
    opportunities.sort_by(|a, b| {
        b.net_profit_percentage
            .partial_cmp(&a.net_profit_percentage)
            .unwrap()
    });
    Ok(opportunities)
}

pub fn calculate_arbitrage(
    exchange_configs: &HashMap<ExchangeName, ExchangeConfig>,
    buy_order_book: &OrderBook,
    sell_order_book: &OrderBook,
) -> ArbitrageOpportunity {
    let buy_config = exchange_configs.get(&buy_order_book.exchange_name).unwrap();
    let sell_config = exchange_configs
        .get(&sell_order_book.exchange_name)
        .unwrap();

    let buy_price = buy_order_book.best_ask_price;
    let sell_price = sell_order_book.best_bid_price;

    // Pre-calculate common values
    let hundred = Decimal::from(100);
    let two = Decimal::from(2);
    let gross_spread = sell_price - buy_price;
    let total_fees = (buy_price * buy_config.taker_fee + sell_price * sell_config.taker_fee) * two;
    let net_profit = gross_spread - total_fees;

    // Calculate percentages based on total transaction value
    let total_transaction_value = buy_price + sell_price;
    let gross_profit_percentage = (gross_spread / total_transaction_value) * hundred;
    let net_profit_percentage = (net_profit / total_transaction_value) * hundred;

    // Calculate maximum tradeable volume
    let max_quantity = buy_order_book
        .best_ask_amount
        .min(sell_order_book.best_bid_amount);

    ArbitrageOpportunity {
        symbol: buy_order_book.symbol.clone(),
        buy_exchange: buy_order_book.exchange_name,
        sell_exchange: sell_order_book.exchange_name,
        buy_price,
        sell_price,
        gross_profit_percentage,
        net_profit_percentage,
        profit_per_unit: net_profit,
        max_quantity,
        timestamp: buy_order_book.timestamp.max(sell_order_book.timestamp),
    }
}
