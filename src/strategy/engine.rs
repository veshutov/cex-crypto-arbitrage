use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::{
    exchanges::{gateway::ExchangeGateway, ExchangeName, OrderBookData},
    strategy::market_data::MarketData,
};

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub symbol: String,
    pub buy_exchange: ExchangeName,
    pub sell_exchange: ExchangeName,
    pub buy_price: Decimal,  // Ask price on buy exchange
    pub sell_price: Decimal, // Bid price on sell exchange
    pub gross_spread_percentage: Decimal,
    pub net_spread_percentage: Decimal, // After fees
    pub estimated_profit_per_unit: Decimal,
    pub max_volume: Decimal,
    pub timestamp: u64,
}

pub struct ArbitrageEngine {
    market_data: MarketData,
    exchange_gateway: ExchangeGateway,
}

impl ArbitrageEngine {
    pub fn new(market_data: MarketData, exchange_gateway: ExchangeGateway) -> ArbitrageEngine {
        ArbitrageEngine {
            market_data,
            exchange_gateway,
        }
    }

    pub async fn start_order_book_processing(&mut self, symbols: Vec<String>) {
        let market_data = self.market_data.clone();
        let mut order_book_receiver = self.exchange_gateway.order_book_receiver.take().unwrap();

        tokio::spawn(async move {
            while let Some(order_book) = order_book_receiver.recv().await {
                let _ = market_data.update_order_book(order_book).await;
            }
        });

        self.exchange_gateway
            .subscribe_to_symbols(symbols)
            .await
            .unwrap();
    }

    pub async fn find_arbitrage_opportunities(
        &self,
        symbol: &str,
        min_profit_percentage: Decimal,
        max_age_ms: u64,
    ) -> Vec<ArbitrageOpportunity> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let order_books = self.market_data.get_all_order_book_for_symbol(symbol).await;
        let mut opportunities = Vec::new();

        // Filter out stale quotes
        let fresh_quotes: HashMap<ExchangeName, OrderBookData> = order_books
            .into_iter()
            .filter(|(_, quote)| current_time - quote.timestamp <= max_age_ms)
            .collect();

        if fresh_quotes.len() < 2 {
            return opportunities;
        }

        // Find arbitrage opportunities between all exchange pairs
        let exchanges: Vec<_> = fresh_quotes.keys().collect();

        for i in 0..exchanges.len() {
            for j in (i + 1)..exchanges.len() {
                let buy_exchange = exchanges[i];
                let sell_exchange = exchanges[j];

                if let (Some(buy_quote), Some(sell_quote)) = (
                    fresh_quotes.get(buy_exchange),
                    fresh_quotes.get(sell_exchange),
                ) {
                    // Check both directions
                    if let Some(opp) = self.calculate_arbitrage(
                        symbol,
                        buy_exchange,
                        sell_exchange,
                        buy_quote,
                        sell_quote,
                        min_profit_percentage,
                    ) {
                        opportunities.push(opp);
                    }

                    if let Some(opp) = self.calculate_arbitrage(
                        symbol,
                        sell_exchange,
                        buy_exchange,
                        sell_quote,
                        buy_quote,
                        min_profit_percentage,
                    ) {
                        opportunities.push(opp);
                    }
                }
            }
        }

        // Sort by profitability
        opportunities.sort_by(|a, b| {
            b.net_spread_percentage
                .partial_cmp(&a.net_spread_percentage)
                .unwrap()
        });
        opportunities
    }

    fn calculate_arbitrage(
        &self,
        symbol: &str,
        buy_exchange: &ExchangeName,
        sell_exchange: &ExchangeName,
        buy_order_book: &OrderBookData,
        sell_order_book: &OrderBookData,
        min_profit_percentage: Decimal,
    ) -> Option<ArbitrageOpportunity> {
        let buy_config = self.exchange_gateway.exchanges.get(buy_exchange)?.config();
        let sell_config = self.exchange_gateway.exchanges.get(sell_exchange)?.config();

        let buy_price = buy_order_book.best_ask_price; // We buy at ask price
        let sell_price = sell_order_book.best_bid_price; // We sell at bid price

        if sell_price <= buy_price {
            return None; // No arbitrage opportunity
        }

        // Calculate gross spread
        let gross_spread = sell_price - buy_price;
        let gross_spread_percentage = (gross_spread / buy_price) * Decimal::from(100);

        // Calculate fees
        let buy_fee = buy_price * buy_config.taker_fee;
        let sell_fee = sell_price * sell_config.taker_fee;

        let total_fees = (buy_fee + sell_fee) * Decimal::from(2);
        let net_profit = gross_spread - total_fees;
        let net_spread_percentage = (net_profit / buy_price) * Decimal::from(100);

        if net_spread_percentage < min_profit_percentage {
            return None;
        }

        // Calculate maximum tradeable volume
        let max_volume = buy_order_book
            .best_ask_amount
            .min(sell_order_book.best_bid_amount);

        Some(ArbitrageOpportunity {
            symbol: symbol.to_string(),
            buy_exchange: buy_exchange.clone(),
            sell_exchange: sell_exchange.clone(),
            buy_price,
            sell_price,
            gross_spread_percentage,
            net_spread_percentage,
            estimated_profit_per_unit: net_profit,
            max_volume,
            timestamp: buy_order_book.timestamp.max(sell_order_book.timestamp),
        })
    }
}
