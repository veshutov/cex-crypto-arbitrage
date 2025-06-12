use std::collections::HashMap;

use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::{
    engine::market_data::{MarketData, UpdateResult},
    exchanges::{gateway::ExchangeGateway, ExchangeConfig, ExchangeName, OrderBook},
    Result,
};

pub mod market_data;
pub mod order_manager;

pub struct ArbitrageEngine {
    pub market_data: MarketData,
    exchange_gateway: ExchangeGateway,
}

impl ArbitrageEngine {
    pub fn new(market_data: MarketData, exchange_gateway: ExchangeGateway) -> ArbitrageEngine {
        ArbitrageEngine {
            market_data,
            exchange_gateway,
        }
    }

    pub async fn start_processing(
        &mut self, 
        symbols: Vec<String>,
        min_profit_percentage: Decimal,
        order_book_max_age_ms: u64,
    ) -> Result<mpsc::Receiver<ArbitrageOpportunity>> {
        let (arbitrage_tx, arbitrage_rx) = mpsc::channel::<ArbitrageOpportunity>(1000);
        let mut order_book_receiver = self.exchange_gateway.order_book_receiver.take().unwrap();
        
        let market_data = self.market_data.clone();
        let exchange_configs = self.exchange_gateway
            .exchanges
            .iter()
            .map(|(name, exchange)| (*name, exchange.config()))
            .collect();

        tokio::spawn(async move {
            while let Some(order_book) = order_book_receiver.recv().await {
                let symbol = order_book.symbol.clone();
                
                // Update the order book and check if it was actually updated
                let update_result: UpdateResult = market_data.update_order_book(order_book);
                if update_result == UpdateResult::Updated {
                    // Order book was updated, check for arbitrage opportunities
                    if let Ok(opportunities) = Self::find_arbitrage_opportunities(
                        &market_data,
                        &exchange_configs,
                        &symbol,
                        min_profit_percentage,
                        order_book_max_age_ms,
                    ).await {
                        // Send each opportunity to the channel
                        for opportunity in opportunities {
                            if arbitrage_tx.send(opportunity).await.is_err() {
                                println!("Engine receiver has been dropped, exit the loop");
                                break;
                            }
                        }
                    }
                }
            }
        });

        self.exchange_gateway.subscribe_to_symbols(symbols).await?;

        Ok(arbitrage_rx)
    }

    async fn find_arbitrage_opportunities(
        market_data: &MarketData,
        exchange_configs: &HashMap<ExchangeName, ExchangeConfig>,
        symbol: &str,
        min_profit_percentage: Decimal,
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
                if let Some(opp) = Self::calculate_arbitrage(
                    exchange_configs,
                    symbol,
                    buy_order_book,
                    sell_order_book,
                    min_profit_percentage,
                ) {
                    opportunities.push(opp);
                }

                if let Some(opp) = Self::calculate_arbitrage(
                    exchange_configs,
                    symbol,
                    sell_order_book,
                    buy_order_book,
                    min_profit_percentage,
                ) {
                    opportunities.push(opp);
                }
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

    fn calculate_arbitrage(
        exchange_configs: &HashMap<ExchangeName, ExchangeConfig>,
        symbol: &str,
        buy_order_book: &OrderBook,
        sell_order_book: &OrderBook,
        min_profit_percentage: Decimal,
    ) -> Option<ArbitrageOpportunity> {
        let buy_config = exchange_configs.get(&buy_order_book.exchange_name)?;
        let sell_config = exchange_configs.get(&sell_order_book.exchange_name)?;

        let buy_price = buy_order_book.best_ask_price;
        let sell_price = sell_order_book.best_bid_price;

        if sell_price <= buy_price {
            return None; // No arbitrage opportunity
        }

        // Pre-calculate common values
        let hundred = Decimal::from(100);
        let two = Decimal::from(2);

        // Pre-calculate common values
        let gross_spread = sell_price - buy_price;
        let total_fees =
            (buy_price * buy_config.taker_fee + sell_price * sell_config.taker_fee) * two;
        let net_profit = gross_spread - total_fees;

        // Calculate percentages based on total transaction value
        let total_transaction_value = buy_price + sell_price;
        let gross_profit_percentage = (gross_spread / total_transaction_value) * hundred;
        let net_profit_percentage = (net_profit / total_transaction_value) * hundred;

        // Early return if not profitable enough
        if net_profit_percentage < min_profit_percentage {
            return None;
        }

        // Calculate maximum tradeable volume
        let max_volume = buy_order_book
            .best_ask_amount
            .min(sell_order_book.best_bid_amount);

        Some(ArbitrageOpportunity {
            symbol: symbol.to_string(),
            buy_exchange: buy_order_book.exchange_name,
            sell_exchange: sell_order_book.exchange_name,
            buy_price,
            sell_price,
            gross_profit_percentage,
            net_profit_percentage,
            profit_per_unit: net_profit,
            max_volume,
            timestamp: buy_order_book.timestamp.max(sell_order_book.timestamp),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub symbol: String,
    pub buy_exchange: ExchangeName,
    pub sell_exchange: ExchangeName,
    pub buy_price: Decimal,  // Ask price on buy exchange
    pub sell_price: Decimal, // Bid price on sell exchange
    pub gross_profit_percentage: Decimal,
    pub net_profit_percentage: Decimal, // After fees
    pub profit_per_unit: Decimal,
    pub max_volume: Decimal,
    pub timestamp: u64,
}