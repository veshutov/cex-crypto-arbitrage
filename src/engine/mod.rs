use rust_decimal::Decimal;

use crate::{
    engine::market_data::MarketData,
    exchanges::{gateway::ExchangeGateway, ExchangeName, OrderBookData},
    Result,
};

pub mod market_data;

pub struct Engine {
    pub market_data: MarketData,
    exchange_gateway: ExchangeGateway,
}

impl Engine {
    pub fn new(market_data: MarketData, exchange_gateway: ExchangeGateway) -> Engine {
        Engine {
            market_data,
            exchange_gateway,
        }
    }

    pub async fn start_order_book_processing(&mut self, symbols: Vec<String>) -> Result<()> {
        let market_data = self.market_data.clone();
        let mut order_book_receiver = self.exchange_gateway.order_book_receiver.take().unwrap();

        tokio::spawn(async move {
            while let Some(order_book) = order_book_receiver.recv().await {
                let _ = market_data.update_order_book(order_book).await;
            }
        });

        self.exchange_gateway.subscribe_to_symbols(symbols).await?;

        Ok(())
    }

    pub async fn find_arbitrage_opportunities(
        &self,
        symbol: &str,
        min_profit_percentage: Decimal,
        max_age_ms: u64,
    ) -> Result<Vec<ArbitrageOpportunity>> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let order_books = self.market_data.get_all_order_book_for_symbol(symbol).await;
        let mut opportunities = Vec::with_capacity(100);

        // Find arbitrage opportunities between all exchange pairs
        let exchanges: Vec<_> = order_books.keys().collect();
        let exchange_count = exchanges.len();

        for i in 0..exchange_count {
            for j in (i + 1)..exchange_count {
                let buy_exchange = exchanges[i];
                let sell_exchange = exchanges[j];

                if let (Some(buy_order_book), Some(sell_order_book)) = (
                    order_books.get(buy_exchange),
                    order_books.get(sell_exchange),
                ) {
                    // Skip stale quotes
                    if current_time.saturating_sub(buy_order_book.timestamp) > max_age_ms
                        || current_time.saturating_sub(sell_order_book.timestamp) > max_age_ms
                    {
                        continue;
                    }

                    // Check both directions
                    if let Some(opp) = self.calculate_arbitrage(
                        symbol,
                        *buy_exchange,
                        *sell_exchange,
                        buy_order_book,
                        sell_order_book,
                        min_profit_percentage,
                    ) {
                        opportunities.push(opp);
                    }

                    if let Some(opp) = self.calculate_arbitrage(
                        symbol,
                        *sell_exchange,
                        *buy_exchange,
                        sell_order_book,
                        buy_order_book,
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
        Ok(opportunities)
    }

    fn calculate_arbitrage(
        &self,
        symbol: &str,
        buy_exchange: ExchangeName,
        sell_exchange: ExchangeName,
        buy_order_book: &OrderBookData,
        sell_order_book: &OrderBookData,
        min_profit_percentage: Decimal,
    ) -> Option<ArbitrageOpportunity> {
        let buy_config = self.exchange_gateway.exchanges.get(&buy_exchange)?.config();
        let sell_config = self
            .exchange_gateway
            .exchanges
            .get(&sell_exchange)?
            .config();

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

        // Early return if not profitable enough
        let net_spread_percentage = (net_profit / buy_price) * hundred;
        if net_spread_percentage < min_profit_percentage {
            return None;
        }

        // Calculate maximum tradeable volume
        let max_volume = buy_order_book
            .best_ask_amount
            .min(sell_order_book.best_bid_amount);

        Some(ArbitrageOpportunity {
            symbol: symbol.to_string(),
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            gross_spread_percentage: (gross_spread / buy_price) * hundred,
            net_spread_percentage,
            estimated_profit_per_unit: net_profit,
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
    pub gross_spread_percentage: Decimal,
    pub net_spread_percentage: Decimal, // After fees
    pub estimated_profit_per_unit: Decimal,
    pub max_volume: Decimal,
    pub timestamp: u64,
}
