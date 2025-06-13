use std::sync::Arc;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::{
    engine::{
        arbitrage::{
            find_arbitrage_opportunities, find_arbitrage_opportunity, ArbitrageOpportunity,
        },
        market_data::{MarketData, UpdateResult},
        order_manager::OrderManager,
    },
    exchanges::{gateway::ExchangeGateway, OrderSide},
    Result,
};

pub mod arbitrage;
pub mod market_data;
pub mod order_manager;

#[derive(Debug, Clone)]
pub struct ArbitrageEngineConfig {
    pub min_open_profit_percentage: Decimal,
    pub max_close_profit_percentage: Decimal,
    pub min_position_value: Decimal,
    pub max_position_value: Decimal,
}

pub struct ArbitrageEngine {
    market_data: MarketData,
    exchange_gateway: Arc<ExchangeGateway>,
    order_manager: OrderManager,
    config: ArbitrageEngineConfig,
}

impl ArbitrageEngine {
    pub fn new(
        market_data: MarketData,
        exchange_gateway: Arc<ExchangeGateway>,
        order_manager: OrderManager,
        config: ArbitrageEngineConfig,
    ) -> ArbitrageEngine {
        ArbitrageEngine {
            market_data,
            exchange_gateway,
            order_manager,
            config,
        }
    }

    pub async fn start_processing(
        &mut self,
        symbols: Vec<String>,
        order_book_max_age_ms: u64,
    ) -> Result<()> {
        self.order_manager.load_positions().await?;

        let (arbitrage_tx, arbitrage_rx) = mpsc::unbounded_channel::<ArbitrageEvent>();

        let market_data = self.market_data.clone();
        let order_manager = self.order_manager.clone();
        let config: ArbitrageEngineConfig = self.config.clone();
        let exchange_configs = self
            .exchange_gateway
            .exchanges
            .iter()
            .map(|(name, exchange)| (*name, exchange.config()))
            .collect();

        let mut order_book_receiver = self.exchange_gateway.subscribe_to_symbols(symbols).await?;

        tokio::spawn(async move {
            while let Some(order_book) = order_book_receiver.recv().await {
                let symbol = order_book.symbol.clone();
                let update_result = market_data.update_order_book(order_book);

                if update_result == UpdateResult::Updated {
                    let open_positions = order_manager.get_open_positions();
                    if let Some(positions) = open_positions.get(&symbol) {
                        // check for position close
                        let buy_exchange = positions
                            .iter()
                            .find(|p| p.1.side == OrderSide::Buy)
                            .unwrap();
                        let sell_exchange = positions
                            .iter()
                            .find(|p| p.1.side == OrderSide::Sell)
                            .unwrap();

                        let buy_order_book =
                            market_data.get_order_book(buy_exchange.0, &symbol).unwrap();
                        let sell_order_book = market_data
                            .get_order_book(sell_exchange.0, &symbol)
                            .unwrap();

                        if let Some(opportunity) = find_arbitrage_opportunity(
                            &exchange_configs,
                            buy_order_book,
                            sell_order_book,
                            order_book_max_age_ms,
                        )
                        .await
                        {
                            if opportunity.net_profit_percentage
                                < config.max_close_profit_percentage
                            {
                                match arbitrage_tx.send(ArbitrageEvent {
                                    opportunity,
                                    action: ArbitrageOpportunityAction::Close,
                                }) {
                                    Ok(_) => {
                                        println!("Close event sent {:?}", symbol)
                                    }
                                    Err(e) => {
                                        println!(
                                            "Engine receiver has been dropped, exit the loop {:?}",
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // check for arbitrage open
                        if let Ok(opportunities) = find_arbitrage_opportunities(
                            &market_data,
                            &exchange_configs,
                            &symbol,
                            order_book_max_age_ms,
                        )
                        .await
                        {
                            for opportunity in opportunities {
                                if opportunity.net_profit_percentage
                                    > config.min_open_profit_percentage
                                {
                                    match arbitrage_tx.send(ArbitrageEvent {
                                        opportunity,
                                        action: ArbitrageOpportunityAction::Open,
                                    }) {
                                        Ok(_) => {
                                            println!("Open event sent {:?}", symbol)
                                        }
                                        Err(e) => {
                                            println!(
                                            "Engine receiver has been dropped, exit the loop {:?}",
                                            e
                                        );
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        self.process_arbitrage_events(arbitrage_rx).await?;

        Ok(())
    }

    async fn process_arbitrage_events(
        &self,
        mut arbitrage_rc: mpsc::UnboundedReceiver<ArbitrageEvent>,
    ) -> Result<()> {
        let order_manager = self.order_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            while let Some(arbitrage_event) = arbitrage_rc.recv().await {
                let opportunity = arbitrage_event.opportunity;
                match arbitrage_event.action {
                    ArbitrageOpportunityAction::Close => {
                        if order_manager.has_open_position(&opportunity.symbol) {
                            order_manager
                                .close_positions(&opportunity)
                                .await
                                .unwrap();
                        }
                    }
                    ArbitrageOpportunityAction::Open => {
                        if !order_manager.has_open_position(&opportunity.symbol) {
                            let price = opportunity.buy_price.min(opportunity.sell_price);
                            let position_value = opportunity.max_quantity * price;

                            if position_value < config.min_position_value {
                                continue;
                            }

                            let quantity = if position_value > config.max_position_value {
                                (config.max_position_value / price)
                                    .trunc()
                                    .to_i32()
                                    .unwrap()
                            } else {
                                opportunity.max_quantity.trunc().to_i32().unwrap()
                            };

                            order_manager
                                .place_orders(&opportunity, quantity)
                                .await
                                .unwrap();
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ArbitrageEvent {
    opportunity: ArbitrageOpportunity,
    action: ArbitrageOpportunityAction,
}

#[derive(Debug, Clone)]
enum ArbitrageOpportunityAction {
    Close,
    Open,
}
