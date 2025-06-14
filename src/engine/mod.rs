use std::sync::Arc;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::{sync::mpsc, task::JoinHandle};

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
    pub max_open_positions: usize,
    pub order_book_max_age_ms: u64,
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

    pub async fn start_processing(&mut self, symbols: Vec<String>) -> Result<JoinHandle<()>> {
        self.order_manager.load_positions().await?;
        println!("Fetched opened positions");

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
        println!("Subscribed to order book updates");

        tokio::spawn(async move {
            'worker: while let Some(order_book) = order_book_receiver.recv().await {
                let symbol = order_book.symbol.clone();
                let update_result = market_data.update_order_book(order_book);

                if update_result == UpdateResult::Updated {
                    if let Some(positions) = order_manager.get_open_positions().get(&symbol) {
                        // check for position close
                        let buy_position = positions
                            .iter()
                            .find(|p| p.1.side == OrderSide::Buy)
                            .unwrap();
                        let sell_position = positions
                            .iter()
                            .find(|p| p.1.side == OrderSide::Sell)
                            .unwrap();

                        let buy_order_book =
                            market_data.get_order_book(buy_position.0, &symbol).unwrap();
                        let sell_order_book = market_data
                            .get_order_book(sell_position.0, &symbol)
                            .unwrap();

                        if let Some(opportunity) = find_arbitrage_opportunity(
                            &exchange_configs,
                            buy_order_book,
                            sell_order_book,
                            config.order_book_max_age_ms,
                        )
                        .await
                        {
                            let position_size = buy_position.1.size.min(sell_position.1.size);
                            let opportunity_quantity =
                                opportunity.max_quantity.trunc().to_i32().unwrap();
                            if opportunity.net_profit_percentage
                                < config.max_close_profit_percentage
                                && opportunity_quantity > position_size
                            {
                                match arbitrage_tx.send(ArbitrageEvent {
                                    opportunity: opportunity.clone(),
                                    action: ArbitrageOpportunityAction::Close,
                                }) {
                                    Ok(_) => {
                                        println!("Close event sent {:?}", opportunity.symbol)
                                    }
                                    Err(e) => {
                                        println!("Exiting engine order book worker {:?}", e);
                                        order_book_receiver.close();
                                        break 'worker;
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
                            config.order_book_max_age_ms,
                        )
                        .await
                        {
                            for opportunity in opportunities {
                                if opportunity.net_profit_percentage
                                    > config.min_open_profit_percentage
                                    && order_manager.get_open_position_count()
                                        < config.max_open_positions
                                {
                                    match arbitrage_tx.send(ArbitrageEvent {
                                        opportunity: opportunity.clone(),
                                        action: ArbitrageOpportunityAction::Open,
                                    }) {
                                        Ok(_) => {}
                                        Err(e) => {
                                            println!("Exiting engine order book worker {:?}", e);
                                            order_book_receiver.close();
                                            break 'worker;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let future = self.process_arbitrage_events(arbitrage_rx).await;
        println!("Started arbitrage events processor");

        Ok(future)
    }

    async fn process_arbitrage_events(
        &self,
        mut arbitrage_rc: mpsc::UnboundedReceiver<ArbitrageEvent>,
    ) -> JoinHandle<()> {
        let order_manager = self.order_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            'worker: while let Some(arbitrage_event) = arbitrage_rc.recv().await {
                let opportunity = arbitrage_event.opportunity;
                match arbitrage_event.action {
                    ArbitrageOpportunityAction::Close => {
                        if order_manager.has_open_position(&opportunity.symbol) {
                            match order_manager.close_positions(&opportunity).await {
                                Ok(_) => {
                                    println!("Closed positions for {:?}", opportunity);
                                }
                                Err(e) => {
                                    println!(
                                        "Error closing positions, exiting worker {:?} {}",
                                        opportunity, e
                                    );
                                    arbitrage_rc.close();
                                    break 'worker;
                                }
                            };
                        }
                    }
                    ArbitrageOpportunityAction::Open => {
                        if !order_manager.has_open_position(&opportunity.symbol) {
                            let price = opportunity.buy_price.min(opportunity.sell_price);
                            let position_value = opportunity.max_quantity * price;

                            if position_value < config.min_position_value {
                                println!("Not enough volume for {:?}", opportunity.symbol);
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

                            match order_manager.place_orders(&opportunity, quantity).await {
                                Ok(_) => {
                                    println!("Placed orders for {}, {:?}", quantity, opportunity);
                                }
                                Err(e) => {
                                    println!(
                                        "Error placing orders, exiting worker {} {:?} {}",
                                        quantity, opportunity, e
                                    );
                                    arbitrage_rc.close();
                                    break 'worker;
                                }
                            };
                        }
                    }
                }
            }
        })
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
