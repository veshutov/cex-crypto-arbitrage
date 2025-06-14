use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    engine::ArbitrageOpportunity,
    exchanges::{gateway::ExchangeGateway, ExchangeName, OrderRequest, OrderSide, Position},
};

#[derive(Error, Debug)]
pub enum OrderManagerError {
    #[error("Failed to get positions from {exchange:?}: {message}")]
    PositionLoadError {
        exchange: ExchangeName,
        message: String,
    },
    #[error("Failed to place order: {message}")]
    OrderPlacementError { message: String },
    #[error("Failed to close position: {message}")]
    PositionCloseError { message: String },
}

#[derive(Clone)]
pub struct OrderManager {
    exchange_gateway: Arc<ExchangeGateway>,
    open_positions: Arc<DashMap<String, HashMap<ExchangeName, Position>>>,
}

impl OrderManager {
    pub fn new(exchange_gateway: Arc<ExchangeGateway>) -> Self {
        Self {
            exchange_gateway,
            open_positions: Arc::new(DashMap::new()),
        }
    }

    pub fn get_open_position_count(&self) -> usize {
        self.open_positions.len()
    }

    pub fn get_open_positions(&self) -> Arc<DashMap<String, HashMap<ExchangeName, Position>>> {
        self.open_positions.clone()
    }

    pub fn has_open_position(&self, symbol: &str) -> bool {
        self.open_positions.contains_key(symbol)
    }

    pub async fn load_positions(&self) -> Result<(), OrderManagerError> {
        for exchange_name in self.exchange_gateway.exchanges.keys() {
            match self
                .exchange_gateway
                .get_open_positions(*exchange_name)
                .await
            {
                Ok(positions) => {
                    for position in positions {
                        self.open_positions
                            .entry(position.symbol.clone())
                            .or_default()
                            .insert(*exchange_name, position);
                    }
                }
                Err(e) => {
                    return Err(OrderManagerError::PositionLoadError {
                        exchange: *exchange_name,
                        message: e.to_string(),
                    })
                }
            }
        }
        Ok(())
    }

    pub async fn place_orders(
        &self,
        opportunity: &ArbitrageOpportunity,
        quantity: i32,
    ) -> Result<(), OrderManagerError> {
        let buy_order = OrderRequest {
            id: Ulid::new().to_string(),
            symbol: opportunity.symbol.clone(),
            side: OrderSide::Buy,
            quantity,
        };
        let sell_order = OrderRequest {
            id: Ulid::new().to_string(),
            symbol: opportunity.symbol.clone(),
            side: OrderSide::Sell,
            quantity,
        };

        let (buy_result, sell_result) = tokio::join!(
            self.exchange_gateway
                .place_order(opportunity.buy_exchange, buy_order),
            self.exchange_gateway
                .place_order(opportunity.sell_exchange, sell_order)
        );

        match (buy_result, sell_result) {
            (Ok(_), Ok(_)) => {
                println!("Orders plased successfully {}", opportunity.symbol);
                self.record_position(opportunity, quantity);
                Ok(())
            }
            (Ok(_), Err(e)) => {
                println!("Buy order succeeded but sell failed, close the buy position {}", opportunity.symbol);
                if let Err(close_err) = self.exchange_gateway
                    .close_position(&Ulid::new().to_string(), opportunity.buy_exchange, &opportunity.symbol, OrderSide::Sell)
                    .await
                {
                    return Err(OrderManagerError::OrderPlacementError {
                        message: format!("Failed to place sell order: {}. Also failed to close buy order: {}", e, close_err),
                    });
                }
                Err(OrderManagerError::OrderPlacementError {
                    message: format!("Failed to place sell order: {}", e),
                })
            }
            (Err(e), Ok(_)) => {
                println!("Sell order succeeded but buy failed, close the sell position {}", opportunity.symbol);
                if let Err(close_err) = self.exchange_gateway
                    .close_position(&Ulid::new().to_string(), opportunity.sell_exchange, &opportunity.symbol, OrderSide::Buy)
                    .await
                {
                    return Err(OrderManagerError::OrderPlacementError {
                        message: format!("Failed to place buy order: {}. Also failed to close sell order: {}", e, close_err),
                    });
                }
                Err(OrderManagerError::OrderPlacementError {
                    message: format!("Failed to place buy order: {}", e),
                })
            }
            (Err(e1), Err(e2)) => Err(OrderManagerError::OrderPlacementError {
                message: format!("Both orders failed. Buy error: {}, Sell error: {}", e1, e2),
            }),
        }
    }

    pub async fn close_positions(
        &self,
        opportunity: &ArbitrageOpportunity,
    ) -> Result<(), OrderManagerError> {
        let buy_order_id = Ulid::new().to_string();
        let sell_order_id = Ulid::new().to_string();

        let (buy_result, sell_result) = tokio::join!(
            self.exchange_gateway.close_position(
                &buy_order_id,
                opportunity.buy_exchange,
                &opportunity.symbol,
                OrderSide::Buy
            ),
            self.exchange_gateway.close_position(
                &sell_order_id,
                opportunity.sell_exchange,
                &opportunity.symbol,
                OrderSide::Sell
            )
        );

        match (buy_result, sell_result) {
            (Ok(_), Ok(_)) => {
                println!("Orders closed successfully {}", opportunity.symbol);
                self.open_positions.remove(&opportunity.symbol);
                Ok(())
            }
            (Err(e), _) | (_, Err(e)) => Err(OrderManagerError::PositionCloseError {
                message: e.to_string(),
            }),
        }
    }

    fn record_position(&self, opportunity: &ArbitrageOpportunity, quantity: i32) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.open_positions.insert(
            opportunity.symbol.clone(),
            HashMap::from([
                (
                    opportunity.buy_exchange,
                    Position {
                        symbol: opportunity.symbol.clone(),
                        size: quantity,
                        entry_price: opportunity.buy_price,
                        entry_time: timestamp,
                        exchange_name: opportunity.buy_exchange,
                        side: OrderSide::Buy,
                    },
                ),
                (
                    opportunity.sell_exchange,
                    Position {
                        symbol: opportunity.symbol.clone(),
                        size: quantity,
                        entry_price: opportunity.sell_price,
                        entry_time: timestamp,
                        exchange_name: opportunity.sell_exchange,
                        side: OrderSide::Sell,
                    },
                ),
            ]),
        );
    }
}
