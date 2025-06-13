use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    engine::ArbitrageOpportunity,
    exchanges::{
        gateway::ExchangeGateway, ExchangeName, OrderRequest, OrderSide, Position,
    },
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
    pub fn new(
        exchange_gateway: Arc<ExchangeGateway>,
    ) -> Self {
        Self {
            exchange_gateway,
            open_positions: Arc::new(DashMap::new()),
        }
    }

    pub fn get_open_positions(&self) -> &DashMap<String, HashMap<ExchangeName, Position>> {
        &self.open_positions
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
            self.exchange_gateway.place_order(opportunity.buy_exchange, buy_order),
            self.exchange_gateway.place_order(opportunity.sell_exchange, sell_order)
        );

        match (buy_result, sell_result) {
            (Ok(_), Ok(_)) => {
                self.record_position(opportunity, quantity);
                Ok(())
            },
            (Err(e), _) | (_, Err(e)) => Err(OrderManagerError::OrderPlacementError {
                message: e.to_string(),
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
                self.open_positions.remove(&opportunity.symbol);
                Ok(())
            },
            (Err(e), _) | (_, Err(e)) => Err(OrderManagerError::PositionCloseError {
                message: e.to_string(),
            }),
        }
    }

    fn record_position(
        &self,
        opportunity: &ArbitrageOpportunity,
        quantity: i32,
    ) {
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
