use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::exchanges::{
    Exchange, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse, OrderSide,
    Position, SubscriptionConfig,
};

pub struct ExchangeGateway {
    pub exchanges: HashMap<ExchangeName, Box<dyn Exchange>>,
}

impl ExchangeGateway {
    pub fn new(exchanges: Vec<Box<dyn Exchange>>) -> Self {
        Self {
            exchanges: exchanges.into_iter().map(|e| (e.name(), e)).collect(),
        }
    }

    pub async fn subscribe_to_symbols(
        &self,
        symbols: Vec<String>,
    ) -> Result<mpsc::UnboundedReceiver<OrderBook>, ExchangeError> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let config = SubscriptionConfig {
            symbols: symbols.clone(),
        };

        for exchange in self.exchanges.values() {
            exchange
                .subscribe_orderbook(config.clone(), sender.clone())
                .await?;
        }

        Ok(receiver)
    }

    pub async fn place_order(
        &self,
        exchange_name: ExchangeName,
        order: OrderRequest,
    ) -> Result<OrderResponse, ExchangeError> {
        let exchange = self.exchanges.get(&exchange_name).ok_or_else(|| {
            ExchangeError::InternalError(format!("Exchange {:?} not found", exchange_name))
        })?;

        exchange.place_order(order).await
    }

    pub async fn close_position(
        &self,
        order_id: &str,
        exchange_name: ExchangeName,
        symbol: &str,
        side: OrderSide,
    ) -> Result<OrderResponse, ExchangeError> {
        let exchange = self.exchanges.get(&exchange_name).ok_or_else(|| {
            ExchangeError::InvalidResponse(format!("Exchange {:?} not found", exchange_name))
        })?;

        exchange.close_position(order_id, symbol, side).await
    }

    pub async fn get_open_positions(
        &self,
        exchange_name: ExchangeName,
    ) -> Result<Vec<Position>, ExchangeError> {
        let exchange = self.exchanges.get(&exchange_name).ok_or_else(|| {
            ExchangeError::InvalidResponse(format!("Exchange {:?} not found", exchange_name))
        })?;

        exchange.get_open_positions().await
    }
}
