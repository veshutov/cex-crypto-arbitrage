use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::exchanges::{Exchange, ExchangeError, ExchangeName, OrderBook, SubscriptionConfig, OrderRequest, OrderResponse, OrderSide};

pub struct ExchangeGateway {
    pub order_book_receiver: Option<mpsc::UnboundedReceiver<OrderBook>>,
    pub exchanges: HashMap<ExchangeName, Box<dyn Exchange>>,
    order_book_sender: mpsc::UnboundedSender<OrderBook>,
}

impl ExchangeGateway {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            exchanges: HashMap::new(),
            order_book_receiver: Some(receiver),
            order_book_sender: sender,
        }
    }

    pub fn add_exchange(&mut self, exchange: Box<dyn Exchange>) {
        let name = exchange.name();
        self.exchanges.insert(name, exchange);
    }

    pub async fn subscribe_to_symbols(
        &mut self,
        symbols: Vec<String>,
    ) -> Result<(), ExchangeError> {
        let config = SubscriptionConfig {
            symbols: symbols.clone(),
        };

        for adapter in self.exchanges.values_mut() {
            let sender = self.order_book_sender.clone();
            adapter.subscribe_orderbook(config.clone(), sender).await?;
        }

        Ok(())
    }

    pub async fn place_order(
        &self,
        exchange_name: ExchangeName,
        order: OrderRequest,
    ) -> Result<OrderResponse, ExchangeError> {
        let exchange = self.exchanges.get(&exchange_name)
            .ok_or_else(|| ExchangeError::InternalError(format!("Exchange {:?} not found", exchange_name)))?;

        exchange.place_order(order).await
    }

    pub async fn close_position(
        &self,
        exchange_name: ExchangeName,
        symbol: String,
        side: OrderSide,
    ) -> Result<OrderResponse, ExchangeError> {
        let exchange = self.exchanges.get(&exchange_name)
            .ok_or_else(|| ExchangeError::InvalidResponse(format!("Exchange {:?} not found", exchange_name)))?;

        exchange.close_position(&symbol, side).await
    }
}

impl Default for ExchangeGateway {
    fn default() -> Self {
        Self::new()
    }
}
