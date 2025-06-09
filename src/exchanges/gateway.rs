use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::exchanges::{Exchange, ExchangeError, ExchangeName, OrderBookData, SubscriptionConfig};

pub struct ExchangeGateway {
    pub order_book_receiver: Option<mpsc::UnboundedReceiver<OrderBookData>>,
    pub exchanges: HashMap<ExchangeName, Box<dyn Exchange>>,
    order_book_sender: mpsc::UnboundedSender<OrderBookData>,
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
}

impl Default for ExchangeGateway {
    fn default() -> Self {
        Self::new()
    }
}
