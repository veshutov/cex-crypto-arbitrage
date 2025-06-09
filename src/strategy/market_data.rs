use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::exchanges::{ExchangeName, OrderBookData};

#[derive(Clone, Debug)]
pub struct MarketData {
    tickers: Arc<DashMap<(String, ExchangeName), Arc<RwLock<OrderBookData>>>>,
}

impl MarketData {
    pub fn new() -> Self {
        Self {
            tickers: Arc::new(DashMap::new()),
        }
    }

    pub async fn update_order_book(&self, new_order_book: OrderBookData) -> UpdateResult {
        let key = (
            new_order_book.symbol.clone(),
            new_order_book.exchange_name.clone(),
        );

        match self.tickers.get(&key) {
            Some(quote_lock) => {
                let mut current = quote_lock.write().await;
                if new_order_book.timestamp > current.timestamp {
                    *current = new_order_book;
                    UpdateResult::Updated
                } else if new_order_book.timestamp == current.timestamp {
                    UpdateResult::Duplicate
                } else {
                    UpdateResult::Outdated
                }
            }
            None => {
                let quote_lock = Arc::new(RwLock::new(new_order_book));
                self.tickers.insert(key, quote_lock);
                UpdateResult::NewEntry
            }
        }
    }

    pub async fn get_order_book(
        &self,
        exchange: ExchangeName,
        symbol: &str,
    ) -> Option<OrderBookData> {
        let key = (symbol.to_string(), exchange);
        let quote_lock = self.tickers.get(&key)?;
        let quote_data = quote_lock.read().await;
        Some(quote_data.clone())
    }

    pub async fn get_all_order_book_for_symbol(
        &self,
        symbol: &str,
    ) -> HashMap<ExchangeName, OrderBookData> {
        let mut result = HashMap::new();

        for entry in self.tickers.iter() {
            if entry.key().0 == symbol {
                let exchange = &entry.key().1;
                let quote_data = entry.value().read().await;
                result.insert(exchange.clone(), quote_data.clone());
            }
        }

        result
    }
}

impl Default for MarketData {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum UpdateResult {
    Updated,
    NewEntry,
    Duplicate,
    Outdated,
}
