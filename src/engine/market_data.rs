use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::exchanges::{ExchangeName, OrderBookData};

// Since we have a small number of exchanges, we can use a more efficient key structure
type OrderBookKey = String; // Just the symbol
type OrderBookValue = Arc<RwLock<OrderBookData>>;
type OrderBookMap = Arc<DashMap<OrderBookKey, OrderBookValue>>;

#[derive(Clone, Debug)]
pub struct MarketData {
    exchange_books: Arc<DashMap<ExchangeName, OrderBookMap>>,
}

impl MarketData {
    pub fn new(exchanges: &[ExchangeName]) -> Self {
        let exchange_books = Arc::new(DashMap::new());
        // Pre-allocate for provided exchanges
        for exchange in exchanges {
            let symbol_map = Arc::new(DashMap::with_capacity(500)); // Pre-allocate for max symbols
            exchange_books.insert(*exchange, symbol_map);
        }
        Self { exchange_books }
    }

    pub async fn update_order_book(&self, new_order_book: OrderBookData) -> UpdateResult {
        let exchange = new_order_book.exchange_name;
        let symbol = new_order_book.symbol.clone();

        let symbol_map = self
            .exchange_books
            .get(&exchange)
            .expect("Unknown exchange");
        let update_result = match symbol_map.get(&symbol) {
            Some(quote_lock) => {
                let mut current = quote_lock.write().await;
                match new_order_book.timestamp.cmp(&current.timestamp) {
                    std::cmp::Ordering::Greater => {
                        *current = new_order_book;
                        UpdateResult::Updated
                    }
                    std::cmp::Ordering::Equal => UpdateResult::Duplicate,
                    std::cmp::Ordering::Less => UpdateResult::Outdated,
                }
            }
            None => {
                let quote_lock = Arc::new(RwLock::new(new_order_book));
                symbol_map.insert(symbol, quote_lock);
                UpdateResult::NewEntry
            }
        };
        update_result
    }

    pub async fn get_order_book(
        &self,
        exchange: ExchangeName,
        symbol: &str,
    ) -> Option<OrderBookData> {
        let symbol_map = self.exchange_books.get(&exchange)?;
        let quote_lock = symbol_map.get(symbol)?;
        let quote_data = quote_lock.read().await;
        Some(quote_data.clone())
    }

    pub async fn get_all_order_book_for_symbol(
        &self,
        symbol: &str,
    ) -> HashMap<ExchangeName, OrderBookData> {
        let mut result = HashMap::with_capacity(10); // Pre-allocate for max exchanges

        for entry in self.exchange_books.iter() {
            let exchange = *entry.key();
            if let Some(quote_lock) = entry.value().get(symbol) {
                let quote_data = quote_lock.read().await;
                result.insert(exchange, quote_data.clone());
            }
        }

        result
    }
}

#[derive(Debug, Clone)]
pub enum UpdateResult {
    Updated,
    NewEntry,
    Duplicate,
    Outdated,
}
