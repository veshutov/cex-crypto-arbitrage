use std::{collections::HashMap, sync::atomic::AtomicPtr, sync::Arc};

use crate::exchanges::{ExchangeName, OrderBook};

// Since we have a small number of exchanges, we can use a more efficient key structure
type Symbol = String; // Just the symbol
type OrderBookValue = AtomicPtr<OrderBook>;

#[derive(Clone, Debug)]
pub struct MarketData {
    exchange_books: Arc<HashMap<ExchangeName, HashMap<Symbol, OrderBookValue>>>,
}

impl MarketData {
    pub fn new(exchanges: &[ExchangeName], symbols: &[String]) -> Self {
        let mut exchange_books = HashMap::with_capacity(exchanges.len());
        // Pre-allocate for provided exchanges
        for exchange in exchanges {
            let mut symbol_map = HashMap::with_capacity(symbols.len());
            for symbol in symbols {
                let new_data =
                    Box::into_raw(Box::new(OrderBook::initial(*exchange, symbol.clone())));
                let quote_ptr = AtomicPtr::new(new_data);
                symbol_map.insert(symbol.clone(), quote_ptr);
            }
            exchange_books.insert(*exchange, symbol_map);
        }
        Self {
            exchange_books: Arc::new(exchange_books),
        }
    }

    pub fn update_order_book(&self, new_order_book: OrderBook) -> UpdateResult {
        let exchange = new_order_book.exchange_name;
        let symbol = new_order_book.symbol.clone();

        let symbol_map = self
            .exchange_books
            .get(&exchange)
            .expect("Unknown exchange");

        match symbol_map.get(&symbol) {
            Some(quote_ptr) => {
                let current = unsafe { &*quote_ptr.load(std::sync::atomic::Ordering::Acquire) };
                match new_order_book.timestamp.cmp(&current.timestamp) {
                    std::cmp::Ordering::Greater => {
                        let new_data = Box::into_raw(Box::new(new_order_book));
                        let old_ptr = quote_ptr.swap(new_data, std::sync::atomic::Ordering::AcqRel);
                        if !old_ptr.is_null() {
                            unsafe { drop(Box::from_raw(old_ptr)) };
                        }
                        UpdateResult::Updated
                    }
                    std::cmp::Ordering::Equal => UpdateResult::Duplicate,
                    std::cmp::Ordering::Less => UpdateResult::Outdated,
                }
            }
            None => {
                panic!("Unknown symbol {}", symbol)
            }
        }
    }

    pub fn get_order_book(&self, exchange: ExchangeName, symbol: &str) -> Option<&OrderBook> {
        let symbol_map = self.exchange_books.get(&exchange)?;
        let quote_ptr = symbol_map.get(symbol)?;
        let quote_data = unsafe { &*quote_ptr.load(std::sync::atomic::Ordering::Acquire) };
        Some(quote_data)
    }

    pub fn get_all_order_book_for_symbol(&self, symbol: &str) -> HashMap<ExchangeName, &OrderBook> {
        let mut result = HashMap::with_capacity(10); // Pre-allocate for max exchanges

        for exchange in self.exchange_books.keys() {
            if let Some(quote_data) = self.get_order_book(*exchange, symbol) {
                result.insert(*exchange, quote_data);
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
