use std::{collections::HashMap, sync::atomic::AtomicPtr, sync::Arc, fmt};

use chrono::{TimeZone, Utc, DateTime};
use rust_decimal::Decimal;

use crate::exchanges::{ExchangeName, OrderBook};

type Symbol = String;
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

    pub fn get_order_book(&self, exchange: &ExchangeName, symbol: &str) -> Option<&OrderBook> {
        let symbol_map = self.exchange_books.get(exchange)?;
        let quote_ptr = symbol_map.get(symbol)?;
        let quote_data = unsafe { &*quote_ptr.load(std::sync::atomic::Ordering::Acquire) };
        Some(quote_data)
    }

    pub fn get_all_order_book_for_symbol(&self, symbol: &str) -> Vec<OrderBook> {
        let exchanges = self.exchange_books.keys();
        let mut result = Vec::with_capacity(exchanges.len());

        for exchange in exchanges {
            if let Some(quote_ptr) = self
                .exchange_books
                .get(exchange)
                .and_then(|m| m.get(symbol))
            {
                let quote_data = unsafe { &*quote_ptr.load(std::sync::atomic::Ordering::Acquire) };
                result.push(quote_data.clone());
            }
        }

        result
    }

    pub fn get_best_prices(&self) -> BestPricesResult {
        let mut result = HashMap::new();
        
        for (exchange, symbol_map) in self.exchange_books.iter() {
            for (symbol, quote_ptr) in symbol_map.iter() {
                let order_book = unsafe { &*quote_ptr.load(std::sync::atomic::Ordering::Acquire) };
                
                result
                    .entry(symbol.clone())
                    .or_insert_with(Vec::new)
                    .push((*exchange, order_book.best_ask_price, order_book.best_bid_price, order_book.timestamp));
            }
        }
        
        BestPricesResult(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResult {
    Updated,
    Duplicate,
    Outdated,
}

#[derive(Debug)]
pub struct BestPricesResult(HashMap<String, Vec<(ExchangeName, Decimal, Decimal, u64)>>);

impl fmt::Display for BestPricesResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (symbol, prices) in &self.0 {
            writeln!(f, "{}:", symbol)?;
            for (exchange, ask, bid, time) in prices {
                writeln!(f, "  {:?}: bid={}, ask={}, time={:?}", exchange, bid, ask, Utc.timestamp_millis_opt(*time as i64).single().unwrap().time())?;
            }
        }
        Ok(())
    }
}
