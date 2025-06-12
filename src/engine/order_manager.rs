use std::collections::HashMap;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use ulid::Ulid;

use crate::exchanges::{Exchange, ExchangeName, OrderBook, OrderRequest, OrderResponse, OrderSide, Position};

pub struct OrderManager {
    exchanges: HashMap<ExchangeName, Box<dyn Exchange>>,
    open_positions: HashMap<String, Position>,
    min_profit_threshold: Decimal,
    max_position_size: i32,
}

// impl OrderManager {
//     pub fn new(
//         exchanges: HashMap<ExchangeName, Box<dyn Exchange>>,
//         min_profit_threshold: Decimal,
//         max_position_size: i32,
//     ) -> Self {
//         Self {
//             exchanges,
//             open_positions: HashMap::new(),
//             min_profit_threshold,
//             max_position_size,
//         }
//     }

//     pub async fn process_orderbook(&mut self, orderbook: OrderBook) -> Result<(), String> {
//         // Get all orderbooks for the same symbol from other exchanges
//         let mut other_orderbooks = Vec::new();
//         for (exchange_name, exchange) in &self.exchanges {
//             if *exchange_name != orderbook.exchange_name {
//                 // Here we would need to get the current orderbook from the exchange
//                 // For now, we'll just use the one we received
//                 other_orderbooks.push(orderbook.clone());
//             }
//         }

//         // Check for arbitrage opportunities
//         for other_orderbook in other_orderbooks {
//             let price_diff = if orderbook.best_bid_price > other_orderbook.best_ask_price {
//                 orderbook.best_bid_price - other_orderbook.best_ask_price
//             } else if other_orderbook.best_bid_price > orderbook.best_ask_price {
//                 other_orderbook.best_bid_price - orderbook.best_ask_price
//             } else {
//                 Decimal::ZERO
//             };

//             if price_diff > self.min_profit_threshold {
//                 // Found arbitrage opportunity
//                 let (buy_exchange, sell_exchange, buy_price, sell_price) = if orderbook.best_bid_price > other_orderbook.best_ask_price {
//                     (other_orderbook.exchange_name, orderbook.exchange_name, other_orderbook.best_ask_price, orderbook.best_bid_price)
//                 } else {
//                     (orderbook.exchange_name, other_orderbook.exchange_name, orderbook.best_ask_price, other_orderbook.best_bid_price)
//                 };

//                 // Check if we already have a position
//                 let position_key = format!("{:?}_{:?}", buy_exchange, sell_exchange);
//                 if !self.open_positions.contains_key(&position_key) {
//                     // Open new position
//                     let order_id = Ulid::new().to_string();
//                     let quantity = self.max_position_size.min(
//                         orderbook.best_bid_amount.min(other_orderbook.best_ask_amount) as i32
//                     );

//                     // Place buy order
//                     let buy_order = OrderRequest {
//                         id: format!("{}_{}", order_id, "buy"),
//                         symbol: orderbook.symbol.clone(),
//                         side: OrderSide::Buy,
//                         quantity,
//                     };

//                     // Place sell order
//                     let sell_order = OrderRequest {
//                         id: format!("{}_{}", order_id, "sell"),
//                         symbol: orderbook.symbol.clone(),
//                         side: OrderSide::Sell,
//                         quantity,
//                     };

//                     if let (Ok(buy_response), Ok(sell_response)) = (
//                         self.exchanges.get(&buy_exchange).unwrap().place_order(buy_order).await,
//                         self.exchanges.get(&sell_exchange).unwrap().place_order(sell_order).await,
//                     ) {
//                         // Record the position
//                         self.open_positions.insert(position_key, Position {
//                             symbol: orderbook.symbol.clone(),
//                             size: quantity,
//                             entry_price: buy_price,
//                             mark_price: sell_price,
//                             unrealized_pnl: (sell_price - buy_price) * Decimal::from(quantity),
//                             realized_pnl: Decimal::ZERO,
//                         });
//                     }
//                 } else {
//                     // Check if we should close the position
//                     let position = self.open_positions.get(&position_key).unwrap();
//                     let current_profit = (sell_price - buy_price) * Decimal::from(position.size);
                    
//                     if current_profit < position.unrealized_pnl {
//                         // Close the position as the profit has decreased
//                         if let (Ok(_), Ok(_)) = (
//                             self.exchanges.get(&buy_exchange).unwrap().close_position(
//                                 &format!("{}_{}", position_key, "buy"),
//                                 &orderbook.symbol,
//                                 OrderSide::Sell,
//                             ).await,
//                             self.exchanges.get(&sell_exchange).unwrap().close_position(
//                                 &format!("{}_{}", position_key, "sell"),
//                                 &orderbook.symbol,
//                                 OrderSide::Buy,
//                             ).await,
//                         ) {
//                             self.open_positions.remove(&position_key);
//                         }
//                     }
//                 }
//             }
//         }

//         Ok(())
//     }

//     pub async fn update_positions(&mut self) -> Result<(), String> {
//         for (exchange_name, exchange) in &self.exchanges {
//             match exchange.get_open_positions().await {
//                 Ok(positions) => {
//                     for position in positions {
//                         let position_key = format!("{}_{}", exchange_name, position.symbol);
//                         self.open_positions.insert(position_key, position);
//                     }
//                 }
//                 Err(e) => return Err(format!("Failed to get positions from {}: {}", exchange_name, e)),
//             }
//         }
//         Ok(())
//     }

//     pub fn get_open_positions(&self) -> &HashMap<String, Position> {
//         &self.open_positions
//     }
// } 