use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{exchanges::{ArbitrageOpportunity, Exchange, OrderType}, AppState};

#[derive(Debug, Deserialize)]
pub struct ArbitrageQuery {
    exchange1_order_type: OrderType,
    exchange2_order_type: OrderType,
}

#[derive(Debug, Serialize)]
pub struct ArbitrageResponse {
    opportunities: Vec<ArbitrageOpportunity>,
}

pub async fn get_arbitrage_opportunities(
    State(state): State<AppState>,
    Query(query): Query<ArbitrageQuery>,
) -> Json<ArbitrageResponse> {
    let mut opportunities = Vec::new();

    // Get all available tickers from both exchanges
    let mut all_tickers = Vec::new();
    let exchanges = state.exchanges;
    for exchange in exchanges.iter() {
        if let Ok(tickers) = exchange.get_futures_tickers().await {
            all_tickers.extend(tickers);
        }
    }
    all_tickers.sort();
    all_tickers.dedup();

    // Check each ticker for arbitrage opportunities
    for ticker in all_tickers {
        let mut prices = Vec::new();
        
        for exchange in exchanges.iter() {
            if let Ok(price) = exchange.get_ticker_price(&ticker).await {
                let fee = exchange.get_fees(if exchange.name() == "bybit" {
                    query.exchange1_order_type
                } else {
                    query.exchange2_order_type
                });
                
                prices.push((exchange.name(), price, fee));
            }
        }

        // Compare prices between exchanges
        for i in 0..prices.len() {
            for j in (i + 1)..prices.len() {
                let (exchange1, price1, fee1) = &prices[i];
                let (exchange2, price2, fee2) = &prices[j];

                let total_fee = (price1 * fee1.taker_fee) + (price2 * fee2.taker_fee);
                let price_diff = (price1 - price2).abs();

                if price_diff > total_fee {
                    let (buy_exchange, buy_price, sell_exchange, sell_price) = if price1 < price2 {
                        (exchange1, price1, exchange2, price2)
                    } else {
                        (exchange2, price2, exchange1, price1)
                    };

                    opportunities.push(ArbitrageOpportunity {
                        symbol: ticker.clone(),
                        buy_exchange: buy_exchange.to_string(),
                        sell_exchange: sell_exchange.to_string(),
                        buy_price: *buy_price,
                        sell_price: *sell_price,
                        potential_profit: price_diff - total_fee,
                        total_fees: total_fee,
                    });
                }
            }
        }
    }

    Json(ArbitrageResponse { opportunities })
} 