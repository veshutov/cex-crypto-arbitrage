use std::time::Duration;
use rust_decimal::Decimal;

use tokio::time::sleep;

use crate::exchanges::bybit::BybitExchange;
use crate::exchanges::gate::GateExchange;
use crate::exchanges::gateway::ExchangeGateway;
use crate::exchanges::kucoin::KucoinExchange;
use crate::exchanges::Exchange;
use crate::strategy::engine::ArbitrageEngine;
use crate::strategy::market_data::MarketData;
use crate::Config;

pub async fn start_arbitrage_checker_ws(cfg: Config) {
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(
            cfg.bybit_api_key.clone(),
            cfg.bybit_api_secret.clone(),
            cfg.bybit_taker_fee,
            cfg.bybit_maker_fee,
        )),
        Box::new(KucoinExchange::new(
            cfg.kucoin_api_key.clone(),
            cfg.kucoin_api_secret.clone(),
            cfg.kucoin_taker_fee,
            cfg.kucoin_maker_fee,
        )),
        Box::new(GateExchange::new(
            cfg.gate_api_key.clone(),
            cfg.gate_api_secret.clone(),
            cfg.gate_taker_fee,
            cfg.gate_maker_fee,
        )),
    ];
    let market_data = MarketData::new();
    let mut exchange_gateway = ExchangeGateway::new();
    for exchange in exchanges {
        exchange_gateway.add_exchange(exchange);
    }

    let mut engine = ArbitrageEngine::new(market_data.clone(), exchange_gateway);

    engine
        .start_order_book_processing(vec!["UMA".to_string()])
        .await;

    loop {
        let opportunities = engine
            .find_arbitrage_opportunities("UMA", Decimal::ZERO, 100_000)
            .await;
        opportunities.iter().take(10).for_each(|o| {
            println!(
                "  ws: symbol: {}, profit per unit: {}, buy: {:?}, sell: {:?}",
                o.symbol, o.estimated_profit_per_unit, o.buy_exchange, o.sell_exchange
            );
        });
        sleep(Duration::from_secs(5)).await;
    }
}
