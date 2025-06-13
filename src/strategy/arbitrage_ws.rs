use std::{sync::Arc, time::Duration};

use rust_decimal::Decimal;
use tokio::time::sleep;

use crate::{
    engine::{
        market_data::MarketData,
        order_manager::{self, OrderManager},
        ArbitrageEngine, ArbitrageEngineConfig,
    },
    exchanges::{
        bybit::BybitExchange, gate::GateExchange, gateway::ExchangeGateway, kucoin::KucoinExchange,
        Exchange, ExchangeName,
    },
    Config, Result,
};

pub async fn start_arbitrage_checker_ws(cfg: Config) -> Result<()> {
    let symbols = vec![
        "ANIME".to_string(),
        "SCA".to_string(),
        "DARK".to_string(),
        "RSS3".to_string(),
        "REX".to_string(),
        // "BTC".to_string(),
        // "ETH".to_string(),
        // "SOL".to_string(),
    ];
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(cfg.bybit.clone())),
        Box::new(KucoinExchange::new(cfg.kucoin.clone())),
        Box::new(GateExchange::new(cfg.gate.clone())),
    ];

    let exchange_names: Vec<ExchangeName> = exchanges.iter().map(|e| e.name()).collect();
    let market_data = MarketData::new(&exchange_names, &symbols);
    let exchange_gateway = Arc::new(ExchangeGateway::new(exchanges));
    let order_manager = OrderManager::new(exchange_gateway.clone());
    let engine_config = ArbitrageEngineConfig {
        min_open_profit_percentage: cfg.min_open_profit_percentage,
        max_close_profit_percentage: cfg.max_close_profit_percentage,
        min_position_value: cfg.min_position_value,
        max_position_value: cfg.max_position_value,
    };

    let mut engine = ArbitrageEngine::new(market_data, exchange_gateway.clone(), order_manager, engine_config);

    let r = exchange_gateway.get_open_positions(ExchangeName::Bybit).await;
    println!("{:?}", r);
    let r = exchange_gateway.get_open_positions(ExchangeName::Kucoin).await;
    println!("{:?}", r);
    let r = exchange_gateway.get_open_positions(ExchangeName::Gate).await;
    println!("{:?}", r);

    Ok(())
}
