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
        "XEM".to_string(),
        "SCA".to_string(),
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
        order_book_max_age_ms: cfg.order_book_max_age_ms,
        min_open_profit_percentage: cfg.min_open_profit_percentage,
        max_close_profit_percentage: cfg.max_close_profit_percentage,
        min_position_value: cfg.min_position_value,
        max_position_value: cfg.max_position_value,
    };

    let mut engine = ArbitrageEngine::new(market_data, exchange_gateway.clone(), order_manager.clone(), engine_config);
    match engine.start_processing(symbols).await {
        Ok(_) => {
            println!("Engine started");
        },
        Err(e) => {
            println!("Engine error: {}", e)
        },
    };


    
    loop {
        println!("{:?}", order_manager.get_open_positions());
        sleep(Duration::from_secs(3)).await;
    }
    Ok(())
}
