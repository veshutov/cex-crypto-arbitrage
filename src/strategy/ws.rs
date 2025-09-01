use std::{sync::Arc, time::Duration};

use tokio::{task::JoinHandle, time::sleep};
use rust_decimal::Decimal;
use ulid::Ulid;

use crate::{
    engine::{
        market_data::MarketData, order_manager::OrderManager, ArbitrageEngine,
        ArbitrageEngineConfig,
    },
    exchanges::{
        bingx::BingxExchange, bybit::BybitExchange, gate::GateExchange, gateway::ExchangeGateway,
        kucoin::KucoinExchange, Exchange, ExchangeName, OrderRequest,
    },
    Config, Result,
};

pub async fn start_arbitrage_checker_ws(cfg: Config) -> Result<JoinHandle<()>> {
    let symbols = vec![
        "QUICK".to_string(),
        "FUEL".to_string(),
        "MEMEFI".to_string(),
        "HSK".to_string(),
        "GTC".to_string(),
        "TGT".to_string(),
        "OMG".to_string(),
        "DARK".to_string(),
        "MAGIC".to_string(),
        "T".to_string(),
        "SCA".to_string(),
        "B2".to_string(),
        "ARPA".to_string(),
        "IDOL".to_string(),
        "SKATE".to_string(),
        "FLOCK".to_string(),
        "ALT".to_string(),
        "DOOD".to_string(),
        "MDT".to_string(),
        "OBOL".to_string(),
        "AIOT".to_string(),
        "ZRC".to_string(),
        "AVL".to_string(),
        "SPK".to_string(),
        "ZEUS".to_string(),
        "NC".to_string(),
        "MASA".to_string(),
        "FUN".to_string(),
        "B3".to_string(),
        "C98".to_string(),
        "AERGO".to_string(),
        "OMNI".to_string(),
        "SUPRA".to_string(),
        "SNT".to_string(),
        "GPS".to_string(),
        "NEWT".to_string(),
        "DUCK".to_string(),
        "LPT".to_string(),
        // "AXL".to_string(),
        
        // "ANIME".to_string(),
        // "ORBS".to_string(),
        // "ULTI".to_string(), - быстро скачет туда сюда
        // "FORM".to_string(), - быстро скачет туда сюда
        // "REX".to_string(), - быстро скачет туда сюда
        // "ZKJ".to_string(), – делистинг
        // "XEM".to_string(), – спред не сужается
        // "RDO".to_string(), – спред не сужается
        // "RSS3".to_string(), – спред не сужается
    ];
    // let op = bingx.get_open_positions().await;
    // println!("op - {:?}", op);
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(cfg.bybit.clone())),
        Box::new(KucoinExchange::new(cfg.kucoin.clone())),
        Box::new(GateExchange::new(cfg.gate.clone())),
        // Box::new(BingxExchange::new(cfg.bingx.clone())),
    ];

    let exchange_names: Vec<ExchangeName> = exchanges.iter().map(|e| e.name()).collect();
    let market_data = MarketData::new(&exchange_names, &symbols);
    let exchange_gateway = Arc::new(ExchangeGateway::new(exchanges));
    let order_manager = OrderManager::new(exchange_gateway.clone());
    let engine_config = ArbitrageEngineConfig {
        max_open_positions: cfg.max_open_positions,
        order_book_max_age_ms: cfg.order_book_max_age_ms,
        min_open_profit_percentage: cfg.min_open_profit_percentage,
        max_close_profit_percentage: cfg.max_close_profit_percentage,
        min_position_value: cfg.min_position_value,
        max_position_value: cfg.max_position_value,
    };

    let mut engine = ArbitrageEngine::new(
        market_data.clone(),
        exchange_gateway.clone(),
        order_manager.clone(),
        engine_config,
    );

    // tokio::spawn(async move {
    //     loop {
    //         println!("{}", market_data.get_best_prices());
    //         sleep(Duration::from_secs(10)).await;
    //     }
    // });

    engine.start_processing(symbols.clone()).await
}
