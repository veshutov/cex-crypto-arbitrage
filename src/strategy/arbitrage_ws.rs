use std::sync::Arc;

use tokio::task::JoinHandle;
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
        // "XEM".to_string(),
        // "AXL".to_string(),
        // "T".to_string(),
        // "TGT".to_string(),
        // "SCA".to_string(),
        // "OMG".to_string(),
        "RDO".to_string(),

        // "ANIME".to_string(),
        // "ORBS".to_string(),
        // "ULTI".to_string(),
        // "RSS3".to_string(), – на gate они по 10шт лот
        // "REX".to_string(), – на gate они по 100шт лот
    ];
    // let op = bingx.get_open_positions().await;
    // println!("op - {:?}", op);
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        // Box::new(BybitExchange::new(cfg.bybit.clone())),
        // Box::new(KucoinExchange::new(cfg.kucoin.clone())),
        Box::new(GateExchange::new(cfg.gate.clone())),
        Box::new(BingxExchange::new(cfg.bingx.clone())),
    ];

    let exchange_names: Vec<ExchangeName> = exchanges.iter().map(|e| e.name()).collect();
    let market_data = MarketData::new(&exchange_names, &symbols);
    let exchange_gateway = Arc::new(ExchangeGateway::new(exchanges));
    // let r = exchange_gateway
    //     .place_order(
    //         ExchangeName::Gate,
    //         OrderRequest {
    //             id: Ulid::new().to_string(),
    //             symbol: "XEM".to_string(),
    //             quantity: 4150,
    //             side: crate::exchanges::OrderSide::Buy,
    //         },
    //     )
    //     .await;
    // println!("order - {:?}", r);
    // let r = exchange_gateway
    //     .place_order(
    //         ExchangeName::Kucoin,
    //         OrderRequest {
    //             id: Ulid::new().to_string(),
    //             symbol: "XEM".to_string(),
    //             quantity: 4000,
    //             side: crate::exchanges::OrderSide::Sell,
    //         },
    //     )
    //     .await;
    // println!("order - {:?}", r);
    // let r = exchange_gateway.close_position(
    //     &Ulid::new().to_string(),
    //     ExchangeName::Bingx,
    //     "WIF",
    //     crate::exchanges::OrderSide::Buy,
    // ).await;
    // println!("order - {:?}", r);
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
    engine.start_processing(symbols.clone()).await
}
