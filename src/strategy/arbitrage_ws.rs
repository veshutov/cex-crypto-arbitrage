use std::time::Duration;

use tokio::time::sleep;

use crate::{
    engine::{market_data::MarketData, ArbitrageEngine},
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
    let mut exchange_gateway = ExchangeGateway::new();

    for exchange in exchanges {
        exchange_gateway.add_exchange(exchange);
    }

    let positions = exchange_gateway.get_open_positions(ExchangeName::Bybit).await;
    println!("pos - {:?}", positions);

    let positions = exchange_gateway.get_open_positions(ExchangeName::Kucoin).await;
    println!("pos - {:?}", positions);

    let positions = exchange_gateway.get_open_positions(ExchangeName::Gate).await;
    println!("pos - {:?}", positions);
    Ok(())

    // let mut engine = ArbitrageEngine::new(market_data, exchange_gateway);
    // let mut arbitrage_rx = engine.start_processing(symbols.clone(), cfg.min_profit_percentage, cfg.order_book_max_age_ms).await?;

    // loop {
    //     while let Ok(opportunity) = arbitrage_rx.try_recv() {
    //         println!(
    //             "  ws: {} – {:.2}, volume: {:?}, buy: {:?}, sell: {:?}",
    //             opportunity.symbol,
    //             opportunity.net_profit_percentage,
    //             opportunity.max_volume * (opportunity.buy_price + opportunity.sell_price),
    //             opportunity.buy_exchange,
    //             opportunity.sell_exchange
    //         );
    //     }
    //     sleep(Duration::from_secs(5)).await;
    // }
}
