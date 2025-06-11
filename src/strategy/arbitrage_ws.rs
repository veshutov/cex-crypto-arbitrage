use rust_decimal::Decimal;
use std::time::Duration;

use tokio::time::sleep;

use crate::{
    engine::{market_data::MarketData, Engine},
    exchanges::bybit::BybitExchange,
    exchanges::gate::GateExchange,
    exchanges::gateway::ExchangeGateway,
    exchanges::kucoin::KucoinExchange,
    exchanges::{Exchange, ExchangeName},
    Config, Result,
};

pub async fn start_arbitrage_checker_ws(cfg: Config) -> Result<()> {
    let symbols = vec![
        "AXL".to_string(),
        "ULTI".to_string(),
        "SCA".to_string(),
        "XEM".to_string(),
    ];
    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(cfg.bybit.clone())),
        Box::new(KucoinExchange::new(cfg.kucoin.clone())),
        Box::new(GateExchange::new(cfg.gate.clone())),
    ];
    let max_age_ms = 50_000;
    let min_profit_percentage = Decimal::ZERO;

    let exchange_names: Vec<ExchangeName> = exchanges.iter().map(|e| e.name()).collect();
    let market_data = MarketData::new(&exchange_names, &symbols);
    let mut exchange_gateway = ExchangeGateway::new();

    for exchange in exchanges {
        exchange_gateway.add_exchange(exchange);
    }

    let mut engine = Engine::new(market_data.clone(), exchange_gateway);

    engine.start_order_book_processing(symbols.clone()).await?;

    loop {
        println!("{:?}", engine.market_data);
        for symbol in symbols.clone() {
            let opportunities = engine
                .find_arbitrage_opportunities(symbol.as_str(), min_profit_percentage, max_age_ms)
                .await?;
            opportunities.iter().take(10).for_each(|o| {
                println!(
                    "  ws: {} – {} ({:.2}), buy: {:?}, sell: {:?}",
                    o.symbol,
                    o.estimated_profit_per_unit,
                    o.net_profit_percentage,
                    o.buy_exchange,
                    o.sell_exchange
                );
            });
        }
        sleep(Duration::from_secs(5)).await;
    }
}
