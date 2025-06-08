use server::{
    exchanges::{self, BybitExchange, Exchange, GateExchange, KuCoinExchange},
    strategy::arbitrage::{start_arbitrage_checker_ws},
    telemetry, AppState, Configuration, Db,
};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // Loads the .env file located in the environment's current directory or its parents in sequence.
    // .env used only for development, so we discard error in all other cases.
    dotenvy::dotenv().ok();

    // Tries to load tracing config from environment (RUST_LOG) or uses "debug".
    telemetry::setup_tracing();

    // Parse configuration from the environment.
    // This will exit with a help message if something is wrong.
    tracing::debug!("Initializing configuration");
    let cfg = Configuration::new();

    // Initialize db pool.
    tracing::debug!("Initializing db pool");
    let db = Db::new(&cfg.db_dsn, cfg.db_pool_max_size)
        .await
        .expect("Failed to initialize db");

    tracing::debug!("Running migrations");
    db.migrate().await.expect("Failed to run migrations");

    let exchanges = (
        BybitExchange::new(
            cfg.bybit_api_key.clone(),
            cfg.bybit_api_secret.clone(),
            cfg.bybit_taker_fee,
            cfg.bybit_maker_fee,
        ),
        KuCoinExchange::new(
            cfg.kucoin_api_key.clone(),
            cfg.kucoin_api_secret.clone(),
            cfg.kucoin_taker_fee,
            cfg.kucoin_maker_fee,
        ),
        GateExchange::new(
            cfg.gate_api_key.clone(),
            cfg.gate_api_secret.clone(),
            cfg.gate_taker_fee,
            cfg.gate_maker_fee,
        )
        // Box::new(OkxExchange::new(
        //     cfg.okx_api_key.clone(),
        //     cfg.okx_api_secret.clone(),
        //     cfg.okx_taker_fee,
        //     cfg.okx_maker_fee,
        // )),
        // Box::new(BitgetExchange::new(
        //     cfg.bitget_api_key.clone(),
        //     cfg.bitget_api_secret.clone(),
        //     cfg.bitget_taker_fee,
        //     cfg.bitget_maker_fee,
        // )),
        // Box::new(HtxExchange::new(
        //     cfg.htx_api_key.clone(),
        //     cfg.htx_api_secret.clone(),
        //     cfg.htx_taker_fee,
        //     cfg.htx_maker_fee,
        // )),
        // Box::new(MexcExchange::new(
        //     cfg.mexc_api_key.clone(),
        //     cfg.mexc_api_secret.clone(),
        //     cfg.mexc_taker_fee,
        //     cfg.mexc_maker_fee,
        // )),
        // Box::new(BingxExchange::new(
        //     cfg.bingx_api_key.clone(),
        //     cfg.bingx_api_secret.clone(),
        //     cfg.bingx_taker_fee,
        //     cfg.bingx_maker_fee,
        // )),
    );
    let exchanges_arc = Arc::new(exchanges);

    // Create app state
    let app_state = AppState {
        db,
        cfg,
        exchanges: exchanges_arc,
    };

    // Start arbitrage checker in background
    // tokio::spawn(start_arbitrage_checker(app_state.clone()));
    tokio::spawn(start_arbitrage_checker_ws(app_state.clone()));

    // Spin up our server.
    let listen_address = app_state.cfg.listen_address;
    tracing::info!("Starting server on {}", listen_address);
    let listener = TcpListener::bind(listen_address)
        .await
        .expect("Failed to bind address");
    let router = server::router(app_state);
    axum::serve(listener, router)
        .await
        .expect("Failed to start server")
}
