use server::{exchanges::{BybitExchange, Exchange, KuCoinExchange}, telemetry, Configuration, Db};
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

    let exchanges: Vec<Box<dyn Exchange>> = vec![
        Box::new(BybitExchange::new(
            cfg.bybit_api_key.clone(),  
            cfg.bybit_api_secret.clone(),
            cfg.bybit_taker_fee,
            cfg.bybit_maker_fee,
        )),
        Box::new(KuCoinExchange::new(
            cfg.kucoin_api_key.clone(),
            cfg.kucoin_api_secret.clone(),
            cfg.kucoin_taker_fee,
            cfg.kucoin_maker_fee,
        )),
    ];

    // Spin up our server.
    tracing::info!("Starting server on {}", cfg.listen_address);
    let listener = TcpListener::bind(&cfg.listen_address)
        .await
        .expect("Failed to bind address");
    let router = server::router(cfg, db, exchanges);
    axum::serve(listener, router)
        .await
        .expect("Failed to start server")
}
