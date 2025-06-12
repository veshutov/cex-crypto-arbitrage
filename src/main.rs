use std::sync::Arc;

use crate::strategy::{arbitrage_ws::start_arbitrage_checker_ws, arbitrage_rest::start_arbitrage_checker_rest};

pub mod cfg;
pub mod exchanges;
pub mod strategy;
pub mod error;
pub mod engine;

pub use cfg::*;
pub use error::*;

#[tokio::main]
async fn main() {
    // Loads the .env file located in the environment's current directory or its parents in sequence.
    // .env used only for development, so we discard error in all other cases.
    dotenvy::dotenv().ok();
    let cfg: Config = Arc::new(Configuration::new());

    // Start arbitrage checker in background
    tokio::spawn(start_arbitrage_checker_rest(cfg.clone()));
    tokio::spawn(start_arbitrage_checker_ws(cfg.clone()));

    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}
