use crate::{
    strategy::{arbitrage::start_arbitrage_checker_ws, old::start_arbitrage_checker}, Configuration
};
use tokio::net::TcpListener;

pub mod cfg;
pub mod exchanges;
pub mod strategy;

pub use cfg::*;

#[tokio::main]
async fn main() {
    // Loads the .env file located in the environment's current directory or its parents in sequence.
    // .env used only for development, so we discard error in all other cases.
    dotenvy::dotenv().ok();
    let cfg = Configuration::new();

    // Start arbitrage checker in background
    tokio::spawn(start_arbitrage_checker(cfg.clone()));
    tokio::spawn(start_arbitrage_checker_ws(cfg.clone()));

    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}
