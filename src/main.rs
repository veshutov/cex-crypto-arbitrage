use std::sync::Arc;

use crate::strategy::{
    rest::start_arbitrage_checker_rest, ws::start_arbitrage_checker_ws,
};

pub mod cfg;
pub mod engine;
pub mod error;
pub mod exchanges;
pub mod strategy;

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
    let start_result = start_arbitrage_checker_ws(cfg.clone()).await;
    match start_result {
        Ok(join_handle) => {
            match join_handle.await {
                Ok(_) => {
                    println!("Exiting main join");
                }
                Err(e) => {
                    println!("Error main join {}", e);
                }
            };
        }
        Err(e) => {
            println!("Error starting engine {}", e);
        }
    };
}
