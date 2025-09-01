use std::sync::Arc;

use crate::strategy::start_arbitrage_engine;

pub mod cfg;
pub mod engine;
pub mod error;
pub mod exchanges;
pub mod strategy;

pub use cfg::*;
pub use error::*;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let cfg: Config = Arc::new(Configuration::new());

    let main_loop = start_arbitrage_engine(cfg.clone()).await;
    match main_loop {
        Ok(join_handle) => {
            match join_handle.await {
                Ok(_) => {
                    println!("Exiting main join");
                }
                Err(e) => {
                    println!("Error main join {e}");
                }
            };
        }
        Err(e) => {
            println!("Error starting engine {e}");
        }
    };
}
