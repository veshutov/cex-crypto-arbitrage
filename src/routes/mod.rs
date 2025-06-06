use axum::{routing::get, Router};

pub mod health_check;
pub mod arbitrage;

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
    .route("/health_check", get(health_check::health_check))
    .route("/arbitrage", get(arbitrage::get_arbitrage_opportunities))
}

pub use health_check::health_check;
pub use arbitrage::get_arbitrage_opportunities;
