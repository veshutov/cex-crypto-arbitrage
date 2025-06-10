use thiserror::Error;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub enum ExchangeError {
    #[error("API request error: {0}")]
    RestApiError(#[from] reqwest::Error),

    #[error("Web socket error: {0}")]
    WebSocketError(#[from] tungstenite::Error),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}
