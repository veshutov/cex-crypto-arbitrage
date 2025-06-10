use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Exchange error: {0}")]
    ExchangeError(#[from] crate::exchanges::ExchangeError),

    #[error("Arbitrage calculation error: {0}")]
    ArbitrageError(String),

    #[error("Invalid price data: {0}")]
    InvalidPriceError(String),

    #[error("Invalid volume data: {0}")]
    InvalidVolumeError(String),

    #[error("Rate limit exceeded: {0}")]
    RateLimitError(String),

    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, AppError>;