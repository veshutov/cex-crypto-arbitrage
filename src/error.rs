use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Exchange error: {0}")]
    ExchangeError(#[from] crate::exchanges::ExchangeError),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
