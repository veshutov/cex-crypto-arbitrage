use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Exchange error: {0}")]
    ExchangeError(#[from] crate::exchanges::ExchangeError),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("OrderManagerError error: {0}")]
    OrderManagerError(#[from] crate::engine::order_manager::OrderManagerError),
}

pub type Result<T> = std::result::Result<T, AppError>;
