use rust_decimal::Decimal;
use std::sync::Arc;

use crate::exchanges::ExchangeConfig;

pub type Config = Arc<Configuration>;

pub struct Configuration {
    pub symbol_min_volume_24h: Decimal,
    pub max_open_positions: usize,
    pub order_book_max_age_ms: u64,
    pub min_open_profit_percentage: Decimal,
    pub max_close_profit_percentage: Decimal,
    pub min_position_value: Decimal,
    pub max_position_value: Decimal,
    pub bybit: ExchangeConfig,
    pub kucoin: ExchangeConfig,
    pub gate: ExchangeConfig,
}

impl Configuration {
    /// Creates a new configuration from environment variables.
    pub fn new() -> Self {
        let symbol_min_volume_24h = env_var("SYMBOL_MIN_VOLUME_24H")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MIN_VOLUME_24H environment variable. Please make sure it is a valid decimal.");

        let order_book_max_age_ms = env_var("ORDER_BOOK_MAX_AGE_MS")
            .parse::<u64>()
            .expect("Unable to parse the value of the ORDER_BOOK_MAX_AGE_MS environment variable. Please make sure it is a valid number.");

        let min_open_profit_percentage = env_var("MIN_OPEN_PROFIT_PERCENTAGE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MIN_OPEN_PROFIT_PERCENTAGE environment variable. Please make sure it is a valid decimal.");

        let max_close_profit_percentage = env_var("MAX_CLOSE_PROFIT_PERCENTAGE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MAX_CLOSE_PROFIT_PERCENTAGE environment variable. Please make sure it is a valid decimal.");

        let min_position_value = env_var("MIN_POSITION_VALUE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MIN_POSITION_VALUE environment variable. Please make sure it is a valid decimal.");

        let max_position_value = env_var("MAX_POSITION_VALUE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MAX_POSITION_VALUE environment variable. Please make sure it is a valid decimal.");

        let max_open_positions = env_var("MAX_OPEN_POSITIONS")
            .parse::<usize>()
            .expect("Unable to parse the value of the MAX_OPEN_POSITIONS environment variable. Please make sure it is a valid number.");

        let bybit = ExchangeConfig {
            api_key: env_var("BYBIT_API_KEY"),
            api_secret: env_var("BYBIT_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("BYBIT_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse BYBIT_TAKER_FEE"),
            maker_fee: env_var("BYBIT_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse BYBIT_MAKER_FEE"),
        };

        let kucoin = ExchangeConfig {
            api_key: env_var("KUCOIN_API_KEY"),
            api_secret: env_var("KUCOIN_API_SECRET"),
            api_passphrase: Some(env_var("KUCOIN_API_PASSPHRASE")),
            taker_fee: env_var("KUCOIN_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse KUCOIN_TAKER_FEE"),
            maker_fee: env_var("KUCOIN_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse KUCOIN_MAKER_FEE"),
        };

        let gate = ExchangeConfig {
            api_key: env_var("GATE_API_KEY"),
            api_secret: env_var("GATE_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("GATE_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse GATE_TAKER_FEE"),
            maker_fee: env_var("GATE_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse GATE_MAKER_FEE"),
        };

        Self {
            symbol_min_volume_24h,
            order_book_max_age_ms,
            min_open_profit_percentage,
            max_close_profit_percentage,
            min_position_value,
            max_position_value,
            max_open_positions,
            bybit,
            kucoin,
            gate,
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self::new()
    }
}

fn env_var(name: &str) -> String {
    std::env::var(name)
        .map_err(|e| format!("{}: {}", name, e))
        .expect("Missing environment variable")
}
