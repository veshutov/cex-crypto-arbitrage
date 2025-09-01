use crate::exchanges::ExchangeConfig;
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Arc};

pub type Config = Arc<Configuration>;

pub struct Configuration {
    /// The environment in which to run the application.
    pub env: Environment,
    pub symbol_min_volume_24h: Decimal,
    pub max_open_positions: usize,
    pub order_book_max_age_ms: u64,
    pub min_open_profit_percentage: Decimal,
    pub max_close_profit_percentage: Decimal,
    pub min_position_value: Decimal,
    pub max_position_value: Decimal,
    pub bybit: ExchangeConfig,
    pub kucoin: ExchangeConfig,
    pub bingx: ExchangeConfig,
    pub bitget: ExchangeConfig,
    pub gate: ExchangeConfig,
    pub htx: ExchangeConfig,
    pub mexc: ExchangeConfig,
    pub okx: ExchangeConfig,
}

#[derive(Debug)]
pub enum Environment {
    Development,
    Production,
}

impl Configuration {
    /// Creates a new configuration from environment variables.
    pub fn new() -> Self {
        let env = env_var("ENV")
            .parse::<Environment>()
            .expect("Unable to parse the value of the ENV environment variable. Please make sure it is a valid environment.");

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

        let bingx = ExchangeConfig {
            api_key: env_var("BINGX_API_KEY"),
            api_secret: env_var("BINGX_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("BINGX_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse BINGX_TAKER_FEE"),
            maker_fee: env_var("BINGX_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse BINGX_MAKER_FEE"),
        };

        let bitget = ExchangeConfig {
            api_key: env_var("BITGET_API_KEY"),
            api_secret: env_var("BITGET_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("BITGET_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse BITGET_TAKER_FEE"),
            maker_fee: env_var("BITGET_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse BITGET_MAKER_FEE"),
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

        let htx = ExchangeConfig {
            api_key: env_var("HTX_API_KEY"),
            api_secret: env_var("HTX_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("HTX_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse HTX_TAKER_FEE"),
            maker_fee: env_var("HTX_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse HTX_MAKER_FEE"),
        };

        let mexc = ExchangeConfig {
            api_key: env_var("MEXC_API_KEY"),
            api_secret: env_var("MEXC_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("MEXC_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse MEXC_TAKER_FEE"),
            maker_fee: env_var("MEXC_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse MEXC_MAKER_FEE"),
        };

        let okx = ExchangeConfig {
            api_key: env_var("OKX_API_KEY"),
            api_secret: env_var("OKX_API_SECRET"),
            api_passphrase: None,
            taker_fee: env_var("OKX_TAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse OKX_TAKER_FEE"),
            maker_fee: env_var("OKX_MAKER_FEE")
                .parse::<Decimal>()
                .expect("Unable to parse OKX_MAKER_FEE"),
        };

        Self {
            env,
            symbol_min_volume_24h,
            order_book_max_age_ms,
            min_open_profit_percentage,
            max_close_profit_percentage,
            min_position_value,
            max_position_value,
            max_open_positions,
            bybit,
            kucoin,
            bingx,
            bitget,
            gate,
            htx,
            mexc,
            okx,
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for Environment {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dev" => Ok(Environment::Development),
            "prod" => Ok(Environment::Production),
            _ => Err(format!(
                "Invalid environment: {s}. Please make sure it is either \"dev\" or \"prod\"."
            )),
        }
    }
}

fn env_var(name: &str) -> String {
    std::env::var(name)
        .map_err(|e| format!("{}: {}", name, e))
        .expect("Missing environment variable")
}
