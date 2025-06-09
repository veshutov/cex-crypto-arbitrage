use serde::Deserialize;
use std::{
    str::FromStr,
    sync::Arc,
};
use rust_decimal::Decimal;

pub type Config = Arc<Configuration>;

#[derive(Deserialize)]
pub struct Configuration {
    /// The environment in which to run the application.
    pub env: Environment,

    pub min_volume_24h: Decimal,

    /// The API key for the Bybit exchange.
    pub bybit_api_key: String,
    /// The API secret for the Bybit exchange.
    pub bybit_api_secret: String,
    /// Taker fee for the Bybit exchange.
    pub bybit_taker_fee: Decimal,
    /// Maker fee for the Bybit exchange.
    pub bybit_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub kucoin_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub kucoin_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub kucoin_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub kucoin_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub bingx_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub bingx_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub bingx_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub bingx_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub bitget_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub bitget_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub bitget_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub bitget_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub gate_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub gate_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub gate_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub gate_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub htx_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub htx_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub htx_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub htx_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub mexc_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub mexc_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub mexc_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub mexc_maker_fee: Decimal,

    /// The API key for the Kucoin exchange.
    pub okx_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub okx_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub okx_taker_fee: Decimal,
    /// Maker fee for the Kucoin exchange.
    pub okx_maker_fee: Decimal,
}

#[derive(Deserialize, Debug)]
pub enum Environment {
    Development,
    Production,
}

impl Configuration {
    /// Creates a new configuration from environment variables.
    pub fn new() -> Config {
        let env = env_var("APP_ENVIRONMENT")
            .parse::<Environment>()
            .expect("Unable to parse the value of the APP_ENVIRONMENT environment variable. Please make sure it is either \"development\" or \"production\".");

        let min_volume_24h = env_var("MIN_VOLUME_24H")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MIN_VOLUME_24H environment variable. Please make sure it is a valid decimal.");

        let bybit_api_key = env_var("BYBIT_API_KEY");
        let bybit_api_secret = env_var("BYBIT_API_SECRET");
        let bybit_taker_fee = env_var("BYBIT_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the BYBIT_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let bybit_maker_fee = env_var("BYBIT_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the BYBIT_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let kucoin_api_key = env_var("KUCOIN_API_KEY");
        let kucoin_api_secret = env_var("KUCOIN_API_SECRET");
        let kucoin_taker_fee = env_var("KUCOIN_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the KUCOIN_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let kucoin_maker_fee = env_var("KUCOIN_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the KUCOIN_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let bingx_api_key = env_var("BINGX_API_KEY");
        let bingx_api_secret = env_var("BINGX_API_SECRET");
        let bingx_taker_fee = env_var("BINGX_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the BINGX_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let bingx_maker_fee = env_var("BINGX_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the BINGX_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let bitget_api_key = env_var("BITGET_API_KEY");
        let bitget_api_secret = env_var("BITGET_API_SECRET");
        let bitget_taker_fee = env_var("BITGET_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the BITGET_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let bitget_maker_fee = env_var("BITGET_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the BITGET_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let gate_api_key = env_var("GATE_API_KEY");
        let gate_api_secret = env_var("GATE_API_SECRET");
        let gate_taker_fee = env_var("GATE_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the GATE_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let gate_maker_fee = env_var("GATE_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the GATE_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let htx_api_key = env_var("HTX_API_KEY");
        let htx_api_secret = env_var("HTX_API_SECRET");
        let htx_taker_fee = env_var("HTX_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the HTX_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let htx_maker_fee = env_var("HTX_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the HTX_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let mexc_api_key = env_var("MEXC_API_KEY");
        let mexc_api_secret = env_var("MEXC_API_SECRET");
        let mexc_taker_fee = env_var("MEXC_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MEXC_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let mexc_maker_fee = env_var("MEXC_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the MEXC_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        let okx_api_key = env_var("OKX_API_KEY");
        let okx_api_secret = env_var("OKX_API_SECRET");
        let okx_taker_fee = env_var("OKX_TAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the OKX_TAKER_FEE environment variable. Please make sure it is a valid decimal.");
        let okx_maker_fee = env_var("OKX_MAKER_FEE")
            .parse::<Decimal>()
            .expect("Unable to parse the value of the OKX_MAKER_FEE environment variable. Please make sure it is a valid decimal.");

        Arc::new(Configuration {
            env,
            bybit_api_key,
            bybit_api_secret,
            bybit_taker_fee,
            bybit_maker_fee,
            kucoin_api_key,
            kucoin_api_secret,
            kucoin_taker_fee,
            kucoin_maker_fee,
            bingx_api_key,
            bingx_api_secret,
            bingx_taker_fee,
            bingx_maker_fee,
            bitget_api_key,
            bitget_api_secret,
            bitget_taker_fee,
            bitget_maker_fee,
            gate_api_key,
            gate_api_secret,
            gate_taker_fee,
            gate_maker_fee,
            htx_api_key,
            htx_api_secret,
            htx_taker_fee,
            htx_maker_fee,
            mexc_api_key,
            mexc_api_secret,
            mexc_taker_fee,
            mexc_maker_fee,
            okx_api_key,
            okx_api_secret,
            okx_taker_fee,
            okx_maker_fee,
            min_volume_24h,
        })
    }
}

impl FromStr for Environment {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "development" => Ok(Environment::Development),
            "production" => Ok(Environment::Production),
            _ => Err(format!(
                "Invalid environment: {}. Please make sure it is either \"development\" or \"production\".",
                s
            )),
        }
    }
}

fn env_var(name: &str) -> String {
    std::env::var(name)
        .map_err(|e| format!("{}: {}", name, e))
        .expect("Missing environment variable")
}
