use serde::Deserialize;
use std::{
    net::{Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

pub type Config = Arc<Configuration>;

#[derive(Deserialize)]
pub struct Configuration {
    /// The environment in which to run the application.
    pub env: Environment,

    /// The address to listen on.
    pub listen_address: SocketAddr,
    /// The port to listen on.
    pub app_port: u16,

    /// The DSN for the database. Currently, only PostgreSQL is supported.
    pub db_dsn: String,
    /// The maximum number of connections for the PostgreSQL pool.
    pub db_pool_max_size: u32,

    /// The API key for the Bybit exchange.
    pub bybit_api_key: String,
    /// The API secret for the Bybit exchange.
    pub bybit_api_secret: String,
    /// Taker fee for the Bybit exchange.
    pub bybit_taker_fee: f64,
    /// Maker fee for the Bybit exchange.
    pub bybit_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub kucoin_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub kucoin_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub kucoin_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub kucoin_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub bingx_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub bingx_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub bingx_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub bingx_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub bitget_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub bitget_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub bitget_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub bitget_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub gate_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub gate_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub gate_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub gate_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub htx_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub htx_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub htx_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub htx_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub mexc_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub mexc_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub mexc_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub mexc_maker_fee: f64,

    /// The API key for the Kucoin exchange.
    pub okx_api_key: String,
    /// The API secret for the Kucoin exchange.
    pub okx_api_secret: String,
    /// Taker fee for the Kucoin exchange.
    pub okx_taker_fee: f64,
    /// Maker fee for the Kucoin exchange.
    pub okx_maker_fee: f64,
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

        let app_port = env_var("PORT")
            .parse::<u16>()
            .expect("Unable to parse the value of the PORT environment variable. Please make sure it is a valid unsigned 16-bit integer");

        let db_dsn = env_var("DATABASE_URL");

        let db_pool_max_size = env_var("DATABASE_POOL_MAX_SIZE")
            .parse::<u32>()
            .expect("Unable to parse the value of the DATABASE_POOL_MAX_SIZE environment variable. Please make sure it is a valid unsigned 32-bit integer.");

        let listen_address = SocketAddr::from((Ipv6Addr::UNSPECIFIED, app_port));

        let bybit_api_key = env_var("BYBIT_API_KEY");
        let bybit_api_secret = env_var("BYBIT_API_SECRET");
        let bybit_taker_fee = env_var("BYBIT_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BYBIT_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let bybit_maker_fee = env_var("BYBIT_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BYBIT_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let kucoin_api_key = env_var("KUCOIN_API_KEY");
        let kucoin_api_secret = env_var("KUCOIN_API_SECRET");
        let kucoin_taker_fee = env_var("KUCOIN_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the KUCOIN_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let kucoin_maker_fee = env_var("KUCOIN_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the KUCOIN_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let bingx_api_key = env_var("BINGX_API_KEY");
        let bingx_api_secret = env_var("BINGX_API_SECRET");
        let bingx_taker_fee = env_var("BINGX_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BINGX_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let bingx_maker_fee = env_var("BINGX_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BINGX_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let bitget_api_key = env_var("BITGET_API_KEY");
        let bitget_api_secret = env_var("BITGET_API_SECRET");
        let bitget_taker_fee = env_var("BITGET_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BITGET_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let bitget_maker_fee = env_var("BITGET_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BITGET_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let gate_api_key = env_var("GATE_API_KEY");
        let gate_api_secret = env_var("GATE_API_SECRET");
        let gate_taker_fee = env_var("GATE_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the GATE_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let gate_maker_fee = env_var("GATE_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the GATE_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let htx_api_key = env_var("HTX_API_KEY");
        let htx_api_secret = env_var("HTX_API_SECRET");
        let htx_taker_fee = env_var("HTX_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the HTX_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let htx_maker_fee = env_var("HTX_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the HTX_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let mexc_api_key = env_var("MEXC_API_KEY");
        let mexc_api_secret = env_var("MEXC_API_SECRET");
        let mexc_taker_fee = env_var("MEXC_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the MEXC_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let mexc_maker_fee = env_var("MEXC_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the MEXC_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let okx_api_key = env_var("OKX_API_KEY");
        let okx_api_secret = env_var("OKX_API_SECRET");
        let okx_taker_fee = env_var("OKX_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the OKX_TAKER_FEE environment variable. Please make sure it is a valid float.");
        let okx_maker_fee = env_var("OKX_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the OKX_MAKER_FEE environment variable. Please make sure it is a valid float.");

        Arc::new(Configuration {
            env,
            listen_address,
            app_port,
            db_dsn,
            db_pool_max_size,
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
        })
    }

    /// Sets the database DSN.
    /// This method is used in tests to override the database DSN.
    pub fn set_dsn(&mut self, db_dsn: String) {
        self.db_dsn = db_dsn
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

pub fn env_var(name: &str) -> String {
    std::env::var(name)
        .map_err(|e| format!("{}: {}", name, e))
        .expect("Missing environment variable")
}
