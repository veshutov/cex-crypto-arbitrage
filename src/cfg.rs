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
        let kucoin_api_key = env_var("KUCOIN_API_KEY");
        let kucoin_api_secret = env_var("KUCOIN_API_SECRET");

        let bybit_taker_fee = env_var("BYBIT_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BYBIT_TAKER_FEE environment variable. Please make sure it is a valid float.");

        let bybit_maker_fee = env_var("BYBIT_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the BYBIT_MAKER_FEE environment variable. Please make sure it is a valid float.");

        let kucoin_taker_fee = env_var("KUCOIN_TAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the KUCOIN_TAKER_FEE environment variable. Please make sure it is a valid float.");

        let kucoin_maker_fee = env_var("KUCOIN_MAKER_FEE")
            .parse::<f64>()
            .expect("Unable to parse the value of the KUCOIN_MAKER_FEE environment variable. Please make sure it is a valid float.");

        Arc::new(Configuration {
            env,
            listen_address,
            app_port,
            db_dsn,
            db_pool_max_size,
            bybit_api_key,
            bybit_api_secret,
            kucoin_api_key,
            kucoin_api_secret,
            bybit_taker_fee,
            bybit_maker_fee,
            kucoin_taker_fee,
            kucoin_maker_fee,
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
