use std::env;

pub struct Config {
    pub helius_http: String,
    pub helius_wss: String,
    pub wallets: Vec<String>,
}

impl Config {
    pub fn load() -> Self {
        dotenvy::dotenv().ok();

        let helius_http = env::var("HELIUS_HTTP").expect("HELIUS_HTTP missing in .env");
        let helius_wss = env::var("HELIUS_WSS").expect("HELIUS_WSS missing in .env");

        // Wallets to follow - comma separated in .env
        // Example: WALLETS=wallet1,wallet2,wallet3
        let wallets_str = env::var("WALLETS").expect("WALLETS missing in .env");
        let wallets: Vec<String> = wallets_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if wallets.is_empty() {
            panic!("No wallets configured in WALLETS env var");
        }

        println!("📋 Loaded {} wallet(s) to follow", wallets.len());

        Self {
            helius_http,
            helius_wss,
            wallets,
        }
    }
}
