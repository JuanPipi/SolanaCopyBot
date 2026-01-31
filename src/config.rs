use std::env;

pub struct Config {
    pub helius_http: String,
    pub helius_wss: String,
    pub wallets: Vec<String>,
    // Jito config
    pub jito_url: Option<String>,
    pub jito_tip_lamports: u64,
    pub jito_auth: Option<String>,
    // Keypair path for real execution
    pub keypair_path: Option<String>,
    // Jupiter API (optional, increases rate limits)
    pub jupiter_api_key: Option<String>,
    /// Si true, vender posiciones untracked cuando el líder vende (default: true)
    pub reconcile_untracked_sell: bool,
}

impl Config {
    pub fn load() -> Self {
        dotenvy::dotenv().ok();

        let helius_http = env::var("HELIUS_HTTP").expect("HELIUS_HTTP missing in .env");
        let helius_wss = env::var("HELIUS_WSS").expect("HELIUS_WSS missing in .env");

        // Wallets to follow - comma separated in .env
        let wallets_str = env::var("WALLETS").expect("WALLETS missing in .env");
        let wallets: Vec<String> = wallets_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if wallets.is_empty() {
            panic!("No wallets configured in WALLETS env var");
        }

        // Jito config (optional)
        let jito_url = env::var("JITO_URL").ok().filter(|s| !s.is_empty());
        let jito_tip_lamports = env::var("JITO_TIP_LAMPORTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_000); // default 20k lamports
        let jito_auth = env::var("JITO_AUTH").ok().filter(|s| !s.is_empty());

        // Keypair path (optional, for real execution)
        let keypair_path = env::var("KEYPAIR_PATH").ok().filter(|s| !s.is_empty());

        // Jupiter API key (optional, increases rate limits)
        let jupiter_api_key = env::var("JUP_API_KEY").ok().filter(|s| !s.is_empty());

        let reconcile_untracked_sell = env::var("RECONCILE_UNTRACKED_SELL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        println!("📋 Loaded {} wallet(s) to follow", wallets.len());
        if jito_url.is_some() {
            println!("📦 Jito enabled: tip={} lamports", jito_tip_lamports);
        }
        if keypair_path.is_some() {
            println!("🔑 Keypair configured for real execution");
        }
        if jupiter_api_key.is_some() {
            println!("🪐 Jupiter API key configured");
        } else {
            println!("🪐 Jupiter API (free tier, rate limited)");
        }

        Self {
            helius_http,
            helius_wss,
            wallets,
            jito_url,
            jito_tip_lamports,
            jito_auth,
            keypair_path,
            jupiter_api_key,
            reconcile_untracked_sell,
        }
    }

    pub fn jito_enabled(&self) -> bool {
        self.jito_url.is_some()
    }
}
