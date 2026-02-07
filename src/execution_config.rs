//! Configuración de ejecución para modo SNIPER vs RETRY

use std::env;

/// Modo de ejecución: sniper (1 intento) vs retry (reintentos por slippage/liquidez)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    #[default]
    Sniper,
    Retry,
}

impl std::str::FromStr for ExecutionMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sniper" | "s" => Ok(ExecutionMode::Sniper),
            "retry" | "r" => Ok(ExecutionMode::Retry),
            _ => Err(format!("Invalid EXECUTION_MODE: {}", s)),
        }
    }
}

/// Modo de priority fee: jito (bundle) vs rpc (compute unit price)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PriorityFeeMode {
    #[default]
    Jito,
    Rpc,
}

impl std::str::FromStr for PriorityFeeMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "jito" | "j" => Ok(PriorityFeeMode::Jito),
            "rpc" | "r" => Ok(PriorityFeeMode::Rpc),
            _ => Err(format!("Invalid PRIORITY_FEE_MODE: {}", s)),
        }
    }
}

/// Modo de preflight: simular antes de enviar vs enviar directo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PreflightMode {
    SimulateThenSend,
    #[default]
    SkipPreflight,
}

impl std::str::FromStr for PreflightMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simulate_then_send" | "simulate" | "s" => Ok(PreflightMode::SimulateThenSend),
            "skip_preflight" | "skip" | "sp" => Ok(PreflightMode::SkipPreflight),
            _ => Err(format!("Invalid PREFLIGHT_MODE: {}", s)),
        }
    }
}

/// Configuración completa de ejecución (SNIPER mode)
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub execution_mode: ExecutionMode,
    pub sniper_single_shot: bool,
    pub max_slippage_bps_sniper: u16,
    pub min_out_bps_guard: u32,
    pub priority_fee_mode: PriorityFeeMode,
    pub jito_tip_lamports_sniper: u64,
    pub compute_unit_limit: u32,
    pub compute_unit_price_microlamports: u64,
    pub preflight_mode: PreflightMode,
    pub quote_before_send: bool,
    pub quote_max_age_ms: u64,
    pub amount_scale_on_low_liquidity: bool,
    pub scale_factor: f64,
    pub cooldown_miss_ms: u64,
    pub confirm_timeout_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::Sniper,
            sniper_single_shot: true,
            max_slippage_bps_sniper: 1500, // 15%
            min_out_bps_guard: 0,
            priority_fee_mode: PriorityFeeMode::Jito,
            jito_tip_lamports_sniper: 20_000,
            compute_unit_limit: 1_400_000,
            compute_unit_price_microlamports: 100_000, // 0.0001 SOL por CU
            preflight_mode: PreflightMode::SkipPreflight,
            quote_before_send: true,
            quote_max_age_ms: 300,
            amount_scale_on_low_liquidity: false,
            scale_factor: 0.5,
            cooldown_miss_ms: 30_000, // 30s
            confirm_timeout_secs: 10,
        }
    }
}

impl ExecutionConfig {
    pub fn load_from_env(jito_tip_default: u64) -> Self {
        dotenvy::dotenv().ok();

        let execution_mode = env::var("EXECUTION_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(ExecutionMode::Sniper);

        let sniper_single_shot = env::var("SNIPER_SINGLE_SHOT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        let max_slippage_bps_sniper = env::var("MAX_SLIPPAGE_BPS_SNIPER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1500);

        let min_out_bps_guard = env::var("MIN_OUT_BPS_GUARD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let priority_fee_mode = env::var("PRIORITY_FEE_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(PriorityFeeMode::Jito);

        let jito_tip_lamports_sniper = env::var("JITO_TIP_LAMPORTS_SNIPER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(jito_tip_default);

        let compute_unit_limit = env::var("COMPUTE_UNIT_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_400_000);

        let compute_unit_price_microlamports = env::var("COMPUTE_UNIT_PRICE_MICROLAMPORTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000);

        let preflight_mode = env::var("PREFLIGHT_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(PreflightMode::SkipPreflight);

        let quote_before_send = env::var("QUOTE_BEFORE_SEND")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        let quote_max_age_ms = env::var("QUOTE_MAX_AGE_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);

        let amount_scale_on_low_liquidity = env::var("AMOUNT_SCALE_ON_LOW_LIQUIDITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);

        let scale_factor = env::var("SCALE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.5);

        let cooldown_miss_ms = env::var("COOLDOWN_MISS_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30_000);

        let confirm_timeout_secs = env::var("CONFIRM_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        Self {
            execution_mode,
            sniper_single_shot,
            max_slippage_bps_sniper,
            min_out_bps_guard,
            priority_fee_mode,
            jito_tip_lamports_sniper,
            compute_unit_limit,
            compute_unit_price_microlamports,
            preflight_mode,
            quote_before_send,
            quote_max_age_ms,
            amount_scale_on_low_liquidity,
            scale_factor,
            cooldown_miss_ms,
            confirm_timeout_secs,
        }
    }

    pub fn is_sniper(&self) -> bool {
        self.execution_mode == ExecutionMode::Sniper
    }
}
