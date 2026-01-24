#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use solana_sdk::pubkey::Pubkey;

/// Tipo de DEX donde se puede operar el token
#[derive(Debug, Clone, PartialEq)]
pub enum DexKind {
    Raydium,
    Orca,
    Pump,
    Jupiter,
    Unknown,
}

/// Datos cacheados para hacer swap rápido
#[derive(Debug, Clone)]
pub struct PreparedSwap {
    pub mint: Pubkey,
    pub dex: DexKind,
    pub pool_id: Option<Pubkey>,
    pub token_program: Pubkey,
    pub last_refreshed: i64,
    // Campos adicionales según el DEX
    pub pool_accounts: Vec<Pubkey>,
}

/// Cache de swaps preparados
pub struct PreparedSwapCache {
    cache: HashMap<String, PreparedSwap>,
    ttl_secs: i64,
}

impl PreparedSwapCache {
    pub fn new(ttl_secs: i64) -> Self {
        Self {
            cache: HashMap::new(),
            ttl_secs,
        }
    }

    fn now_ts() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Busca en cache, retorna None si no existe o expiró
    pub fn get(&self, mint: &str) -> Option<&PreparedSwap> {
        let entry = self.cache.get(mint)?;
        let now = Self::now_ts();

        if now - entry.last_refreshed > self.ttl_secs {
            return None; // Expirado
        }

        Some(entry)
    }

    /// Inserta o actualiza en cache
    pub fn insert(&mut self, mint: String, mut prepared: PreparedSwap) {
        prepared.last_refreshed = Self::now_ts();
        self.cache.insert(mint, prepared);
    }

    /// Limpia entradas expiradas
    pub fn cleanup(&mut self) {
        let now = Self::now_ts();
        self.cache
            .retain(|_, v| now - v.last_refreshed <= self.ttl_secs);
    }

    /// Número de entradas en cache
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

/// Builder para preparar un swap (on-demand)
pub struct SwapPreparer {
    // En el futuro: RPC client para buscar pools
}

impl SwapPreparer {
    pub fn new() -> Self {
        Self {}
    }

    /// Prepara los datos para hacer swap de un mint
    /// Por ahora es placeholder - en producción buscaría el pool real
    pub async fn prepare(&self, mint_str: &str) -> Option<PreparedSwap> {
        // TODO: Implementar búsqueda real de pool
        // 1. Buscar en Raydium pools
        // 2. Si no, buscar en Orca
        // 3. Si no, buscar en pump.fun
        // 4. Cachear resultado

        let mint: Pubkey = mint_str.parse().ok()?;

        // Placeholder: asumimos que existe y es Raydium
        let prepared = PreparedSwap {
            mint,
            dex: DexKind::Unknown,
            pool_id: None,
            token_program: spl_token::id(),
            last_refreshed: 0,
            pool_accounts: vec![],
        };

        println!(
            "🔧 [PREPARE] Prepared swap for mint={} (placeholder)",
            mint_str
        );

        Some(prepared)
    }
}

impl Default for SwapPreparer {
    fn default() -> Self {
        Self::new()
    }
}
