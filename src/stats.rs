//! Contadores de observabilidad para modo SNIPER

use std::sync::atomic::{AtomicU64, Ordering};

/// Contadores globales (thread-safe)
#[derive(Debug, Default)]
pub struct SniperStats {
    pub signals_seen: AtomicU64,
    pub attempted_buys: AtomicU64,
    pub executed_buys: AtomicU64,
    pub miss_liquidity: AtomicU64,
    pub miss_risk: AtomicU64,
    pub miss_cooldown: AtomicU64,
    pub miss_other: AtomicU64,
    pub fallback_quote_used: AtomicU64,
}

impl SniperStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_signals_seen(&self) {
        self.signals_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_attempted_buys(&self) {
        self.attempted_buys.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_executed_buys(&self) {
        self.executed_buys.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_miss_liquidity(&self) {
        self.miss_liquidity.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_miss_risk(&self) {
        self.miss_risk.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_miss_cooldown(&self) {
        self.miss_cooldown.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_miss_other(&self) {
        self.miss_other.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_fallback_quote_used(&self) {
        self.fallback_quote_used.fetch_add(1, Ordering::Relaxed);
    }

    pub fn signals_seen(&self) -> u64 {
        self.signals_seen.load(Ordering::Relaxed)
    }

    pub fn print_summary(&self) {
        let s = self.signals_seen();
        let a = self.attempted_buys.load(Ordering::Relaxed);
        let e = self.executed_buys.load(Ordering::Relaxed);
        let liq = self.miss_liquidity.load(Ordering::Relaxed);
        let risk = self.miss_risk.load(Ordering::Relaxed);
        let cd = self.miss_cooldown.load(Ordering::Relaxed);
        let other = self.miss_other.load(Ordering::Relaxed);
        let fallback = self.fallback_quote_used.load(Ordering::Relaxed);
        println!(
            "[STATS] signals={} attempted={} filled={} miss_liq={} miss_risk={} miss_cd={} miss_other={} fallback={}",
            s, a, e, liq, risk, cd, other, fallback
        );
    }
}
