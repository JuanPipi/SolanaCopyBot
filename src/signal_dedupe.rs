//! Señal con ID determinístico y dedupe (processed / missed / inflight)

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::signals::{Side, TradeSignal};

/// ID determinístico para una señal: mint + side + leader_sig (o fallback)
pub fn signal_id(signal: &TradeSignal) -> String {
    let side_str = match signal.side {
        Side::Buy => "B",
        Side::Sell => "S",
    };
    format!(
        "{}|{}|{}|{}|{}",
        signal.mint,
        side_str,
        signal.sig,
        signal.ts,
        signal.leader_sol_delta.to_bits()
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Entry para señales procesadas con éxito (TTL largo)
#[derive(Debug, Clone)]
pub struct ProcessedEntry {
    pub ts_ms: u64,
}

/// Entry para señales que fallaron (MISS) - cooldown
#[derive(Debug, Clone)]
pub struct MissedEntry {
    pub ts_ms: u64,
    pub reason: String,
}

/// Cache de dedupe de señales
pub struct SignalDedupe {
    /// Señales ya ejecutadas con éxito
    pub processed_signals: HashMap<String, ProcessedEntry>,
    /// Señales que fallaron (liquidez/slippage) - no reintentar por cooldown_miss_ms
    pub missed_signals: HashMap<String, MissedEntry>,
    /// Señales en progreso (evitar doble envío)
    pub inflight_signals: HashMap<String, u64>,
    pub cooldown_miss_ms: u64,
    pub processed_ttl_ms: u64,
}

impl SignalDedupe {
    pub fn new(cooldown_miss_ms: u64) -> Self {
        Self {
            processed_signals: HashMap::new(),
            missed_signals: HashMap::new(),
            inflight_signals: HashMap::new(),
            cooldown_miss_ms,
            processed_ttl_ms: 300_000, // 5 min para processed
        }
    }

    /// Verificar si la señal debe ser procesada (no duplicada, no en miss cooldown)
    pub fn should_process(&mut self, id: &str) -> DedupeDecision {
        let now = now_ms();

        // Limpiar inflight expirados (> 60s)
        self.inflight_signals.retain(|_, &mut ts| now.saturating_sub(ts) < 60_000);
        self.processed_signals.retain(|_, e| now.saturating_sub(e.ts_ms) < self.processed_ttl_ms);
        self.missed_signals.retain(|_, e| now.saturating_sub(e.ts_ms) < self.cooldown_miss_ms);

        if self.processed_signals.contains_key(id) {
            return DedupeDecision::AlreadyProcessed;
        }

        if let Some(e) = self.missed_signals.get(id) {
            if now.saturating_sub(e.ts_ms) < self.cooldown_miss_ms {
                return DedupeDecision::MissCooldown(e.reason.clone());
            }
        }

        if self.inflight_signals.contains_key(id) {
            return DedupeDecision::Inflight;
        }

        DedupeDecision::Process
    }

    /// Marcar señal como inflight
    pub fn mark_inflight(&mut self, id: &str) {
        self.inflight_signals.insert(id.to_string(), now_ms());
    }

    /// Marcar señal como procesada con éxito
    pub fn mark_processed(&mut self, id: &str) {
        self.inflight_signals.remove(id);
        self.processed_signals.insert(
            id.to_string(),
            ProcessedEntry { ts_ms: now_ms() },
        );
    }

    /// Marcar señal como missed (falló por liquidez/slippage)
    pub fn mark_missed(&mut self, id: &str, reason: &str) {
        self.inflight_signals.remove(id);
        self.missed_signals.insert(
            id.to_string(),
            MissedEntry {
                ts_ms: now_ms(),
                reason: reason.to_string(),
            },
        );
    }

    /// Quitar inflight (al terminar ejecución)
    pub fn clear_inflight(&mut self, id: &str) {
        self.inflight_signals.remove(id);
    }
}

#[derive(Debug, Clone)]
pub enum DedupeDecision {
    Process,
    AlreadyProcessed,
    MissCooldown(String),
    Inflight,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::{Side, TradeSignal};

    fn make_signal() -> TradeSignal {
        TradeSignal {
            leader_wallet: "abc".into(),
            side: Side::Buy,
            mint: "mint123".into(),
            leader_sol_delta: 1.0,
            sig: "sig123".into(),
            ts: 1000,
        }
    }

    #[test]
    fn test_dedupe_same_signal_once() {
        let mut d = SignalDedupe::new(30_000);
        let s = make_signal();
        let id = signal_id(&s);
        assert!(matches!(d.should_process(&id), DedupeDecision::Process));
        d.mark_inflight(&id);
        assert!(matches!(d.should_process(&id), DedupeDecision::Inflight));
        d.mark_processed(&id);
        assert!(matches!(d.should_process(&id), DedupeDecision::AlreadyProcessed));
    }

    #[test]
    fn test_dedupe_miss_cooldown() {
        let mut d = SignalDedupe::new(30_000);
        let s = make_signal();
        let id = signal_id(&s);
        d.mark_inflight(&id);
        d.mark_missed(&id, "MISS_LIQUIDITY");
        assert!(matches!(d.should_process(&id), DedupeDecision::MissCooldown(_)));
    }
}
