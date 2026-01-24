use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::signals::{Side, TradeSignal};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Configuración de riesgo con sizing dinámico
#[derive(Debug, Clone)]
pub struct RiskConfig {
    // Sizing dinámico
    pub min_trade_sol: f64,         // mínimo por trade (ej 0.02)
    pub max_trade_sol: f64,         // máximo por trade (ej 0.10)
    pub k_leader_scale: f64,        // factor de escala: my_trade = k * abs(leader_delta)
    
    // Quality Gate
    pub min_leader_sol_delta: f64,  // mínimo delta del líder para copiar (ej 0.15)
    pub exposure_cap_sol: f64,      // máximo total expuesto (ej 0.35)
    pub reserve_sol: f64,           // reserva intocable (ej 0.20)
    pub total_capital_sol: f64,     // capital total (ej 1.0)
    
    // Rate limits y timing
    pub min_buy_interval_secs: i64, // segundos mínimos entre BUYs (ej 15)
    pub cooldown_secs: i64,         // cooldown después de orphan sell (ej 60)
    pub max_hold_secs: i64,         // max hold antes de SELL forzado (ej 6h)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub mint: String,
    pub opened_sig: String,
    pub opened_ts: i64,
    pub leader_sol_delta_at_open: f64,
    #[serde(default)]
    pub my_trade_sol: f64,  // tamaño de MI trade (para calcular exposure)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrphanSell {
    pub sell_sig: String,
    pub sell_ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CooldownEntry {
    pub reason: String,
    pub until_ts: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EngineState {
    pub open_positions: HashMap<String, Position>,
    pub orphan_sells: HashMap<String, OrphanSell>,
    pub cooldown_blacklist: HashMap<String, CooldownEntry>,
    #[serde(default)]
    pub last_processed_ts: i64,
    #[serde(default)]
    pub last_buy_ts: i64,  // para rate limiting
}

#[derive(Debug, Clone)]
pub enum Action {
    Buy { mint: String, sol_amount: f64, reason: String },
    Sell { mint: String, reason: String },
    Skip { reason: String },
}

pub struct DecisionEngine {
    pub risk: RiskConfig,
    pub state: EngineState,
    state_path: String,
}

// ============ FUNCIONES DE SIZING ============

fn clamp(x: f64, lo: f64, hi: f64) -> f64 {
    if x < lo { lo } else if x > hi { hi } else { x }
}

fn compute_my_trade_sol(risk: &RiskConfig, leader_sol_delta: f64) -> f64 {
    let l = leader_sol_delta.abs();
    let raw = risk.k_leader_scale * l;
    clamp(raw, risk.min_trade_sol, risk.max_trade_sol)
}

fn current_exposure_sol(state: &EngineState) -> f64 {
    state.open_positions.values().map(|p| p.my_trade_sol).sum()
}

// ============ DECISION ENGINE ============

impl DecisionEngine {
    pub fn new(risk: RiskConfig, state_path: impl Into<String>) -> Self {
        let state_path = state_path.into();
        
        let abs_path = std::env::current_dir()
            .map(|p| p.join(&state_path).display().to_string())
            .unwrap_or_else(|_| state_path.clone());
        
        let state = match Self::load_state(&state_path) {
            Some(s) => {
                let exposure = current_exposure_sol(&s);
                println!("📂 [ENGINE] Estado cargado desde: {}", abs_path);
                println!(
                    "   └─ {} posiciones | exposure={:.4} SOL | {} orphans | {} cooldowns | last_ts={}",
                    s.open_positions.len(),
                    exposure,
                    s.orphan_sells.len(),
                    s.cooldown_blacklist.len(),
                    s.last_processed_ts
                );
                s
            }
            None => {
                println!("📂 [ENGINE] Sin estado previo en: {}", abs_path);
                println!("   └─ Iniciando fresh");
                EngineState::default()
            }
        };
        Self {
            risk,
            state,
            state_path,
        }
    }

    fn load_state(path: &str) -> Option<EngineState> {
        if !Path::new(path).exists() {
            return None;
        }
        let txt = fs::read_to_string(path).ok()?;
        serde_json::from_str(&txt).ok()
    }

    fn save_state(&self) {
        if let Ok(txt) = serde_json::to_string_pretty(&self.state) {
            let _ = fs::write(&self.state_path, txt);
        }
    }

    fn cleanup_cooldowns(&mut self, now_ts: i64) {
        let expired: Vec<String> = self
            .state
            .cooldown_blacklist
            .iter()
            .filter(|(_, e)| e.until_ts <= now_ts)
            .map(|(k, _)| k.clone())
            .collect();

        for mint in expired {
            self.state.cooldown_blacklist.remove(&mint);
        }
    }

    pub fn housekeeping(&mut self, now_ts: i64) -> Vec<Action> {
        let mut actions: Vec<Action> = vec![];

        self.cleanup_cooldowns(now_ts);

        if self.risk.max_hold_secs <= 0 {
            return actions;
        }

        let mut to_force_sell: Vec<String> = vec![];

        for (mint, pos) in self.state.open_positions.iter() {
            let age = now_ts - pos.opened_ts;
            if age >= self.risk.max_hold_secs {
                to_force_sell.push(mint.clone());
            }
        }

        for mint in to_force_sell {
            self.state.open_positions.remove(&mint);

            actions.push(Action::Sell {
                mint: mint.clone(),
                reason: "max_hold".to_string(),
            });

            println!("⏳ [FALLBACK] Max-hold alcanzado -> FORZAR SELL mint={}", mint);
        }

        if !actions.is_empty() {
            self.save_state();
        }

        actions
    }

    pub fn handle_signal(&mut self, s: TradeSignal) -> Vec<Action> {
        // 0) Filtrar señales viejas
        if s.ts < self.state.last_processed_ts {
            let reason = format!(
                "Señal vieja (ts={} <= last={}) -> SKIP | mint={} sig={}",
                s.ts, self.state.last_processed_ts, s.mint, &s.sig[..8.min(s.sig.len())]
            );
            println!("⏭️ [DEDUPE] {}", reason);
            return vec![Action::Skip { reason }];
        }

        // 1) Housekeeping
        let mut actions = self.housekeeping(s.ts);

        // 2) Procesar señal
        let main_action = match s.side {
            Side::Buy => self.on_buy(s.clone()),
            Side::Sell => self.on_sell(s.clone()),
        };

        actions.push(main_action);

        // 3) Actualizar last_processed_ts
        if s.ts > self.state.last_processed_ts {
            self.state.last_processed_ts = s.ts;
            self.save_state();
        }

        actions
    }

    fn on_buy(&mut self, s: TradeSignal) -> Action {
        // ========== QUALITY GATE CON SIZING DINÁMICO ==========

        // 1. Check WSOL
        if s.mint == WSOL_MINT {
            let reason = format!("WSOL mint -> SKIP | sig={}", &s.sig[..8.min(s.sig.len())]);
            println!("⚠️ [GATE] {}", reason);
            return Action::Skip { reason };
        }

        // 2. Check leader_sol_delta mínimo
        let leader_l = s.leader_sol_delta.abs();
        if leader_l < self.risk.min_leader_sol_delta {
            let reason = format!(
                "leader_delta {:.4} < min {:.4} -> SKIP | mint={}",
                leader_l, self.risk.min_leader_sol_delta, &s.mint[..8.min(s.mint.len())]
            );
            println!("⚠️ [GATE] {}", reason);
            return Action::Skip { reason };
        }

        // 3. Rate limit (min_buy_interval_secs)
        if s.ts - self.state.last_buy_ts < self.risk.min_buy_interval_secs {
            let reason = format!(
                "rate_limit ({}s desde último BUY) -> SKIP | mint={}",
                s.ts - self.state.last_buy_ts, &s.mint[..8.min(s.mint.len())]
            );
            println!("⚠️ [RATE] {}", reason);
            return Action::Skip { reason };
        }

        // 4. Check posición existente
        if self.state.open_positions.contains_key(&s.mint) {
            let reason = format!("Ya hay posición -> SKIP BUY | mint={}", &s.mint[..8.min(s.mint.len())]);
            println!("⚠️ [RISK] {}", reason);
            return Action::Skip { reason };
        }

        // 5. Check cooldown blacklist
        if let Some(entry) = self.state.cooldown_blacklist.get(&s.mint) {
            if s.ts < entry.until_ts {
                let reason = format!(
                    "Mint en cooldown ({}) -> SKIP | mint={}",
                    entry.reason, &s.mint[..8.min(s.mint.len())]
                );
                println!("⚠️ [GUARD] {}", reason);
                return Action::Skip { reason };
            }
        }

        // 6. Check orphan sell reciente
        if let Some(orphan) = self.state.orphan_sells.get(&s.mint) {
            if s.ts - orphan.sell_ts < self.risk.cooldown_secs {
                let reason = format!(
                    "BUY después de orphan SELL (<{}s) -> SKIP | mint={}",
                    self.risk.cooldown_secs, &s.mint[..8.min(s.mint.len())]
                );
                println!("⚠️ [GUARD] {}", reason);
                return Action::Skip { reason };
            }
        }

        // 7. Calcular sizing dinámico
        let my_trade_sol = compute_my_trade_sol(&self.risk, s.leader_sol_delta);
        let exposure = current_exposure_sol(&self.state);

        // 8. Check exposure cap
        if exposure + my_trade_sol > self.risk.exposure_cap_sol {
            let reason = format!(
                "exposure_cap {:.3}+{:.3}>{:.3} -> SKIP | mint={}",
                exposure, my_trade_sol, self.risk.exposure_cap_sol, &s.mint[..8.min(s.mint.len())]
            );
            println!("⚠️ [RISK] {}", reason);
            return Action::Skip { reason };
        }

        // 9. Check reserve (no quedarse sin SOL)
        let remaining_after = self.risk.total_capital_sol - (exposure + my_trade_sol);
        if remaining_after < self.risk.reserve_sol {
            let reason = format!(
                "reserve_sol violada (remaining={:.3} < reserve={:.3}) -> SKIP | mint={}",
                remaining_after, self.risk.reserve_sol, &s.mint[..8.min(s.mint.len())]
            );
            println!("⚠️ [RISK] {}", reason);
            return Action::Skip { reason };
        }

        // ========== PASSED QUALITY GATE ==========

        let pos = Position {
            mint: s.mint.clone(),
            opened_sig: s.sig.clone(),
            opened_ts: s.ts,
            leader_sol_delta_at_open: s.leader_sol_delta,
            my_trade_sol,
        };

        self.state.open_positions.insert(s.mint.clone(), pos);
        self.state.last_buy_ts = s.ts;
        self.save_state();

        println!(
            "✅ [COPY] ABRIRÍA POSICIÓN | mint={} | my_sol={:.4} | leader_delta={:.4} | exposure={:.4}+{:.4}={:.4}",
            &s.mint[..8.min(s.mint.len())],
            my_trade_sol,
            s.leader_sol_delta,
            exposure,
            my_trade_sol,
            exposure + my_trade_sol
        );

        Action::Buy {
            mint: s.mint,
            sol_amount: my_trade_sol,
            reason: "copy_buy".to_string(),
        }
    }

    fn on_sell(&mut self, s: TradeSignal) -> Action {
        if let Some(pos) = self.state.open_positions.remove(&s.mint) {
            self.save_state();
            println!(
                "✅ [COPY] CERRARÍA POSICIÓN | mint={} | my_sol={:.4} | sig={}",
                &s.mint[..8.min(s.mint.len())],
                pos.my_trade_sol,
                &s.sig[..8.min(s.sig.len())]
            );
            Action::Sell {
                mint: s.mint,
                reason: "leader_sell".to_string(),
            }
        } else {
            self.state.orphan_sells.insert(
                s.mint.clone(),
                OrphanSell {
                    sell_sig: s.sig.clone(),
                    sell_ts: s.ts,
                },
            );

            self.state.cooldown_blacklist.insert(
                s.mint.clone(),
                CooldownEntry {
                    reason: "orphan_sell".to_string(),
                    until_ts: s.ts + self.risk.cooldown_secs,
                },
            );

            self.save_state();

            let reason = format!(
                "SELL sin posición -> IGNORE + cooldown | mint={}",
                &s.mint[..8.min(s.mint.len())]
            );
            println!("ℹ️ [ORPHAN] {}", reason);
            Action::Skip { reason }
        }
    }
}
