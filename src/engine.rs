use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::signals::{Side, TradeSignal};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PENDING_TIMEOUT_SECS: i64 = 45; // Timeout para pending buys

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

    // Reconciliación: si true, vender untracked cuando el líder vende
    pub reconcile_untracked_sell: bool,
}

/// BUY en proceso (antes de confirmar)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingBuy {
    pub mint: String,
    pub started_at: i64,
    pub leader_sig: String,
    pub intended_sol: f64,
    pub leader_delta: f64,
}

/// Posición confirmada (después de verificar balance > 0)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub mint: String,
    pub opened_sig: String,       // MI signature, no del líder
    pub opened_ts: i64,
    pub leader_sol_delta_at_open: f64,
    #[serde(default)]
    pub my_trade_sol: f64,        // SOL que gasté
    #[serde(default)]
    pub my_token_balance: u64,    // Tokens que recibí
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
    #[serde(default)]
    pub pending_buys: HashMap<String, PendingBuy>,  // BUYs en proceso
    pub open_positions: HashMap<String, Position>,   // Posiciones confirmadas
    pub orphan_sells: HashMap<String, OrphanSell>,
    pub cooldown_blacklist: HashMap<String, CooldownEntry>,
    #[serde(default)]
    pub last_processed_ts: i64,
    #[serde(default)]
    pub last_buy_ts: i64,  // para rate limiting
}

#[derive(Debug, Clone)]
pub enum Action {
    Buy { mint: String, sol_amount: f64, leader_delta: f64, leader_sig: String },
    Sell { mint: String, reason: String },
    WaitAndSell { mint: String, wait_ms: u64, max_retries: u32 },  // SELL llegó pero hay pending
    Skip { reason: String },
}

/// BUY fallido reciente (para no generar orphan cuando líder vende justo después)
#[derive(Debug, Clone)]
pub struct FailedBuyEntry {
    pub ts: i64,
    pub reason: String,
}

/// BUY ignorado/skippeado (rate-limit, exposure, no copiamos, etc.)
#[derive(Debug, Clone)]
pub struct IgnoredMintEntry {
    pub ts: i64,
    pub reason: String,
}

const FAILED_BUY_TTL_SECS: i64 = 120;   // 2 min (buy falló, sell viene enseguida)
const IGNORED_MINT_TTL_SECS: i64 = 3600; // 60 min (no copiamos, líder puede vender después)

pub struct DecisionEngine {
    pub risk: RiskConfig,
    pub state: EngineState,
    /// Posiciones con balance > 0 no trackeadas (fuera del state por reset/etc)
    pub untracked_positions: HashMap<String, u64>,
    /// BUYs fallidos recientes (mint -> {ts, reason}), TTL 120s
    pub recent_failed_buys: HashMap<String, FailedBuyEntry>,
    /// BUYs ignorados/skipped (mint -> {ts, reason}), TTL 60 min
    pub recent_ignored_mints: HashMap<String, IgnoredMintEntry>,
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

/// Exposure total = open_positions + pending_buys
fn current_exposure_sol(state: &EngineState) -> f64 {
    let open: f64 = state.open_positions.values().map(|p| p.my_trade_sol).sum();
    let pending: f64 = state.pending_buys.values().map(|p| p.intended_sol).sum();
    open + pending
}

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
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
                    "   └─ {} open | {} pending | exposure={:.4} SOL | {} orphans | {} cooldowns",
                    s.open_positions.len(),
                    s.pending_buys.len(),
                    exposure,
                    s.orphan_sells.len(),
                    s.cooldown_blacklist.len(),
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
            untracked_positions: HashMap::new(),
            recent_failed_buys: HashMap::new(),
            recent_ignored_mints: HashMap::new(),
            state_path,
        }
    }

    /// Registrar BUY ignorado/skipped (rate-limit, exposure, etc.) - TTL 60 min
    pub fn record_ignored_mint(&mut self, mint: &str, reason: &str) {
        self.recent_ignored_mints.insert(
            mint.to_string(),
            IgnoredMintEntry { ts: now_ts(), reason: reason.to_string() },
        );
    }

    fn cleanup_recent_ignored_mints(&mut self) {
        let now = now_ts();
        self.recent_ignored_mints.retain(|_, e| now - e.ts < IGNORED_MINT_TTL_SECS);
    }

    /// Registrar BUY fallido (para no marcar orphan si llega SELL del líder)
    pub fn record_failed_buy(&mut self, mint: &str, reason: &str) {
        self.recent_failed_buys.insert(
            mint.to_string(),
            FailedBuyEntry { ts: now_ts(), reason: reason.to_string() },
        );
    }

    /// Limpiar entries expirados de recent_failed_buys
    fn cleanup_recent_failed_buys(&mut self) {
        let now = now_ts();
        self.recent_failed_buys.retain(|_, e| now - e.ts < FAILED_BUY_TTL_SECS);
    }

    /// Reconciliación al inicio: compara balances reales vs open_positions
    pub fn reconcile_untracked(&mut self, real_balances: HashMap<String, u64>) {
        self.untracked_positions.clear();
        for (mint, balance) in real_balances {
            if balance == 0 {
                continue;
            }
            if mint == WSOL_MINT {
                continue;
            }
            if self.state.open_positions.contains_key(&mint) {
                continue;
            }
            self.untracked_positions.insert(mint, balance);
        }
        if !self.untracked_positions.is_empty() {
            println!("⚠️ [RECONCILE] {} posiciones fuera del state (tokens en wallet no trackeados):", self.untracked_positions.len());
            for (mint, bal) in &self.untracked_positions {
                println!("   └─ {} | balance={}", &mint[..8.min(mint.len())], bal);
            }
            println!("   Si llega SELL del líder para estos mints, se venderá automáticamente.");
        }
    }

    fn load_state(path: &str) -> Option<EngineState> {
        if !Path::new(path).exists() {
            return None;
        }
        let txt = fs::read_to_string(path).ok()?;
        serde_json::from_str(&txt).ok()
    }

    pub fn save_state(&self) {
        if let Ok(txt) = serde_json::to_string_pretty(&self.state) {
            let _ = fs::write(&self.state_path, txt);
        }
    }

    // ============ PENDING BUY MANAGEMENT ============

    /// Agregar pending buy (llamar ANTES de ejecutar)
    pub fn add_pending_buy(&mut self, mint: &str, intended_sol: f64, leader_sig: &str, leader_delta: f64) {
        let pending = PendingBuy {
            mint: mint.to_string(),
            started_at: now_ts(),
            leader_sig: leader_sig.to_string(),
            intended_sol,
            leader_delta,
        };
        self.state.pending_buys.insert(mint.to_string(), pending);
        self.state.last_buy_ts = now_ts();
        self.save_state();
        println!("📝 [ENGINE] Pending BUY agregado | mint={} | sol={:.4}", &mint[..8.min(mint.len())], intended_sol);
    }

    /// Confirmar posición (llamar SOLO si executor verificó balance > 0)
    pub fn confirm_position(&mut self, mint: &str, my_sig: &str, my_token_balance: u64, my_sol_spent: f64) {
        if let Some(pending) = self.state.pending_buys.remove(mint) {
            let pos = Position {
                mint: mint.to_string(),
                opened_sig: my_sig.to_string(),  // MI signature
                opened_ts: now_ts(),
                leader_sol_delta_at_open: pending.leader_delta,
                my_trade_sol: my_sol_spent,
                my_token_balance,
            };
            self.state.open_positions.insert(mint.to_string(), pos);
            self.save_state();
            println!(
                "✅ [ENGINE] Posición CONFIRMADA | mint={} | my_sig={} | tokens={} | sol={:.4}",
                &mint[..8.min(mint.len())],
                &my_sig[..12.min(my_sig.len())],
                my_token_balance,
                my_sol_spent
            );
        } else {
            println!("⚠️ [ENGINE] confirm_position pero no hay pending para mint={}", &mint[..8.min(mint.len())]);
        }
    }

    /// Cooldown agresivo para mints con Invalid Mint (0x2) / Token-2022 problemático
    pub fn add_invalid_mint_cooldown(&mut self, mint: &str, duration_secs: i64) {
        let until = now_ts() + duration_secs;
        self.state.cooldown_blacklist.insert(
            mint.to_string(),
            CooldownEntry {
                reason: "invalid_mint".to_string(),
                until_ts: until,
            },
        );
        self.save_state();
        println!("🚫 [ENGINE] Mint en cooldown {}min | mint={} (Invalid Mint / ruta Token-2022)", duration_secs / 60, &mint[..8.min(mint.len())]);
    }

    /// Cancelar pending BUY (si executor falla) - SIEMPRE registra failed_buy para anti-orphan
    pub fn cancel_pending_buy(&mut self, mint: &str, reason: &str) {
        if self.state.pending_buys.remove(mint).is_some() {
            // Normalizar reason para record
            let record_reason = if reason.contains("COULD_NOT_FIND_ANY_ROUTE") || reason.contains("no route") || reason.contains("Quote failed") {
                "quote_no_route"
            } else if reason.contains("insufficient") {
                "insufficient_sol"
            } else if reason.contains("0x2") || reason.contains("Invalid Mint") || reason.contains("Mint invalid") {
                "invalid_mint"
            } else {
                reason
            };
            let record_reason_short = if record_reason.len() > 60 {
                format!("{}...", &record_reason[..57])
            } else {
                record_reason.to_string()
            };

            self.record_failed_buy(mint, &record_reason_short);
            println!("📝 [ENGINE] failed_buy recorded | mint={} | reason={}", &mint[..8.min(mint.len())], record_reason_short);

            // Cooldown según tipo de fallo (invalid_mint lo maneja main con add_invalid_mint_cooldown)
            let (cooldown_secs, cooldown_reason) = if record_reason == "quote_no_route" {
                (30 * 60, "no_route") // 30 min para no_route
            } else {
                (30, "pending_failed") // 30s default
            };

            self.state.cooldown_blacklist.insert(
                mint.to_string(),
                CooldownEntry {
                    reason: format!("{}: {}", cooldown_reason, &record_reason_short[..record_reason_short.len().min(40)]),
                    until_ts: now_ts() + cooldown_secs,
                },
            );
            self.save_state();
            println!("❌ [ENGINE] Pending CANCELADO | mint={} | reason={}", &mint[..8.min(mint.len())], record_reason_short);
        }
    }

    /// Check si hay pending buy para un mint
    pub fn has_pending_buy(&self, mint: &str) -> bool {
        self.state.pending_buys.contains_key(mint)
    }

    /// Remover posición abierta (después de SELL exitoso)
    pub fn remove_position(&mut self, mint: &str) {
        if self.state.open_positions.remove(mint).is_some() {
            self.save_state();
            println!("🗑️ [ENGINE] Posición removida | mint={}", &mint[..8.min(mint.len())]);
        }
    }

    // ============ HOUSEKEEPING ============

    fn cleanup_cooldowns(&mut self, ts: i64) {
        let expired: Vec<String> = self
            .state
            .cooldown_blacklist
            .iter()
            .filter(|(_, e)| e.until_ts <= ts)
            .map(|(k, _)| k.clone())
            .collect();

        for mint in expired {
            self.state.cooldown_blacklist.remove(&mint);
        }
    }

    /// Limpiar pending buys que excedieron timeout (45s)
    fn cleanup_pending_timeouts(&mut self) {
        let now = now_ts();
        let expired: Vec<String> = self
            .state
            .pending_buys
            .iter()
            .filter(|(_, p)| now - p.started_at > PENDING_TIMEOUT_SECS)
            .map(|(k, _)| k.clone())
            .collect();

        for mint in expired {
            if let Some(pending) = self.state.pending_buys.remove(&mint) {
                println!(
                    "⏱️ [ENGINE] Pending TIMEOUT ({}s) | mint={} | intended_sol={:.4}",
                    PENDING_TIMEOUT_SECS,
                    &mint[..8.min(mint.len())],
                    pending.intended_sol
                );
                // Cooldown corto
                self.state.cooldown_blacklist.insert(
                    mint,
                    CooldownEntry {
                        reason: "pending_timeout".to_string(),
                        until_ts: now + 30,
                    },
                );
            }
        }
    }

    pub fn housekeeping(&mut self, ts: i64) -> Vec<Action> {
        let mut actions: Vec<Action> = vec![];

        self.cleanup_cooldowns(ts);
        self.cleanup_pending_timeouts();

        if self.risk.max_hold_secs <= 0 {
            return actions;
        }

        let mut to_force_sell: Vec<String> = vec![];

        for (mint, pos) in self.state.open_positions.iter() {
            let age = ts - pos.opened_ts;
            if age >= self.risk.max_hold_secs {
                to_force_sell.push(mint.clone());
            }
        }

        for mint in to_force_sell {
            // No remover aquí, el executor lo hará si tiene éxito
            actions.push(Action::Sell {
                mint: mint.clone(),
                reason: "max_hold".to_string(),
            });
            println!("⏳ [FALLBACK] Max-hold alcanzado -> FORZAR SELL mint={}", &mint[..8.min(mint.len())]);
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
        let now = now_ts();
        if now - self.state.last_buy_ts < self.risk.min_buy_interval_secs {
            let reason = format!(
                "rate_limit ({}s desde último BUY) -> SKIP | mint={}",
                now - self.state.last_buy_ts, &s.mint[..8.min(s.mint.len())]
            );
            println!("⚠️ [RATE] {}", reason);
            return Action::Skip { reason };
        }

        // 4. IDEMPOTENCIA: Check posición existente O pending
        if self.state.open_positions.contains_key(&s.mint) {
            let reason = format!("Ya hay posición abierta -> SKIP BUY | mint={}", &s.mint[..8.min(s.mint.len())]);
            println!("⚠️ [RISK] {}", reason);
            return Action::Skip { reason };
        }
        if self.state.pending_buys.contains_key(&s.mint) {
            let reason = format!("Ya hay pending BUY -> SKIP | mint={}", &s.mint[..8.min(s.mint.len())]);
            println!("⚠️ [RISK] {}", reason);
            return Action::Skip { reason };
        }

        // 5. Check cooldown blacklist
        if let Some(entry) = self.state.cooldown_blacklist.get(&s.mint) {
            if now < entry.until_ts {
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
            if now - orphan.sell_ts < self.risk.cooldown_secs {
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
        // NO guardar en open_positions todavía - solo agregar a pending
        // La posición se confirma cuando executor verifica balance > 0

        self.add_pending_buy(&s.mint, my_trade_sol, &s.sig, s.leader_sol_delta);

        println!(
            "🎯 [COPY] Intentando BUY | mint={} | my_sol={:.4} | leader_delta={:.4} | exposure={:.4}+{:.4}={:.4}",
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
            leader_delta: s.leader_sol_delta,
            leader_sig: s.sig,
        }
    }

    fn on_sell(&mut self, s: TradeSignal) -> Action {
        let now = now_ts();
        
        // Caso 1: Posición abierta confirmada -> vender
        if self.state.open_positions.contains_key(&s.mint) {
            // No remover aquí - el main loop lo hará si el SELL tiene éxito
            let pos = self.state.open_positions.get(&s.mint).unwrap();
            println!(
                "🎯 [COPY] Intentando SELL | mint={} | my_sol={:.4} | leader_sig={}",
                &s.mint[..8.min(s.mint.len())],
                pos.my_trade_sol,
                &s.sig[..8.min(s.sig.len())]
            );
            return Action::Sell {
                mint: s.mint,
                reason: "leader_sell".to_string(),
            };
        }
        
        // Caso 2: Posición untracked (reconciliación) -> vender si config lo permite
        if self.risk.reconcile_untracked_sell && self.untracked_positions.contains_key(&s.mint) {
            let bal = self.untracked_positions.get(&s.mint).copied().unwrap_or(0);
            println!(
                "🔄 [RECONCILE] SELL untracked | mint={} | balance={} | vendiendo (líder vendió)",
                &s.mint[..8.min(s.mint.len())], bal
            );
            return Action::Sell {
                mint: s.mint,
                reason: "leader_sell_untracked".to_string(),
            };
        }

        // Caso 3: Hay pending BUY -> esperar y reintentar
        if self.state.pending_buys.contains_key(&s.mint) {
            println!(
                "⏳ [COPY] SELL llegó pero BUY pending | mint={} | esperando...",
                &s.mint[..8.min(s.mint.len())]
            );
            return Action::WaitAndSell {
                mint: s.mint,
                wait_ms: 2000,    // 2 segundos entre intentos
                max_retries: 3,  // máximo 3 intentos (total ~6s)
            };
        }
        
        // Caso 4: Ni posición ni pending ni untracked
        // Si hubo BUY fallido O ignorado reciente -> no orphan (solo cooldown)
        self.cleanup_recent_failed_buys();
        self.cleanup_recent_ignored_mints();

        if let Some(failed) = self.recent_failed_buys.get(&s.mint) {
            if now - failed.ts < FAILED_BUY_TTL_SECS {
                self.state.cooldown_blacklist.insert(
                    s.mint.clone(),
                    CooldownEntry {
                        reason: format!("failed_buy_recent: {}", failed.reason),
                        until_ts: now + self.risk.cooldown_secs,
                    },
                );
                self.save_state();
                println!(
                    "ℹ️ [SKIP] SELL sin posición pero BUY falló hace {}s -> cooldown (no orphan) | mint={}",
                    now - failed.ts, &s.mint[..8.min(s.mint.len())]
                );
                return Action::Skip {
                    reason: format!("recent_failed_buy ({})", failed.reason),
                };
            }
        }

        if let Some(ignored) = self.recent_ignored_mints.get(&s.mint) {
            if now - ignored.ts < IGNORED_MINT_TTL_SECS {
                self.state.cooldown_blacklist.insert(
                    s.mint.clone(),
                    CooldownEntry {
                        reason: format!("ignored_buy_recent: {}", ignored.reason),
                        until_ts: now + self.risk.cooldown_secs,
                    },
                );
                self.save_state();
                println!(
                    "ℹ️ [SKIP] SELL sin posición pero BUY ignorado hace {}min -> cooldown (no orphan) | mint={}",
                    (now - ignored.ts) / 60, &s.mint[..8.min(s.mint.len())]
                );
                return Action::Skip {
                    reason: format!("recent_ignored_buy ({})", ignored.reason),
                };
            }
        }

        // Caso 5: Real orphan - log diagnóstico
        let untracked_bal = self.untracked_positions.get(&s.mint).copied().unwrap_or(0);
        println!(
            "ℹ️ [SELL_NO_POS] mint={} full={} | failed_buy={} ignored={} untracked_bal={} -> ORPHAN",
            &s.mint[..8.min(s.mint.len())],
            s.mint,
            self.recent_failed_buys.contains_key(&s.mint),
            self.recent_ignored_mints.contains_key(&s.mint),
            untracked_bal
        );

        self.state.orphan_sells.insert(
            s.mint.clone(),
            OrphanSell {
                sell_sig: s.sig.clone(),
                sell_ts: now,
            },
        );

        self.state.cooldown_blacklist.insert(
            s.mint.clone(),
            CooldownEntry {
                reason: "orphan_sell".to_string(),
                until_ts: now + self.risk.cooldown_secs,
            },
        );

        self.save_state();

        let reason = format!(
            "SELL sin posición ni pending -> orphan + cooldown | mint={}",
            &s.mint[..8.min(s.mint.len())]
        );
        println!("ℹ️ [ORPHAN] {}", reason);
        Action::Skip { reason }
    }
}
