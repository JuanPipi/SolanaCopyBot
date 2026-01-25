mod config;
mod listener;
mod decoder;
mod signals;
mod engine;
mod csv_logger;
mod metrics;
mod broadcaster;
mod prepared;
mod tx_builder;
mod executor;

use config::Config;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::mpsc;
use crate::decoder::analyze_transaction_async;
use crate::engine::{Action, DecisionEngine, RiskConfig};
use crate::csv_logger::CsvLogger;
use crate::executor::{Executor, ExecutorConfig};
use tokio::time::{sleep, Duration};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load();

    println!("🚀 Bot iniciado");
    println!("═══════════════════════════════════════════════════════");

    // Canal compartido para todas las transacciones (WS + poller)
    let (txq, mut rxq) = mpsc::channel::<(String, String)>(200);

    // Risk config con SIZING DINÁMICO
    // Con 1 SOL de capital:
    // - Trades entre 0.02 y 0.10 SOL según convicción del líder
    // - k=0.035 significa: si líder mete 2 SOL -> yo meto 0.07 SOL
    // - Exposure cap 0.35 SOL -> puedo tener 3-5 posiciones chicas
    // - Reserve 0.20 SOL -> nunca me quedo sin gas
    let risk = RiskConfig {
        // Sizing dinámico
        min_trade_sol: 0.02,         // mínimo por trade
        max_trade_sol: 0.10,         // máximo por trade
        k_leader_scale: 0.035,       // my_trade = k * abs(leader_delta)
        
        // Quality Gate
        min_leader_sol_delta: 0.15,  // filtrar micro-ruido del líder
        exposure_cap_sol: 0.35,      // máximo total expuesto
        reserve_sol: 0.20,           // colchón intocable (fees + margen)
        total_capital_sol: 1.0,      // mi capital total
        
        // Rate limits y timing
        min_buy_interval_secs: 15,   // evitar spam
        cooldown_secs: 60,           // cooldown después de orphan sell
        max_hold_secs: 6 * 60 * 60,  // 6 horas max hold
    };

    println!("📋 Risk Config (Sizing Dinámico):");
    println!("   - trade_range: {}-{} SOL", risk.min_trade_sol, risk.max_trade_sol);
    println!("   - k_scale: {} (líder 2 SOL -> yo ~{:.3} SOL)", risk.k_leader_scale, risk.k_leader_scale * 2.0);
    println!("   - min_leader_delta: {} SOL", risk.min_leader_sol_delta);
    println!("   - exposure_cap: {} SOL", risk.exposure_cap_sol);
    println!("   - reserve: {} SOL", risk.reserve_sol);
    println!("   - total_capital: {} SOL", risk.total_capital_sol);
    println!("   - min_buy_interval: {}s", risk.min_buy_interval_secs);
    println!("   - cooldown: {}s | max_hold: {}h", risk.cooldown_secs, risk.max_hold_secs / 3600);
    println!("═══════════════════════════════════════════════════════");

    // Executor config (dry_run por defecto)
    let exec_config = ExecutorConfig {
        rpc_url: cfg.helius_http.clone(),
        dry_run: true, // IMPORTANTE: cambiar a false para ejecución real
        jito_enabled: cfg.jito_enabled(),
        jito_url: cfg.jito_url.clone(),
        jito_auth: cfg.jito_auth.clone(),
        jito_tip_lamports: cfg.jito_tip_lamports,
        compute_units: 200_000,
        priority_fee_micro_lamports: 1_000,
        keypair_path: cfg.keypair_path.clone(),
    };

    println!("🎮 Executor Config:");
    println!("   - dry_run: {} (NO ejecuta trades reales)", exec_config.dry_run);
    println!("   - jito_enabled: {}", exec_config.jito_enabled);
    if exec_config.jito_enabled {
        println!("   - jito_tip: {} lamports", exec_config.jito_tip_lamports);
    }
    println!("   - compute_units: {}", exec_config.compute_units);
    println!("   - priority_fee: {} micro-lamports/CU", exec_config.priority_fee_micro_lamports);
    println!("═══════════════════════════════════════════════════════");

    // RPC Client compartido (evita crear uno nuevo por cada tx)
    let rpc_client = Arc::new(RpcClient::new(cfg.helius_http.clone()));
    let rpc_client_worker = rpc_client.clone();

    // Worker: procesa signatures a ritmo controlado + dedupe global
    let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let seen_worker = seen.clone();

    tokio::spawn(async move {
        let mut engine = DecisionEngine::new(risk, "state.json");
        let mut executor = Executor::new(exec_config);
        let mut csv_logger = CsvLogger::new("signals.csv").ok();

        println!("✅ Engine y Executor inicializados");
        println!("✅ RpcClient compartido activo");

        while let Some((wallet, sig)) = rxq.recv().await {
            let sig = sig.trim().to_string();

            // Dedupe global (WS + poller)
            {
                let mut s = seen_worker.lock().await;
                if !s.insert(sig.clone()) {
                    continue;
                }
                // Limpieza para que no crezca infinito
                if s.len() > 50_000 {
                    s.clear();
                }
            }

            // Extraer wallet limpio y source (sin tag |WS o |POLL)
            let (wallet_clean, source) = if wallet.contains('|') {
                let parts: Vec<&str> = wallet.split('|').collect();
                (parts[0].to_string(), parts.get(1).unwrap_or(&"?").to_string())
            } else {
                (wallet.clone(), "?".to_string())
            };

            // Pacing: 120ms ≈ 8.3 req/s (seguro con tu límite 10/s)
            sleep(Duration::from_millis(120)).await;

            match analyze_transaction_async(&rpc_client_worker, &wallet_clean, &sig).await {
                Ok(Some(signal)) => {
                    // Log CSV con source
                    if let Some(logger) = csv_logger.as_mut() {
                        logger.log_signal(&signal, &source);
                    }

                    // Decision Engine (Quality Gate + Sizing Dinámico)
                    let actions = engine.handle_signal(signal.clone());

                    // Ejecutar TODAS las acciones
                    for action in actions {
                        // Log de decisión al CSV
                        if let Some(logger) = csv_logger.as_mut() {
                            let signal_ref = match &action {
                                Action::Sell { reason, .. } if reason == "max_hold" => None,
                                _ => Some(&signal),
                            };
                            logger.log_decision(signal_ref, &action);
                        }

                        // Executor (Build -> Broadcast -> Confirm)
                        match &action {
                            Action::Buy { .. } | Action::Sell { .. } => {
                                if let Err(e) = executor.execute(action).await {
                                    eprintln!("⚠️ Error ejecutando: {}", e);
                                }
                            }
                            Action::Skip { .. } => {
                                // Ya se logueó en el engine
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => eprintln!("⚠️ Error analizando {}: {}", sig, e),
            }

            // Limpiar cache periódicamente
            executor.cleanup_cache();
        }
    });

    // WebSocket para todas las wallets
    listener::listen_wallets(
        &cfg.helius_wss,
        &cfg.helius_http,
        &cfg.wallets,
        txq,
    )
    .await?;

    Ok(())
}
