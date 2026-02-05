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
mod jupiter;

use config::Config;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::read_keypair_file;
use tokio::sync::mpsc;
use crate::decoder::analyze_transaction_async;
use crate::engine::{Action, DecisionEngine, RiskConfig};
use crate::csv_logger::CsvLogger;
use crate::executor::{Executor, ExecutorConfig};
use crate::broadcaster::{BroadcastConfig, Broadcaster};
use crate::tx_builder::{TxBuilder, TxBuilderConfig};
use tokio::time::{sleep, Duration};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load();

    let args: Vec<String> = std::env::args().collect();
    let test_jito = args.iter().any(|a| a == "--test-jito");
    let sell_all = args.iter().any(|a| a == "--sell-all");

    if test_jito {
        return run_jito_test(&cfg).await;
    }
    if sell_all {
        return run_sell_all(&cfg).await;
    }

    let git_sha = option_env!("GIT_SHA").unwrap_or("unknown");
    let build_ts = option_env!("BUILD_TS").unwrap_or("unknown");
    println!("🤖 Bot iniciado | version={} | build={}", git_sha, build_ts);
    println!("═══════════════════════════════════════════════════════");

    // Canal compartido para todas las transacciones (WS + poller)
    let (txq, mut rxq) = mpsc::channel::<(String, String)>(200);

    // Risk config con SIZING DINÁMICO
    // MODO TEST SEGURO: trades chicos para validar pipeline
    // - max 0.01 SOL por trade
    // - exposure cap bajo
    let risk = RiskConfig {
        // Sizing dinámico - MODO TEST
        min_trade_sol: 0.005,        // mínimo por trade (test)
        max_trade_sol: 0.01,         // máximo por trade (test)
        k_leader_scale: 0.005,       // my_trade = k * abs(leader_delta)
        
        // Quality Gate
        min_leader_sol_delta: 0.10,  // filtrar micro-ruido del líder
        exposure_cap_sol: 0.15,      // máximo total expuesto
        reserve_sol: 0.05,           // colchón intocable (fees + margen)
        total_capital_sol: 0.25,     // mi capital total
        
        // Rate limits y timing
        min_buy_interval_secs: 15,   // evitar spam
        cooldown_secs: 60,           // cooldown después de orphan sell
        max_hold_secs: 6 * 60 * 60,  // 6 horas max hold
        reconcile_untracked_sell: cfg.reconcile_untracked_sell,
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

    // Executor config (ejecución REAL activada)
    let exec_config = ExecutorConfig {
        rpc_url: cfg.helius_http.clone(),
        dry_run: false, // EJECUCIÓN REAL ACTIVADA
        jito_enabled: cfg.jito_enabled(),
        jito_url: cfg.jito_url.clone(),
        jito_auth: cfg.jito_auth.clone(),
        jito_tip_lamports: cfg.jito_tip_lamports,
        compute_units: 200_000,
        priority_fee_micro_lamports: 1_000,
        keypair_path: cfg.keypair_path.clone(),
        jupiter_api_key: cfg.jupiter_api_key.clone(),
        slippage_bps: 300, // 3% slippage (pump tokens volatiles)
        reserve_sol: risk.reserve_sol,
    };

    println!("🎮 Executor Config:");
    println!("   - dry_run: {} {}", exec_config.dry_run, if exec_config.dry_run { "(simulación)" } else { "(REAL!)" });
    println!("   - jito_enabled: {}", exec_config.jito_enabled);
    if exec_config.jito_enabled {
        println!("   - jito_tip: {} lamports", exec_config.jito_tip_lamports);
    }
    println!("   - slippage: {}bps ({}%)", exec_config.slippage_bps, exec_config.slippage_bps as f64 / 100.0);
    println!("   - jupiter_api: {}", if exec_config.jupiter_api_key.is_some() { "configured" } else { "free tier" });
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

        // Reconciliación: detectar posiciones fuera del state
        if let Some(owner) = executor.owner_pubkey() {
            if let Ok(balances) = executor.get_all_token_balances(&owner).await {
                engine.reconcile_untracked(balances);
            }
        }

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

                        // Ejecutar acción con el nuevo ciclo de vida
                        match action {
                            Action::Buy { mint, sol_amount, leader_delta: _, leader_sig: _ } => {
                                // pending_buy ya fue agregado por el engine
                                match executor.execute_buy(&mint, sol_amount).await {
                                    Ok(result) => {
                                        // ÉXITO: confirmar posición con MI signature
                                        engine.confirm_position(
                                            &mint,
                                            &result.my_sig,
                                            result.my_token_balance,
                                            result.my_sol_spent,
                                        );
                                    }
                                    Err(e) => {
                                        let err_str = e.to_string();
                                        engine.cancel_pending_buy(&mint, &err_str); // incluye record_failed_buy
                                        if err_str.contains("0x2") || err_str.contains("Invalid Mint") || err_str.contains("Mint invalid") {
                                            engine.add_invalid_mint_cooldown(&mint, 60 * 60); // 60 min
                                        }
                                        if err_str.contains("insufficient_sol") {
                                            println!("   ⛔ [GUARD] No se intentó swap (balance insuficiente)");
                                        }
                                    }
                                }
                            }
                            
                            Action::Sell { mint, reason } => {
                                match executor.execute_sell(&mint, &reason).await {
                                    Ok(_result) => {
                                        // ÉXITO: remover posición (open o untracked)
                                        engine.remove_position(&mint);
                                        engine.untracked_positions.remove(&mint);
                                    }
                                    Err(e) => {
                                        eprintln!("⚠️ [MAIN] SELL failed for {}: {}", &mint[..8.min(mint.len())], e);
                                        let err_str = e.to_string();
                                        if err_str.contains("0x2") || err_str.contains("Invalid Mint") || err_str.contains("Mint invalid") {
                                            engine.add_invalid_mint_cooldown(&mint, 60 * 60); // 60 min
                                        }
                                    }
                                }
                            }
                            
                            Action::WaitAndSell { mint, wait_ms, max_retries } => {
                                // SELL llegó pero BUY está pending - esperar y reintentar
                                for attempt in 0..max_retries {
                                    println!("⏳ [WAIT] SELL attempt {}/{} for {} - waiting {}ms", 
                                             attempt + 1, max_retries, &mint[..8.min(mint.len())], wait_ms);
                                    sleep(Duration::from_millis(wait_ms)).await;
                                    
                                    // Re-check si ahora hay posición abierta
                                    if engine.state.open_positions.contains_key(&mint) {
                                        match executor.execute_sell(&mint, "leader_sell_delayed").await {
                                            Ok(_) => {
                                                engine.remove_position(&mint);
                                                println!("✅ [WAIT] SELL delayed exitoso para {}", &mint[..8.min(mint.len())]);
                                            }
                                            Err(e) => {
                                                eprintln!("⚠️ [WAIT] SELL delayed failed: {}", e);
                                                let err_str = e.to_string();
                                                if err_str.contains("0x2") || err_str.contains("Invalid Mint") || err_str.contains("Mint invalid") {
                                                    engine.add_invalid_mint_cooldown(&mint, 60 * 60);
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    
                                    // Si ya no hay pending ni position, salir
                                    if !engine.has_pending_buy(&mint) && !engine.state.open_positions.contains_key(&mint) {
                                        println!("ℹ️ [WAIT] No pending ni position para {} - skip", &mint[..8.min(mint.len())]);
                                        break;
                                    }
                                }
                            }
                            
                            Action::Skip { reason } => {
                                if matches!(signal.side, crate::signals::Side::Buy) {
                                    if !reason.contains("Ya hay posición") && !reason.contains("Ya hay pending") {
                                        engine.record_ignored_mint(&signal.mint, &reason);
                                    }
                                }
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

/// Test Jito bundle pipeline without real swaps
async fn run_jito_test(cfg: &Config) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════");
    println!("🧪 TEST JITO BUNDLE");
    println!("═══════════════════════════════════════════════════════");

    // 1. Check config
    let keypair_path = cfg.keypair_path.as_ref()
        .ok_or_else(|| anyhow::anyhow!("KEYPAIR_PATH not set in .env"))?;
    
    let jito_url = cfg.jito_url.as_ref()
        .ok_or_else(|| anyhow::anyhow!("JITO_URL not set in .env"))?;

    println!("📁 Keypair: {}", keypair_path);
    println!("🌐 Jito URL: {}", jito_url);
    println!("💰 Tip: {} lamports", cfg.jito_tip_lamports);

    // 2. Load keypair
    let payer = read_keypair_file(keypair_path)
        .map_err(|e| anyhow::anyhow!("Failed to load keypair: {}", e))?;
    
    use solana_sdk::signer::Signer;
    println!("🔑 Bot pubkey: {}", payer.pubkey());

    // 3. Create broadcaster
    let broadcaster = Broadcaster::new(BroadcastConfig {
        jito_enabled: true,
        jito_tip_lamports: cfg.jito_tip_lamports,
        rpc_url: cfg.helius_http.clone(),
        jito_url: Some(jito_url.clone()),
        jito_auth: cfg.jito_auth.clone(),
    });

    // 4. Create tx builder
    let tx_builder = TxBuilder::new(TxBuilderConfig::default());

    // 5. Get RPC client for blockhash
    let rpc_client = RpcClient::new(cfg.helius_http.clone());

    // 6. Check balance
    let balance = rpc_client.get_balance(&payer.pubkey()).await?;
    let balance_sol = balance as f64 / 1_000_000_000.0;
    println!("💵 Balance: {} SOL ({} lamports)", balance_sol, balance);

    if balance < 50_000 {
        return Err(anyhow::anyhow!("Insufficient balance! Need at least 50,000 lamports for test"));
    }

    // 7. Get tip account from Jito
    println!("\n📡 Fetching tip accounts from Jito...");
    let tip_account_str = broadcaster.pick_tip_account().await?;
    let tip_pubkey: Pubkey = tip_account_str.parse()?;
    println!("✅ Tip account: {}", tip_pubkey);

    // 8. Get recent blockhash
    println!("\n📡 Getting recent blockhash...");
    let blockhash = tx_builder.get_recent_blockhash(&rpc_client).await?;
    println!("✅ Blockhash: {}", blockhash);

    // 9. Build transactions
    println!("\n🔧 Building transactions...");
    
    // Dummy tx: self-transfer of 1 lamport (valid but does nothing)
    let dummy_tx = tx_builder.build_self_transfer_tx(&payer, blockhash)?;
    println!("✅ Dummy TX built (self-transfer 1 lamport)");

    // Tip tx: transfer to Jito tip account
    let tip_tx = tx_builder.build_tip_tx(&payer, blockhash, &tip_pubkey, cfg.jito_tip_lamports)?;
    println!("✅ Tip TX built ({} lamports to {})", cfg.jito_tip_lamports, &tip_account_str[..8]);

    // 10. Send bundle ONCE!
    println!("\n📦 Sending bundle to Jito (ONE TIME ONLY)...");
    let t_send_start = std::time::Instant::now();
    let bundle_id = broadcaster.send_bundle_base64(&[dummy_tx, tip_tx]).await?;
    let send_ms = t_send_start.elapsed().as_millis() as u64;
    println!("✅ Bundle sent! ID: {}", bundle_id);
    println!("   └─ send_ms: {}ms", send_ms);

    // 11. Poll status with backoff (NO RE-SEND!)
    println!("\n📊 Polling bundle status (backoff: 500ms->1s->2s->4s, timeout: 30s)...");
    let (landed, status_str, land_ms) = broadcaster.poll_bundle_until_landed(&bundle_id, 30_000).await;

    println!("\n═══════════════════════════════════════════════════════");
    if landed {
        println!("🎉 BUNDLE LANDED!");
        println!("   └─ status: {}", status_str);
        println!("   └─ land_ms: {}ms (time from send to confirmed)", land_ms);
        println!("   └─ total pipeline: send={}ms + land={}ms = {}ms", send_ms, land_ms, send_ms + land_ms);
    } else {
        println!("⚠️ BUNDLE DID NOT LAND (may still be processing)");
        println!("   └─ last status: {}", status_str);
        println!("   └─ This could be: rate limit, dropped, or slow confirmation");
        println!("\n💡 Check manually:");
        println!("   https://explorer.jito.wtf/bundle/{}", bundle_id);
    }
    println!("═══════════════════════════════════════════════════════");

    println!("\n📊 METRICS SUMMARY:");
    println!("   ├─ send_ms: {}ms (time to submit to Jito)", send_ms);
    println!("   ├─ land_ms: {}ms (time until confirmed)", land_ms);
    println!("   └─ total: {}ms", send_ms + land_ms);
    
    if landed {
        println!("\n✅ Pipeline verificado. Podés usar el bot con confianza.");
    }

    Ok(())
}

/// Vende todos los tokens a SOL y resetea state (--sell-all)
async fn run_sell_all(cfg: &Config) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════");
    println!("🔥 SELL ALL - Liquidar todo a SOL");
    println!("═══════════════════════════════════════════════════════");

    let keypair_path = cfg.keypair_path.as_ref()
        .ok_or_else(|| anyhow::anyhow!("KEYPAIR_PATH not set in .env"))?;

    let risk = RiskConfig {
        min_trade_sol: 0.005,
        max_trade_sol: 0.01,
        k_leader_scale: 0.005,
        min_leader_sol_delta: 0.10,
        exposure_cap_sol: 1.0,
        reserve_sol: 0.01,
        total_capital_sol: 1.0,
        min_buy_interval_secs: 0,
        cooldown_secs: 0,
        max_hold_secs: 0,
        reconcile_untracked_sell: true,
    };

    let exec_config = ExecutorConfig {
        rpc_url: cfg.helius_http.clone(),
        dry_run: false,
        jito_enabled: cfg.jito_enabled(),
        jito_url: cfg.jito_url.clone(),
        jito_auth: cfg.jito_auth.clone(),
        jito_tip_lamports: cfg.jito_tip_lamports,
        compute_units: 200_000,
        priority_fee_micro_lamports: 1_000,
        keypair_path: Some(keypair_path.clone()),
        jupiter_api_key: cfg.jupiter_api_key.clone(),
        slippage_bps: 500, // 5% para liquidación
        reserve_sol: risk.reserve_sol,
    };

    let mut executor = Executor::new(exec_config);
    let owner = executor.owner_pubkey()
        .ok_or_else(|| anyhow::anyhow!("No keypair loaded"))?;

    println!("🔑 Wallet: {}", owner);

    let balances = executor.get_all_token_balances(&owner).await?;
    let to_sell: Vec<_> = balances.into_iter()
        .filter(|(mint, bal)| *mint != "So11111111111111111111111111111111111111112" && *bal > 0)
        .collect();

    if to_sell.is_empty() {
        println!("✅ No hay tokens para vender (solo SOL)");
    } else {
        println!("📋 Tokens a vender: {}", to_sell.len());
        for (mint, bal) in &to_sell {
            println!("   └─ {} | balance={}", &mint[..8.min(mint.len())], bal);
        }

        for (mint, _) in &to_sell {
            println!("\n🔄 Vendiendo {}...", &mint[..8.min(mint.len())]);
            for retry in 0..3 {
                match executor.execute_sell(mint, "sell_all").await {
                    Ok(r) => {
                        println!("   ✅ Vendido | ~{:.6} SOL", r.sol_received);
                        break;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("Blockhash not found") && retry < 2 {
                            println!("   ⚡ Blockhash expirado, reintentando ({})...", retry + 2);
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            eprintln!("   ⚠️ Falló: {}", e);
                            break;
                        }
                    }
                }
            }
            sleep(Duration::from_secs(2)).await; // pausa entre ventas
        }
    }

    let rpc = RpcClient::new(cfg.helius_http.clone());
    let final_bal = rpc.get_balance(&owner).await?;
    println!("\n💰 Balance final: {:.6} SOL", final_bal as f64 / 1e9);

    let state_path = "state.json";
    let empty = serde_json::json!({
        "pending_buys": {},
        "open_positions": {},
        "orphan_sells": {},
        "cooldown_blacklist": {},
        "last_processed_ts": 0,
        "last_buy_ts": 0
    });
    std::fs::write(state_path, serde_json::to_string_pretty(&empty).unwrap_or_default())?;
    println!("📂 state.json reseteado");

    println!("\n✅ Listo para empezar de 0");
    Ok(())
}
