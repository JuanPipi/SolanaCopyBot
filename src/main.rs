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

    // Check for --test-jito flag
    let args: Vec<String> = std::env::args().collect();
    let test_jito = args.iter().any(|a| a == "--test-jito");

    if test_jito {
        return run_jito_test(&cfg).await;
    }

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

    // 10. Send bundle!
    println!("\n📦 Sending bundle to Jito...");
    let bundle_id = broadcaster.send_bundle_base64(&[dummy_tx, tip_tx]).await?;
    println!("✅ Bundle sent! ID: {}", bundle_id);

    // 11. Check status (optional)
    println!("\n📊 Checking bundle status...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    let status = broadcaster.get_bundle_statuses(&[bundle_id.clone()]).await?;
    println!("📦 Bundle status: {}", serde_json::to_string_pretty(&status)?);

    println!("\n═══════════════════════════════════════════════════════");
    println!("🎉 JITO TEST COMPLETE!");
    println!("═══════════════════════════════════════════════════════");
    println!("\nIf bundle status shows 'Landed', your pipeline is working!");
    println!("You can now switch to real swaps by setting dry_run: false");

    Ok(())
}
