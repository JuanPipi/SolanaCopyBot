#![allow(dead_code)]
#![allow(unused_imports)]

use std::sync::Arc;
use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer, read_keypair_file};

use crate::broadcaster::{BroadcastConfig, BroadcastResult, Broadcaster};
use crate::engine::Action;
use crate::metrics::MetricsTracker;
use crate::prepared::{PreparedSwapCache, SwapPreparer};
use crate::tx_builder::{SwapInstructionBuilder, TxBuilder, TxBuilderConfig};

pub struct ExecutorConfig {
    pub rpc_url: String,
    pub dry_run: bool,
    pub jito_enabled: bool,
    pub jito_url: Option<String>,
    pub jito_auth: Option<String>,
    pub jito_tip_lamports: u64,
    pub compute_units: u32,
    pub priority_fee_micro_lamports: u64,
    pub keypair_path: Option<String>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            rpc_url: String::new(),
            dry_run: true,
            jito_enabled: false,
            jito_url: None,
            jito_auth: None,
            jito_tip_lamports: 20_000,
            compute_units: 200_000,
            priority_fee_micro_lamports: 1_000,
            keypair_path: None,
        }
    }
}

pub struct Executor {
    config: ExecutorConfig,
    rpc_client: Arc<RpcClient>,
    broadcaster: Broadcaster,
    tx_builder: TxBuilder,
    swap_cache: PreparedSwapCache,
    swap_preparer: SwapPreparer,
    payer: Option<Keypair>,
}

impl Executor {
    pub fn new(config: ExecutorConfig) -> Self {
        let rpc_client = Arc::new(RpcClient::new(config.rpc_url.clone()));

        let broadcaster = Broadcaster::new(BroadcastConfig {
            jito_enabled: config.jito_enabled,
            jito_tip_lamports: config.jito_tip_lamports,
            rpc_url: config.rpc_url.clone(),
            jito_url: config.jito_url.clone(),
            jito_auth: config.jito_auth.clone(),
        });

        let tx_builder = TxBuilder::new(TxBuilderConfig {
            compute_units: config.compute_units,
            priority_fee_micro_lamports: config.priority_fee_micro_lamports,
        });

        let swap_cache = PreparedSwapCache::new(300); // 5 min TTL
        let swap_preparer = SwapPreparer::new();

        // Load keypair if path provided and not in dry_run
        let payer = if !config.dry_run {
            if let Some(ref path) = config.keypair_path {
                match read_keypair_file(path) {
                    Ok(kp) => {
                        println!("🔑 [EXEC] Keypair loaded: {}", kp.pubkey());
                        Some(kp)
                    }
                    Err(e) => {
                        println!("⚠️ [EXEC] Failed to load keypair: {}", e);
                        None
                    }
                }
            } else {
                println!("⚠️ [EXEC] No keypair path, real execution disabled");
                None
            }
        } else {
            None
        };

        Self {
            config,
            rpc_client,
            broadcaster,
            tx_builder,
            swap_cache,
            swap_preparer,
            payer,
        }
    }

    /// Ejecuta una acción (BUY/SELL)
    pub async fn execute(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Buy { mint, sol_amount, reason } => {
                self.execute_buy(&mint, sol_amount, &reason).await
            }
            Action::Sell { mint, reason } => {
                self.execute_sell(&mint, &reason).await
            }
            Action::Skip { .. } => {
                Ok(())
            }
        }
    }

    async fn execute_buy(&mut self, mint: &str, sol_amount: f64, reason: &str) -> Result<()> {
        let mut metrics = MetricsTracker::new();

        println!(
            "🔄 [EXEC] Iniciando BUY | mint={} | sol={:.6} | reason={} | dry_run={}",
            mint, sol_amount, reason, self.config.dry_run
        );

        metrics.mark_detect_done();

        // Preparar swap (buscar pool, accounts, etc.)
        let _prepared = if let Some(cached) = self.swap_cache.get(mint) {
            println!("✅ [CACHE] Hit para mint={}", mint);
            cached.clone()
        } else {
            println!("🔍 [CACHE] Miss, preparando mint={}", mint);
            match self.swap_preparer.prepare(mint).await {
                Some(p) => {
                    self.swap_cache.insert(mint.to_string(), p.clone());
                    p
                }
                None => {
                    let m = metrics.finalize(false, Some("No se pudo preparar swap".to_string()));
                    m.log("BUY", mint);
                    return Ok(());
                }
            }
        };

        // DRY RUN: solo simular
        if self.config.dry_run {
            println!("🏜️ [DRY_RUN] BUY simulado | mint={} | sol={:.6} | reason={}", mint, sol_amount, reason);
            metrics.mark_build_done();
            metrics.mark_send_done();
            let m = metrics.finalize(true, None);
            m.log("BUY (dry)", mint);
            return Ok(());
        }

        // REAL EXECUTION
        let payer = match &self.payer {
            Some(kp) => kp,
            None => {
                println!("❌ [EXEC] No keypair loaded, cannot execute real BUY");
                let m = metrics.finalize(false, Some("No keypair".to_string()));
                m.log("BUY", mint);
                return Ok(());
            }
        };

        // Get blockhash
        let blockhash = self.tx_builder.get_recent_blockhash(&self.rpc_client).await?;

        // Build swap TX (placeholder - needs real DEX implementation)
        // For now, build a dummy tx for testing the bundle pipeline
        let swap_tx = self.tx_builder.build_dummy_tx(payer, blockhash)?;
        metrics.mark_build_done();

        // Si Jito está habilitado, crear bundle con tip
        if self.config.jito_enabled && self.config.jito_url.is_some() {
            match self.broadcaster.pick_tip_account().await {
                Ok(tip_account_str) => {
                    let tip_pubkey: Pubkey = tip_account_str.parse()
                        .map_err(|e| anyhow!("Invalid tip account pubkey: {}", e))?;
                    
                    let tip_tx = self.tx_builder.build_tip_tx(
                        payer,
                        blockhash,
                        &tip_pubkey,
                        self.config.jito_tip_lamports,
                    )?;

                    println!("📦 [EXEC] Enviando bundle [swap_tx, tip_tx] via Jito...");
                    let result = self.broadcaster.send_bundle_with_fallback(&swap_tx, &tip_tx).await;
                    metrics.mark_send_done();

                    match result {
                        BroadcastResult::BundleSuccess { bundle_id, via } => {
                            println!("✅ [EXEC] Bundle exitoso: {} via {}", bundle_id, via);
                            let m = metrics.finalize(true, None);
                            m.log("BUY (bundle)", mint);
                        }
                        BroadcastResult::Success { signature, via } => {
                            println!("✅ [EXEC] TX exitosa (fallback): {} via {}", signature, via);
                            let m = metrics.finalize(true, None);
                            m.log("BUY (rpc_fallback)", mint);
                        }
                        BroadcastResult::Failed { error } => {
                            println!("❌ [EXEC] BUY falló: {}", error);
                            let m = metrics.finalize(false, Some(error));
                            m.log("BUY", mint);
                        }
                    }
                }
                Err(e) => {
                    println!("⚠️ [EXEC] No pude obtener tip account: {}, fallback RPC", e);
                    let result = self.broadcaster.send_with_fallback(&swap_tx).await;
                    metrics.mark_send_done();
                    self.handle_broadcast_result(result, metrics, "BUY", mint);
                }
            }
        } else {
            // Sin Jito, enviar por RPC directo
            let result = self.broadcaster.send_with_fallback(&swap_tx).await;
            metrics.mark_send_done();
            self.handle_broadcast_result(result, metrics, "BUY", mint);
        }

        Ok(())
    }

    async fn execute_sell(&mut self, mint: &str, reason: &str) -> Result<()> {
        let mut metrics = MetricsTracker::new();

        println!(
            "🔄 [EXEC] Iniciando SELL | mint={} | reason={} | dry_run={}",
            mint, reason, self.config.dry_run
        );

        metrics.mark_detect_done();

        if self.config.dry_run {
            println!("🏜️ [DRY_RUN] SELL simulado | mint={} | reason={}", mint, reason);
            metrics.mark_build_done();
            metrics.mark_send_done();
            let m = metrics.finalize(true, None);
            m.log(&format!("SELL (dry, {})", reason), mint);
            return Ok(());
        }

        // REAL EXECUTION
        let payer = match &self.payer {
            Some(kp) => kp,
            None => {
                println!("❌ [EXEC] No keypair loaded, cannot execute real SELL");
                let m = metrics.finalize(false, Some("No keypair".to_string()));
                m.log("SELL", mint);
                return Ok(());
            }
        };

        // Get blockhash
        let blockhash = self.tx_builder.get_recent_blockhash(&self.rpc_client).await?;

        // Build sell TX (placeholder - needs real DEX implementation)
        // TODO: get token balance from ATA and sell all
        let swap_tx = self.tx_builder.build_dummy_tx(payer, blockhash)?;
        metrics.mark_build_done();

        // Si Jito está habilitado, crear bundle con tip
        if self.config.jito_enabled && self.config.jito_url.is_some() {
            match self.broadcaster.pick_tip_account().await {
                Ok(tip_account_str) => {
                    let tip_pubkey: Pubkey = tip_account_str.parse()
                        .map_err(|e| anyhow!("Invalid tip account pubkey: {}", e))?;
                    
                    let tip_tx = self.tx_builder.build_tip_tx(
                        payer,
                        blockhash,
                        &tip_pubkey,
                        self.config.jito_tip_lamports,
                    )?;

                    println!("📦 [EXEC] Enviando bundle [sell_tx, tip_tx] via Jito...");
                    let result = self.broadcaster.send_bundle_with_fallback(&swap_tx, &tip_tx).await;
                    metrics.mark_send_done();

                    match result {
                        BroadcastResult::BundleSuccess { bundle_id, via } => {
                            println!("✅ [EXEC] Bundle exitoso: {} via {}", bundle_id, via);
                            let m = metrics.finalize(true, None);
                            m.log(&format!("SELL bundle ({})", reason), mint);
                        }
                        BroadcastResult::Success { signature, via } => {
                            println!("✅ [EXEC] TX exitosa (fallback): {} via {}", signature, via);
                            let m = metrics.finalize(true, None);
                            m.log(&format!("SELL rpc_fallback ({})", reason), mint);
                        }
                        BroadcastResult::Failed { error } => {
                            println!("❌ [EXEC] SELL falló: {}", error);
                            let m = metrics.finalize(false, Some(error));
                            m.log("SELL", mint);
                        }
                    }
                }
                Err(e) => {
                    println!("⚠️ [EXEC] No pude obtener tip account: {}, fallback RPC", e);
                    let result = self.broadcaster.send_with_fallback(&swap_tx).await;
                    metrics.mark_send_done();
                    self.handle_broadcast_result(result, metrics, &format!("SELL ({})", reason), mint);
                }
            }
        } else {
            let result = self.broadcaster.send_with_fallback(&swap_tx).await;
            metrics.mark_send_done();
            self.handle_broadcast_result(result, metrics, &format!("SELL ({})", reason), mint);
        }

        Ok(())
    }

    fn handle_broadcast_result(&self, result: BroadcastResult, metrics: MetricsTracker, op: &str, mint: &str) {
        match result {
            BroadcastResult::Success { signature, via } => {
                println!("✅ [EXEC] {} exitoso: {} via {}", op, signature, via);
                let m = metrics.finalize(true, None);
                m.log(op, mint);
            }
            BroadcastResult::BundleSuccess { bundle_id, via } => {
                println!("✅ [EXEC] {} bundle exitoso: {} via {}", op, bundle_id, via);
                let m = metrics.finalize(true, None);
                m.log(op, mint);
            }
            BroadcastResult::Failed { error } => {
                println!("❌ [EXEC] {} falló: {}", op, error);
                let m = metrics.finalize(false, Some(error));
                m.log(op, mint);
            }
        }
    }

    /// Limpia el cache de swaps preparados
    pub fn cleanup_cache(&mut self) {
        self.swap_cache.cleanup();
    }

    /// Test de bundle: envía dummy tx + tip por Jito
    pub async fn test_jito_bundle(&self) -> Result<String> {
        let payer = self.payer.as_ref()
            .ok_or_else(|| anyhow!("No keypair loaded for bundle test"))?;

        if !self.config.jito_enabled || self.config.jito_url.is_none() {
            return Err(anyhow!("Jito not configured"));
        }

        let blockhash = self.tx_builder.get_recent_blockhash(&self.rpc_client).await?;
        let dummy_tx = self.tx_builder.build_dummy_tx(payer, blockhash)?;

        let tip_account_str = self.broadcaster.pick_tip_account().await?;
        let tip_pubkey: Pubkey = tip_account_str.parse()?;
        let tip_tx = self.tx_builder.build_tip_tx(
            payer,
            blockhash,
            &tip_pubkey,
            self.config.jito_tip_lamports,
        )?;

        println!("🧪 [TEST] Enviando bundle de prueba...");
        let bundle_id = self.broadcaster.send_bundle_base64(&[dummy_tx, tip_tx]).await?;
        println!("✅ [TEST] Bundle ID: {}", bundle_id);

        Ok(bundle_id)
    }
}
