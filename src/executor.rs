#![allow(dead_code)]
#![allow(unused_imports)]

use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;

use crate::broadcaster::{BroadcastConfig, BroadcastResult, Broadcaster};
use crate::engine::Action;
use crate::metrics::MetricsTracker;
use crate::prepared::{PreparedSwapCache, SwapPreparer};
use crate::tx_builder::{SwapInstructionBuilder, TxBuilder, TxBuilderConfig};

pub struct ExecutorConfig {
    pub rpc_url: String,
    pub dry_run: bool, // Si true, no envía transacciones reales
    pub jito_enabled: bool,
    pub jito_url: Option<String>,
    pub jito_tip_lamports: u64,
    pub compute_units: u32,
    pub priority_fee_micro_lamports: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            rpc_url: String::new(),
            dry_run: true,
            jito_enabled: false,
            jito_url: None,
            jito_tip_lamports: 10_000,
            compute_units: 200_000,
            priority_fee_micro_lamports: 1_000,
        }
    }
}

pub struct Executor {
    config: ExecutorConfig,
    rpc_client: RpcClient,
    broadcaster: Broadcaster,
    tx_builder: TxBuilder,
    swap_cache: PreparedSwapCache,
    swap_preparer: SwapPreparer,
    // TODO: Agregar wallet keypair cuando se implemente ejecución real
}

impl Executor {
    pub fn new(config: ExecutorConfig) -> Self {
        let rpc_client = RpcClient::new(config.rpc_url.clone());

        let broadcaster = Broadcaster::new(BroadcastConfig {
            jito_enabled: config.jito_enabled,
            jito_tip_lamports: config.jito_tip_lamports,
            rpc_url: config.rpc_url.clone(),
            jito_url: config.jito_url.clone(),
        });

        let tx_builder = TxBuilder::new(TxBuilderConfig {
            compute_units: config.compute_units,
            priority_fee_micro_lamports: config.priority_fee_micro_lamports,
        });

        let swap_cache = PreparedSwapCache::new(300); // 5 min TTL
        let swap_preparer = SwapPreparer::new();

        Self {
            config,
            rpc_client,
            broadcaster,
            tx_builder,
            swap_cache,
            swap_preparer,
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
                // Skip ya se logueó en el engine, no duplicar
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

        // 1. Detect ya pasó (en el engine)
        metrics.mark_detect_done();

        // 2. Preparar swap (buscar pool, accounts, etc.)
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

        // 3. Build TX
        if self.config.dry_run {
            println!("🏜️ [DRY_RUN] BUY simulado | mint={} | sol={:.6} | reason={}", mint, sol_amount, reason);
            metrics.mark_build_done();
            metrics.mark_send_done();
            let m = metrics.finalize(true, None);
            m.log("BUY (dry)", mint);
            return Ok(());
        }

        // TODO: Implementar ejecución real cuando tengamos keypair
        // let blockhash = self.tx_builder.get_recent_blockhash(&self.rpc_client).await?;
        // let lamports = (sol_amount * 1_000_000_000.0) as u64;
        // let ixs = SwapInstructionBuilder::build_buy_instruction(&payer.pubkey(), &mint_pubkey, lamports);
        // let tx = self.tx_builder.build_transaction(&payer, ixs, blockhash)?;
        // metrics.mark_build_done();
        // 
        // let result = self.broadcaster.send_with_fallback(&tx).await;
        // metrics.mark_send_done();

        println!("⚠️ [EXEC] Ejecución real no implementada aún");
        metrics.mark_build_done();
        metrics.mark_send_done();
        let m = metrics.finalize(false, Some("Not implemented".to_string()));
        m.log("BUY", mint);

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

        // TODO: Implementar ejecución real
        // 1. Buscar balance del mint en tu wallet (ATA)
        // 2. Vender TODO el balance (sell_all)
        println!("⚠️ [EXEC] Ejecución real no implementada aún");
        metrics.mark_build_done();
        metrics.mark_send_done();
        let m = metrics.finalize(false, Some("Not implemented".to_string()));
        m.log("SELL", mint);

        Ok(())
    }

    /// Limpia el cache de swaps preparados
    pub fn cleanup_cache(&mut self) {
        self.swap_cache.cleanup();
    }
}
