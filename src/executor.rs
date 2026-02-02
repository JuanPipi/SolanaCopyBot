#![allow(dead_code)]
#![allow(unused_imports)]

use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer, read_keypair_file, Signature};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use tokio::time::sleep;

use crate::broadcaster::{BroadcastConfig, BroadcastResult, Broadcaster};
use crate::engine::Action;
use crate::jupiter::{JupiterClient, sol_to_lamports, mints};
use crate::metrics::MetricsTracker;
use crate::prepared::{PreparedSwapCache, SwapPreparer};
use crate::tx_builder::{SwapInstructionBuilder, TxBuilder, TxBuilderConfig};

/// Resultado de un BUY exitoso (con MI signature, no del lider)
#[derive(Debug, Clone)]
pub struct BuyExecutionResult {
    pub my_sig: String,
    pub my_token_balance: u64,
    pub my_sol_spent: f64,
}

/// Resultado de un SELL exitoso
#[derive(Debug, Clone)]
pub struct SellExecutionResult {
    pub my_sig: String,
    pub tokens_sold: u64,
    pub sol_received: f64,
}

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
    pub jupiter_api_key: Option<String>,
    pub slippage_bps: u16,
    /// Reserva SOL intocable (para balance guard)
    pub reserve_sol: f64,
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
            jupiter_api_key: None,
            slippage_bps: 100, // 1% default
            reserve_sol: 0.05,
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
    jupiter: Option<JupiterClient>,
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

        // Create Jupiter client for real swaps
        let jupiter = if !config.dry_run {
            match JupiterClient::new(config.jupiter_api_key.clone(), config.slippage_bps) {
                Ok(jup) => {
                    println!("🪐 [EXEC] Jupiter client initialized (slippage={}bps)", config.slippage_bps);
                    Some(jup)
                }
                Err(e) => {
                    println!("⚠️ [EXEC] Failed to create Jupiter client: {}", e);
                    None
                }
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
            jupiter,
        }
    }

    /// Pubkey del owner (para reconciliación)
    pub fn owner_pubkey(&self) -> Option<Pubkey> {
        self.payer.as_ref().map(|p| p.pubkey())
    }

    /// Ejecuta BUY con slippage dinámico (retry si falla por 0x1771)
    pub async fn execute_buy(&mut self, mint: &str, sol_amount: f64) -> Result<BuyExecutionResult> {
        // DRY RUN: simular éxito
        if self.config.dry_run {
            println!("🏜️ [DRY_RUN] BUY simulado | mint={} | sol={:.6}", &mint[..8.min(mint.len())], sol_amount);
            return Ok(BuyExecutionResult {
                my_sig: format!("dry_run_{}", &mint[..8.min(mint.len())]),
                my_token_balance: 1_000_000,
                my_sol_spent: sol_amount,
            });
        }

        // Balance guard ANTES de intentar (evita insufficient lamports)
        let payer = self.payer.as_ref().ok_or_else(|| anyhow!("No keypair loaded"))?;
        let mint_pubkey: Pubkey = mint.parse().map_err(|_| anyhow!("Invalid mint: {}", mint))?;
        if let Err(e) = self.check_buy_balance(payer.pubkey(), sol_amount, &mint_pubkey).await {
            return Err(e);
        }

        // Slippage levels para retry: 300 -> 450 -> 600 -> 800 bps
        let slippage_levels = [
            self.config.slippage_bps,                    // Default (300)
            self.config.slippage_bps + 150,              // 450
            self.config.slippage_bps + 300,              // 600
            self.config.slippage_bps + 500,              // 800
        ];

        let mut last_error = anyhow!("No attempts made");

        for (attempt, &slippage) in slippage_levels.iter().enumerate() {
            println!(
                "🔄 [EXEC] BUY attempt {}/{} | mint={} | sol={:.4} | slippage={}bps",
                attempt + 1, slippage_levels.len(), &mint[..8.min(mint.len())], sol_amount, slippage
            );

            match self.try_buy_with_slippage(mint, sol_amount, slippage).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let err_str = e.to_string();
                    last_error = e;

                    // 0x1771 = slippage exceeded en pump.fun
                    // Si es error de slippage y quedan intentos, retry
                    if err_str.contains("0x1771") && attempt < slippage_levels.len() - 1 {
                        println!(
                            "   ⚡ [RETRY] Slippage exceeded, retrying con {}bps...",
                            slippage_levels[attempt + 1]
                        );
                        continue;
                    }

                    // Blockhash not found -> rebuild needed, pero no retry infinito
                    if err_str.contains("Blockhash not found") && attempt < slippage_levels.len() - 1 {
                        println!("   ⚡ [RETRY] Blockhash stale, rebuilding tx...");
                        continue;
                    }

                    // Otro error -> no retry
                    break;
                }
            }
        }

        Err(last_error)
    }

    /// Intento de BUY con un slippage específico
    async fn try_buy_with_slippage(&mut self, mint: &str, sol_amount: f64, slippage_bps: u16) -> Result<BuyExecutionResult> {
        let mut metrics = MetricsTracker::new();
        metrics.mark_detect_done();

        let payer = self.payer.as_ref()
            .ok_or_else(|| anyhow!("No keypair loaded"))?;
        let jupiter = self.jupiter.as_ref()
            .ok_or_else(|| anyhow!("Jupiter client not available"))?;

        let amount_lamports = sol_to_lamports(sol_amount);
        let mint_pubkey: Pubkey = mint.parse()
            .map_err(|_| anyhow!("Invalid mint address: {}", mint))?;

        // Validación de mint real (owner = Tokenkeg o TokenzQd)
        if let Err(e) = self.validate_mint(&mint_pubkey).await {
            return Err(anyhow!("Mint invalid: {}", e));
        }

        // Quote + build + send con retry si tx "too large"
        let params_list = [
            JupiterClient::default_quote_params(),
            JupiterClient::fallback_quote_params(),
        ];
        let mut last_err = anyhow!("No attempts");
        let mut signed_tx_opt = None;

        for (i, params) in params_list.iter().enumerate() {
            let label = if i == 0 { "default" } else { "fallback (onlyDirect)" };
            println!("🪐 [JUPITER] Quote: {:.4} SOL -> {} (slippage={}bps) [{}]", sol_amount, &mint[..8.min(mint.len())], slippage_bps, label);
            match jupiter.quote_and_build(
                mints::WSOL,
                mint,
                amount_lamports,
                &payer.pubkey(),
                Some(slippage_bps),
                Some(50_000),
                Some(params),
            ).await {
                Ok(swap_result) => {
                    if let Some(out) = swap_result.quote.out_amount() {
                        println!("   ✓ Quote: {} lamports -> {} tokens", amount_lamports, out);
                    }
                    signed_tx_opt = Some(jupiter.sign_swap_tx(swap_result, payer)
                        .map_err(|e| anyhow!("Failed to sign tx: {}", e))?);
                    break;
                }
                Err(e) => {
                    last_err = e;
                    if i < params_list.len() - 1 {
                        println!("   ⚡ [RETRY] Quote falló, intentando rutas más simples...");
                    }
                }
            }
        }

        let signed_tx = signed_tx_opt.ok_or_else(|| last_err)?;
        metrics.mark_build_done();
        
        // Send (rápido) y confirm (medible) con fallback si la tx es demasiado grande
println!("📤 [EXEC] Sending swap transaction...");
let sig = match self.rpc_client.send_transaction(&signed_tx).await {
    Ok(s) => s,
    Err(e) => {
        let err_str = e.to_string();
        let is_too_large = err_str.contains("too large") || err_str.contains("1688") || err_str.contains("1644") || err_str.contains("1232");
        if is_too_large {
            println!("   ⚡ [RETRY] Tx too large, requoting con onlyDirectRoutes...");
            let fallback = JupiterClient::fallback_quote_params();
            let swap_result = jupiter.quote_and_build(
                mints::WSOL,
                mint,
                amount_lamports,
                &payer.pubkey(),
                Some(slippage_bps),
                Some(50_000),
                Some(&fallback),
            ).await.map_err(|e| anyhow!("Requote failed: {}", e))?;
            let retry_tx = jupiter.sign_swap_tx(swap_result, payer)
                .map_err(|e| anyhow!("Failed to sign: {}", e))?;
            self.rpc_client.send_transaction(&retry_tx).await
                .map_err(|e| anyhow!("Send failed (after requote): {}", e))?
        } else {
            // Si falla, simular para obtener logs completos
            if let Ok(sim) = self.rpc_client.simulate_transaction(&signed_tx).await {
                if let Some(logs) = sim.value.logs {
                    eprintln!("   [SIM_LOGS] {} mensajes:", logs.len());
                    for (i, line) in logs.iter().enumerate() {
                        eprintln!("      [{}] {}", i + 1, line);
                    }
                }
            }
            return Err(anyhow!("Send failed: {}", e));
        }
    }
};

metrics.mark_send_done();

// Confirmación real on-chain (esto hace que confirm_ms sea real)
println!("⏳ [EXEC] Esperando confirmación on-chain...");
match self.broadcaster.confirm_transaction(&sig).await {
    Ok(true) => {}
    Ok(false) => return Err(anyhow!("Tx not confirmed within timeout (sig={})", sig)),
    Err(e) => return Err(anyhow!("Confirm failed: {} (sig={})", e, sig)),
}
metrics.mark_confirm_done();

// CRITICAL: Verificar tokens recibidos
        // 1) Primero por postTokenBalances de la tx (ultra robusto, no depende del indexado)
        println!("⏳ [EXEC] Verificando balance...");
        sleep(Duration::from_millis(300)).await;

        let token_balance = match self.verify_tokens_from_tx_with_retry(&sig, &payer.pubkey(), mint, 8).await? {
            Some(amt) => {
                if amt > 0 {
                    println!("   ✓ [TX] postTokenBalances confirma {} tokens", amt);
                    amt
                } else {
                    // confirm_ms ya mide confirmación on-chain
            // (skip mark_confirm_done)
            
                    let m = metrics.finalize(false, Some("No tokens in postTokenBalances".to_string()));
                    m.log("BUY", mint);
                    return Err(anyhow!("BUY tx confirmada pero postTokenBalances=0 (sig={})", sig));
                }
            }
            None => {
                // 2) Fallback: ATA con retry (tx puede tardar en indexarse)
                println!("   [EXEC] postTokenBalances no disponible, usando get_token_balance...");
                self.get_token_balance_with_retry(&payer.pubkey(), &mint_pubkey, 8).await?
            }
        };

        if token_balance == 0 {
            // Log diagnóstico para distinguir index delay vs swap fallido vs mint raro
            if let Ok(token_program) = self.get_mint_token_program_id(&mint_pubkey).await {
                let ata = get_associated_token_address_with_program_id(&payer.pubkey(), &mint_pubkey, &token_program);
                eprintln!("   [DIAG] balance=0 | mint={} | token_program={} | ata={}", 
                    &mint[..8.min(mint.len())], token_program, ata);
            }
            // confirm_ms ya mide confirmación on-chain
            // (skip mark_confirm_done)
            
            let m = metrics.finalize(false, Some("No tokens received".to_string()));
            m.log("BUY", mint);
            return Err(anyhow!("BUY tx confirmed but no tokens received (sig={})", sig));
        }

        // confirm_ms ya mide confirmación on-chain
            // (skip mark_confirm_done)
            
        
        println!("✅ [EXEC] BUY VERIFIED | sig={} | balance={} tokens", sig, token_balance);
        let m = metrics.finalize(true, None);
        m.log("BUY (jupiter)", mint);
        
        Ok(BuyExecutionResult {
            my_sig: sig.to_string(),
            my_token_balance: token_balance,
            my_sol_spent: sol_amount,
        })
    }

    /// Ejecuta SELL con retry por slippage (0x1771) igual que BUY
    pub async fn execute_sell(&mut self, mint: &str, reason: &str) -> Result<SellExecutionResult> {
        let mut metrics = MetricsTracker::new();

        println!(
            "🔄 [EXEC] Iniciando SELL | mint={} | reason={} | dry_run={}",
            &mint[..8.min(mint.len())], reason, self.config.dry_run
        );

        metrics.mark_detect_done();

        // DRY RUN
        if self.config.dry_run {
            println!("🏜️ [DRY_RUN] SELL simulado | mint={}", &mint[..8.min(mint.len())]);
            metrics.mark_build_done();
            metrics.mark_send_done();
            metrics.mark_confirm_done();
            let m = metrics.finalize(true, None);
            m.log(&format!("SELL (dry, {})", reason), mint);
            return Ok(SellExecutionResult {
                my_sig: format!("dry_run_sell_{}", &mint[..8.min(mint.len())]),
                tokens_sold: 1_000_000,
                sol_received: 0.01,
            });
        }

        // Slippage levels para retry (igual que BUY): 300 -> 450 -> 600 -> 800 bps
        let slippage_levels = [
            self.config.slippage_bps,
            self.config.slippage_bps + 150,
            self.config.slippage_bps + 300,
            self.config.slippage_bps + 500,  // 800 bps
        ];

        let mut last_error = anyhow!("No attempts made");
        for (attempt, &slippage) in slippage_levels.iter().enumerate() {
            println!(
                "🔄 [EXEC] SELL attempt {}/{} | mint={} | slippage={}bps",
                attempt + 1, slippage_levels.len(), &mint[..8.min(mint.len())], slippage
            );
            match self.try_sell_with_slippage(mint, reason, slippage).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let err_str = e.to_string();
                    last_error = e;
                    // No retry para errores que slippage no arregla
                    if err_str.contains("0x2") || err_str.contains("Invalid Mint") || err_str.contains("insufficient") || err_str.contains("balance=0") {
                        break;
                    }
                    if err_str.contains("0x1771") && attempt < slippage_levels.len() - 1 {
                        let next_slippage = slippage_levels[attempt + 1];
                        println!("   ⚡ [RETRY] sell slippage -> {}bps...", next_slippage);
                        continue;
                    }
                    if err_str.contains("Blockhash not found") && attempt < slippage_levels.len() - 1 {
                        println!("   ⚡ [RETRY] Blockhash stale, requoting...");
                        continue;
                    }
                    break;
                }
            }
        }
        Err(last_error)
    }

    /// Intento de SELL con slippage específico
    async fn try_sell_with_slippage(&mut self, mint: &str, reason: &str, slippage_bps: u16) -> Result<SellExecutionResult> {
        let mut metrics = MetricsTracker::new();
        metrics.mark_detect_done();

        let payer = self.payer.as_ref()
            .ok_or_else(|| anyhow!("No keypair loaded"))?;
        let jupiter = self.jupiter.as_ref()
            .ok_or_else(|| anyhow!("Jupiter client not available"))?;

        let mint_pubkey: Pubkey = mint.parse()
            .map_err(|_| anyhow!("Invalid mint address: {}", mint))?;
        
        let token_balance = self.get_token_balance_with_retry(&payer.pubkey(), &mint_pubkey, 8).await?;

        if token_balance == 0 {
            return Err(anyhow!("No tokens to sell (balance=0)"));
        }

        let quote_params = JupiterClient::default_quote_params();
        println!("🪐 [JUPITER] Quote: {} tokens -> SOL (slippage={}bps)", token_balance, slippage_bps);
        let swap_result = jupiter.quote_and_build(
            mint,
            mints::WSOL,
            token_balance,
            &payer.pubkey(),
            Some(slippage_bps),
            Some(50_000),
            Some(&quote_params),
        ).await.map_err(|e| anyhow!("Jupiter quote failed: {}", e))?;

        let expected_sol = swap_result.quote.out_amount()
            .map(|out| out as f64 / 1_000_000_000.0)
            .unwrap_or(0.0);
        
        if expected_sol > 0.0 {
            println!("   ✓ Quote: {} tokens -> ~{:.6} SOL", token_balance, expected_sol);
        }

        let signed_tx = jupiter.sign_swap_tx(swap_result, payer)
            .map_err(|e| anyhow!("Failed to sign tx: {}", e))?;

        metrics.mark_build_done();
        
        println!("📤 [EXEC] Sending sell transaction...");
let sig = match self.rpc_client.send_transaction(&signed_tx).await {
    Ok(s) => s,
    Err(e) => {
        let err_str = e.to_string();
        let is_too_large = err_str.contains("too large") || err_str.contains("1688") || err_str.contains("1644") || err_str.contains("1232");
        if is_too_large {
            println!("   ⚡ [RETRY] Tx too large, requoting con onlyDirectRoutes...");
            let fallback = JupiterClient::fallback_quote_params();
            let swap_result = jupiter.quote_and_build(
                mint,
                mints::WSOL,
                token_balance,
                &payer.pubkey(),
                Some(slippage_bps),
                Some(50_000),
                Some(&fallback),
            ).await.map_err(|e| anyhow!("Requote failed: {}", e))?;
            let retry_tx = jupiter.sign_swap_tx(swap_result, payer)
                .map_err(|e| anyhow!("Failed to sign: {}", e))?;
            self.rpc_client.send_transaction(&retry_tx).await
                .map_err(|e| anyhow!("Send failed (after requote): {}", e))?
        } else {
            return Err(anyhow!("Send failed: {}", e));
        }
    }
};

metrics.mark_send_done();

// Confirmación real on-chain (confirm_ms ya no queda en 0)
println!("⏳ [EXEC] Esperando confirmación on-chain...");
match self.broadcaster.confirm_transaction(&sig).await {
    Ok(true) => {}
    Ok(false) => return Err(anyhow!("Tx not confirmed within timeout (sig={})", sig)),
    Err(e) => return Err(anyhow!("Confirm failed: {} (sig={})", e, sig)),
}
metrics.mark_confirm_done();

        
        metrics.mark_send_done();
        metrics.mark_confirm_done();
        
        println!("✅ [EXEC] SELL SUCCESS | sig={} | tokens={} | ~{:.6} SOL", sig, token_balance, expected_sol);
        let m = metrics.finalize(true, None);
        m.log(&format!("SELL jupiter ({})", reason), mint);
        
        Ok(SellExecutionResult {
            my_sig: sig.to_string(),
            tokens_sold: token_balance,
            sol_received: expected_sol,
        })
    }

    /// Verificación ultra robusta: lee postTokenBalances de la tx confirmada
    /// Con retry (getTransaction puede tardar) y commitment finalized
    async fn verify_tokens_from_tx_with_retry(
        &self,
        sig: &Signature,
        owner: &Pubkey,
        mint: &str,
        max_retries: u32,
    ) -> Result<Option<u64>> {
        let mut wait_ms = 300u64;
        let mut last_err = None::<String>;

        for attempt in 0..max_retries {
            match self.verify_tokens_from_tx_once(sig, owner, mint).await {
                Ok(Some(amt)) => return Ok(Some(amt)),
                Ok(None) => {
                    // None = tx no disponible o owner+mint no en post
                    last_err = Some("getTransaction returned null or no match".to_string());
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                }
            }

            if attempt < max_retries - 1 {
                println!("   [EXEC] getTransaction attempt {}/{} failed, retry in {}ms", attempt + 1, max_retries, wait_ms);
                sleep(Duration::from_millis(wait_ms)).await;
                wait_ms = (wait_ms + 200).min(3000); // 300->500->700->... cap 3s
            }
        }

        if let Some(ref e) = last_err {
            eprintln!("   [DIAG] getTransaction falló tras {} intentos: {}", max_retries, e);
        }
        Ok(None)
    }

    /// Una llamada a getTransaction para verificar postTokenBalances (commitment: finalized)
    async fn verify_tokens_from_tx_once(&self, sig: &Signature, owner: &Pubkey, mint: &str) -> Result<Option<u64>> {
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            commitment: Some(CommitmentConfig::finalized()), // más lento pero más real
            max_supported_transaction_version: Some(0),
        };

        let tx = match self.rpc_client.get_transaction_with_config(sig, cfg).await {
            Ok(t) => t,
            Err(e) => return Err(anyhow!("getTransaction: {}", e)),
        };

        let meta = match tx.transaction.meta {
            Some(m) => m,
            None => {
                eprintln!("   [DIAG] meta is None");
                return Ok(None);
            }
        };

        // Verificar que la tx no falló
        if meta.err.is_some() {
            eprintln!("   [DIAG] meta.err={:?}", meta.err);
            return Ok(Some(0)); // Tx falló -> 0 tokens
        }

        let post: Vec<_> = match meta.post_token_balances.into() {
            Some(p) => p,
            None => {
                eprintln!("   [DIAG] post_token_balances is empty/null");
                return Ok(None);
            }
        };

        if post.is_empty() {
            eprintln!("   [DIAG] post_token_balances vacío (swap puede haber devuelto SOL)");
        }

        let owner_str = owner.to_string();

        for b in &post {
            let bal_owner: Option<String> = b.owner.clone().into();
            if bal_owner.as_deref() != Some(owner_str.as_str()) {
                continue;
            }
            if b.mint != mint {
                continue;
            }
            let amount: u64 = b.ui_token_amount.amount.parse().unwrap_or(0);
            return Ok(Some(amount));
        }

        // Owner+mint no encontrado en post - log diagnóstico
        let logs_opt: Option<Vec<String>> = meta.log_messages.clone().into();
        if let Some(logs) = logs_opt {
            let tail: Vec<_> = logs.iter().rev().take(3).collect();
            eprintln!("   [DIAG] owner+mint no en postTokenBalances. Tx logs (last 3): {:?}", tail);
        } else {
            eprintln!("   [DIAG] owner+mint no en postTokenBalances (post len={})", post.len());
        }
        Ok(None)
    }

    /// Devuelve el token_program_id real (Tokenkeg o Token-2022) mirando el owner del mint
    async fn get_mint_token_program_id(&self, mint: &Pubkey) -> Result<Pubkey> {
        let mint_acc = self.rpc_client.get_account(mint).await?;
        Ok(mint_acc.owner)
    }

    const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

    /// Valida que el mint existe y pertenece a SPL Token o Token-2022
    async fn validate_mint(&self, mint: &Pubkey) -> Result<()> {
        let mint_acc = self.rpc_client.get_account_with_commitment(mint, CommitmentConfig::finalized())
            .await
            .map_err(|e| anyhow!("get_account mint failed: {}", e))?;
        let acc = mint_acc.value.ok_or_else(|| anyhow!("Mint account not found"))?;
        let owner_str = acc.owner.to_string();
        if owner_str != Self::TOKEN_PROGRAM && owner_str != Self::TOKEN_2022_PROGRAM {
            return Err(anyhow!("Mint owner {} not a valid token program", owner_str));
        }
        Ok(())
    }

    /// Buffer para ATA rent si el ATA no existe (~0.003 SOL)
    const ATA_RENT_BUFFER_LAMPORTS: u64 = 3_000_000;
    /// Buffer adicional para fees/tx
    const FEE_BUFFER_LAMPORTS: u64 = 500_000;

    /// Verifica balance suficiente antes de BUY (evita insufficient lamports)
    async fn check_buy_balance(&self, owner: Pubkey, sol_amount: f64, mint: &Pubkey) -> Result<()> {
        let balance = self.rpc_client.get_balance(&owner).await?;
        let trade_lamports = sol_to_lamports(sol_amount);
        let reserve_lamports = sol_to_lamports(self.config.reserve_sol);
        let fee_buffer = Self::FEE_BUFFER_LAMPORTS;

        let ata_buffer = {
            let token_program = self.get_mint_token_program_id(mint).await?;
            let ata = get_associated_token_address_with_program_id(&owner, mint, &token_program);
            match self.rpc_client.get_account_with_commitment(&ata, CommitmentConfig::confirmed()).await {
                Ok(res) if res.value.is_some() => 0,
                _ => Self::ATA_RENT_BUFFER_LAMPORTS,
            }
        };

        let required = trade_lamports + reserve_lamports + fee_buffer + ata_buffer;
        if balance < required {
            return Err(anyhow!(
                "insufficient_sol: balance={} lamports ({:.4} SOL), need {} (trade={:.4}+reserve={:.4}+buffer)",
                balance,
                balance as f64 / 1e9,
                required,
                sol_amount,
                self.config.reserve_sol
            ));
        }
        Ok(())
    }

    /// Get token balance usando ATA correcto para SPL Token o Token-2022
    async fn get_token_balance(&self, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
        // 1) Detectar token program real del mint (SPL Token o Token-2022)
        let token_program_id = self.get_mint_token_program_id(mint).await?;

        // 2) Derivar ATA correcto para ESE token program
        let ata = get_associated_token_address_with_program_id(owner, mint, &token_program_id);

        // 3) Pedir balance por RPC getTokenAccountBalance (no parsear data manualmente)
        match self.rpc_client.get_token_account_balance(&ata).await {
            Ok(bal) => {
                // bal.amount es string entero en base units (u64)
                let amount: u64 = bal.amount.parse().unwrap_or(0);
                Ok(amount)
            }
            Err(_) => Ok(0), // ATA no existe / no indexado / etc.
        }
    }

    /// Obtiene todos los token accounts del owner (SPL + Token-2022)
    pub async fn get_all_token_balances(&self, owner: &Pubkey) -> Result<std::collections::HashMap<String, u64>> {
        use reqwest::Client;
        use serde_json::Value;

        let mut result = std::collections::HashMap::new();
        let owner_str = owner.to_string();

        for program_id in &[
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  // SPL Token
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  // Token-2022
        ] {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    owner_str,
                    { "programId": program_id },
                    { "encoding": "jsonParsed" }
                ]
            });

            let client = Client::new();
            let res = client
                .post(&self.config.rpc_url)
                .json(&body)
                .send()
                .await
                .map_err(|e| anyhow!("getTokenAccountsByOwner req: {}", e))?;

            let json: Value = res.json().await.map_err(|e| anyhow!("parse response: {}", e))?;
            let value = json.get("result").and_then(|r| r.get("value"));
            let accounts = match value {
                Some(serde_json::Value::Array(arr)) => arr,
                _ => continue,
            };

            for acc in accounts {
                if let Some(info) = acc
                    .get("account")
                    .and_then(|a| a.get("data"))
                    .and_then(|d| d.get("parsed"))
                    .and_then(|p| p.get("info"))
                {
                    let mint = info.get("mint").and_then(|m| m.as_str()).unwrap_or("").to_string();
                    let amount_str = info
                        .get("tokenAmount")
                        .and_then(|t| t.get("amount"))
                        .and_then(|a| a.as_str())
                        .unwrap_or("0");
                    let amount: u64 = amount_str.parse().unwrap_or(0);
                    if !mint.is_empty() && amount > 0 {
                        result.insert(mint, amount);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get token balance con reintentos y backoff progresivo
    async fn get_token_balance_with_retry(&self, owner: &Pubkey, mint: &Pubkey, max_retries: u32) -> Result<u64> {
        let mut wait_ms = 500u64;

        for attempt in 0..max_retries {
            let balance = self.get_token_balance(owner, mint).await?;

            if balance > 0 {
                return Ok(balance);
            }

            if attempt < max_retries - 1 {
                println!("   [EXEC] Balance=0, retry {}/{} en {}ms...", attempt + 1, max_retries, wait_ms);
                sleep(Duration::from_millis(wait_ms)).await;
                wait_ms = (wait_ms * 2).min(4000); // backoff: 500 -> 1000 -> 2000 -> 4000 (cap 4s)
            }
        }

        Ok(0)
    }

    /// Maneja el resultado del broadcast con confirmación async
    async fn handle_broadcast_result_async(&self, result: BroadcastResult, metrics: &mut MetricsTracker, op: &str, mint: &str) {
        match result {
            BroadcastResult::Success { signature, via } => {
                println!("⏳ [EXEC] {} enviado, esperando confirmación: {} via {}", op, signature, via);
                match self.broadcaster.confirm_transaction(&signature).await {
                    Ok(true) => {
                        metrics.mark_confirm_done();
                        println!("✅ [EXEC] {} confirmado: {}", op, signature);
                        let m = std::mem::replace(metrics, MetricsTracker::new());
                        m.finalize(true, None).log(op, mint);
                    }
                    Ok(false) => {
                        metrics.mark_confirm_done();
                        println!("⚠️ [EXEC] {} no confirmado aún: {}", op, signature);
                        let m = std::mem::replace(metrics, MetricsTracker::new());
                        m.finalize(true, Some("not_confirmed_yet".to_string())).log(op, mint);
                    }
                    Err(e) => {
                        metrics.mark_confirm_done();
                        println!("⚠️ [EXEC] Error confirmando {}: {}", op, e);
                        let m = std::mem::replace(metrics, MetricsTracker::new());
                        m.finalize(true, Some(format!("confirm_error: {}", e))).log(op, mint);
                    }
                }
            }
            BroadcastResult::BundleSuccess { bundle_id, via } => {
                println!("✅ [EXEC] {} bundle enviado: {} via {}", op, bundle_id, via);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                metrics.mark_confirm_done();
                let m = std::mem::replace(metrics, MetricsTracker::new());
                m.finalize(true, None).log(op, mint);
            }
            BroadcastResult::Failed { error } => {
                println!("❌ [EXEC] {} falló: {}", op, error);
                let m = std::mem::replace(metrics, MetricsTracker::new());
                m.finalize(false, Some(error)).log(op, mint);
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
