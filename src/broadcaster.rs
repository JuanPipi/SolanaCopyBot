#![allow(dead_code)]

use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::transaction::Transaction;
use solana_sdk::signature::Signature;

use reqwest::Client;
use serde_json::json;
use base64::{engine::general_purpose, Engine as _};
use rand::seq::SliceRandom;

pub struct BroadcastConfig {
    pub jito_enabled: bool,
    pub jito_tip_lamports: u64,
    pub rpc_url: String,
    pub jito_url: Option<String>,
    pub jito_auth: Option<String>,
}

pub struct Broadcaster {
    config: BroadcastConfig,
    rpc_client: RpcClient,
    http: Client,
}

#[derive(Debug)]
pub enum BroadcastResult {
    Success { signature: Signature, via: String },
    BundleSuccess { bundle_id: String, via: String },
    Failed { error: String },
}

impl Broadcaster {
    pub fn new(config: BroadcastConfig) -> Self {
        let rpc_client = RpcClient::new(config.rpc_url.clone());
        let http = Client::new();
        Self { config, rpc_client, http }
    }

    fn jito_headers(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(uuid) = &self.config.jito_auth {
            if !uuid.is_empty() {
                return req.header("x-jito-auth", uuid);
            }
        }
        req
    }

    fn jito_url(&self, path: &str) -> Result<String> {
        let base = self.config.jito_url.clone()
            .ok_or_else(|| anyhow!("Jito URL missing"))?;
        Ok(format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/')))
    }

    /// GET tip accounts (JSON-RPC method getTipAccounts)
    pub async fn get_tip_accounts(&self) -> Result<Vec<String>> {
        let url = self.jito_url("/api/v1/bundles")?;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTipAccounts",
            "params": []
        });

        let req = self.http.post(&url).json(&body);
        let req = self.jito_headers(req);

        let resp = req.send().await
            .map_err(|e| anyhow!("getTipAccounts request failed: {}", e))?;
        
        let v: serde_json::Value = resp.json().await
            .map_err(|e| anyhow!("getTipAccounts parse failed: {}", e))?;
        
        if let Some(err) = v.get("error") {
            return Err(anyhow!("getTipAccounts error: {}", err));
        }

        let arr = v.get("result")
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("getTipAccounts: bad response: {}", v))?;

        Ok(arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
    }

    /// sendBundle (base64 encoding - recommended by Jito)
    pub async fn send_bundle_base64(&self, txs: &[Transaction]) -> Result<String> {
        if txs.is_empty() || txs.len() > 5 {
            return Err(anyhow!("Bundle must have 1..=5 txs, got {}", txs.len()));
        }

        let url = self.jito_url("/api/v1/bundles")?;

        let mut encoded: Vec<String> = Vec::with_capacity(txs.len());
        for tx in txs {
            let bytes = bincode::serialize(tx)
                .map_err(|e| anyhow!("Failed to serialize tx: {}", e))?;
            let b64 = general_purpose::STANDARD.encode(bytes);
            encoded.push(b64);
        }

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                encoded,
                { "encoding": "base64" }
            ]
        });

        let req = self.http.post(&url).json(&body);
        let req = self.jito_headers(req);

        let resp = req.send().await
            .map_err(|e| anyhow!("sendBundle request failed: {}", e))?;
        
        let v: serde_json::Value = resp.json().await
            .map_err(|e| anyhow!("sendBundle parse failed: {}", e))?;

        if let Some(err) = v.get("error") {
            return Err(anyhow!("sendBundle error: {}", err));
        }

        let bundle_id = v.get("result")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("sendBundle: no bundle_id in response: {}", v))?;

        Ok(bundle_id.to_string())
    }

    /// getBundleStatuses(bundle_ids[])
    pub async fn get_bundle_statuses(&self, bundle_ids: &[String]) -> Result<serde_json::Value> {
        let url = self.jito_url("/api/v1/bundles")?;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [ bundle_ids ]
        });

        let req = self.http.post(&url).json(&body);
        let req = self.jito_headers(req);

        let resp = req.send().await
            .map_err(|e| anyhow!("getBundleStatuses request failed: {}", e))?;
        
        let v: serde_json::Value = resp.json().await
            .map_err(|e| anyhow!("getBundleStatuses parse failed: {}", e))?;
        
        Ok(v)
    }

    /// Poll bundle status con backoff exponencial hasta "Landed" o timeout
    /// Retorna: (landed: bool, status_str: String, elapsed_ms: u64)
    pub async fn poll_bundle_until_landed(
        &self,
        bundle_id: &str,
        timeout_ms: u64,
    ) -> (bool, String, u64) {
        use std::time::Instant;
        use tokio::time::{sleep, Duration};

        let start = Instant::now();
        let mut delay_ms: u64 = 500; // Start at 500ms
        let max_delay_ms: u64 = 4000; // Cap at 4s
        let mut last_status = "unknown".to_string();
        let mut attempts = 0;

        loop {
            let elapsed = start.elapsed().as_millis() as u64;
            
            // Timeout check
            if elapsed >= timeout_ms {
                return (false, format!("timeout after {}ms (last: {})", elapsed, last_status), elapsed);
            }

            attempts += 1;
            
            // Poll status (NO re-send!)
            match self.get_bundle_statuses(&[bundle_id.to_string()]).await {
                Ok(v) => {
                    // Check for rate limit error
                    if let Some(err) = v.get("error") {
                        let err_str = err.to_string();
                        if err_str.contains("rate limited") || err_str.contains("-32097") {
                            // Rate limited - backoff silently
                            last_status = "rate_limited".to_string();
                        } else {
                            last_status = format!("error: {}", err_str);
                        }
                    } else if let Some(result) = v.get("result") {
                        // Parse the status from result
                        if let Some(value) = result.get("value") {
                            if let Some(arr) = value.as_array() {
                                if let Some(first) = arr.first() {
                                    if let Some(status) = first.get("confirmation_status").and_then(|s| s.as_str()) {
                                        last_status = status.to_string();
                                        
                                        // Check if landed
                                        if status == "confirmed" || status == "finalized" {
                                            let land_ms = start.elapsed().as_millis() as u64;
                                            return (true, status.to_string(), land_ms);
                                        }
                                    }
                                    // Also check for "Landed" in different formats
                                    if let Some(status) = first.get("status").and_then(|s| s.as_str()) {
                                        last_status = status.to_string();
                                        if status.to_lowercase().contains("landed") || status.to_lowercase().contains("confirmed") {
                                            let land_ms = start.elapsed().as_millis() as u64;
                                            return (true, status.to_string(), land_ms);
                                        }
                                    }
                                }
                            }
                        }
                        // If result exists but no clear status, check if it's empty (still processing)
                        if result.get("value").and_then(|v| v.as_array()).map(|a| a.is_empty()).unwrap_or(false) {
                            last_status = "processing".to_string();
                        }
                    }
                }
                Err(e) => {
                    last_status = format!("poll_error: {}", e);
                }
            }

            // Log progress every few attempts
            if attempts % 3 == 0 {
                println!("   ⏳ Poll #{} | status={} | elapsed={}ms", attempts, last_status, elapsed);
            }

            // Backoff exponencial
            sleep(Duration::from_millis(delay_ms)).await;
            delay_ms = std::cmp::min(delay_ms * 2, max_delay_ms);
        }
    }

    /// RPC fallback
    pub async fn send_via_rpc(&self, tx: &Transaction) -> Result<Signature> {
        let sig = self.rpc_client.send_transaction(tx).await
            .map_err(|e| anyhow!("RPC send failed: {}", e))?;
        
        println!("📤 [RPC] TX enviada: {}", sig);
        Ok(sig)
    }

    /// Bundle con fallback: intenta Jito bundle y si falla usa RPC
    pub async fn send_bundle_with_fallback(
        &self,
        swap_tx: &Transaction,
        tip_tx: &Transaction,
    ) -> BroadcastResult {
        if self.config.jito_enabled && self.config.jito_url.is_some() {
            match self.send_bundle_base64(&[swap_tx.clone(), tip_tx.clone()]).await {
                Ok(bundle_id) => {
                    println!("📦 [JITO] Bundle enviado: {}", bundle_id);
                    return BroadcastResult::BundleSuccess { 
                        bundle_id, 
                        via: "jito_bundle".into() 
                    };
                }
                Err(e) => {
                    println!("⚠️ [BROADCAST] Jito bundle falló: {}, fallback RPC…", e);
                }
            }
        }

        // Fallback: solo mandamos el swap_tx por RPC (sin tip)
        match self.send_via_rpc(swap_tx).await {
            Ok(sig) => BroadcastResult::Success { 
                signature: sig, 
                via: "rpc".into() 
            },
            Err(e) => BroadcastResult::Failed { 
                error: format!("{}", e) 
            },
        }
    }

    /// Envía con fallback simple (sin bundle)
    pub async fn send_with_fallback(&self, tx: &Transaction) -> BroadcastResult {
        match self.send_via_rpc(tx).await {
            Ok(sig) => BroadcastResult::Success {
                signature: sig,
                via: "rpc".to_string(),
            },
            Err(e) => BroadcastResult::Failed {
                error: e.to_string(),
            },
        }
    }

    /// Elige una tip account al azar (reduce contención)
    pub async fn pick_tip_account(&self) -> Result<String> {
        let tips = self.get_tip_accounts().await?;
        if tips.is_empty() {
            return Err(anyhow!("No tip accounts available"));
        }
        let mut rng = rand::thread_rng();
        tips.choose(&mut rng)
            .cloned()
            .ok_or_else(|| anyhow!("Failed to pick tip account"))
    }

    /// Confirma que la transacción llegó a la cadena.
///
/// IMPORTANTE: `send_transaction` (RPC) devuelve rápido, pero el *commitment* tarda.
/// Esta función hace polling con backoff corto para medir bien el tiempo de confirmación.
pub async fn confirm_transaction(&self, sig: &Signature) -> Result<bool> {
    use tokio::time::{sleep, Duration, Instant};
    use solana_client::rpc_response::RpcSignatureResult;
    use solana_client::rpc_config::RpcSignatureStatusConfig;

    let start = Instant::now();
    let timeout = Duration::from_secs(20); // suficiente para mainnet sin quedar colgado
    let mut delay = Duration::from_millis(250);

    loop {
        // get_signature_statuses devuelve Option en el mismo orden
        let statuses = self.rpc_client
            .get_signature_statuses_with_config(
                &[*sig],
                RpcSignatureStatusConfig { search_transaction_history: true }
            )
            .await
            .map_err(|e| anyhow!("Confirm poll failed: {}", e))?;

        let st = statuses.value.get(0).cloned().flatten();

        if let Some(st) = st {
            if let Some(err) = st.err {
                return Err(anyhow!("Tx failed: {:?}", err));
            }
            // Consideramos confirmada si tiene confirmation_status (processed/confirmed/finalized)
            // Para copy-trading, "confirmed" suele ser suficiente.
            if st.confirmation_status.is_some() {
                return Ok(true);
            }
        }

        if start.elapsed() >= timeout {
            return Ok(false);
        }

        sleep(delay).await;
        // Backoff suave, cap 1s
        delay = std::cmp::min(delay * 2, Duration::from_secs(1));
    }
}

