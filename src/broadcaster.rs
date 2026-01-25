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

    /// Confirma que la transacción llegó a la cadena
    pub async fn confirm_transaction(&self, sig: &Signature) -> Result<bool> {
        let confirmed = self.rpc_client.confirm_transaction(sig).await
            .map_err(|e| anyhow!("Confirm failed: {}", e))?;
        Ok(confirmed)
    }
}
