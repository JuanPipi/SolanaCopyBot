#![allow(dead_code)]

use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::transaction::Transaction;
use solana_sdk::signature::Signature;

pub struct BroadcastConfig {
    pub jito_enabled: bool,
    pub jito_tip_lamports: u64,
    pub rpc_url: String,
    pub jito_url: Option<String>,
}

pub struct Broadcaster {
    config: BroadcastConfig,
    rpc_client: RpcClient,
}

#[derive(Debug)]
pub enum BroadcastResult {
    Success { signature: Signature, via: String },
    Failed { error: String },
}

impl Broadcaster {
    pub fn new(config: BroadcastConfig) -> Self {
        let rpc_client = RpcClient::new(config.rpc_url.clone());
        Self { config, rpc_client }
    }

    /// Envía transacción con fallback: Jito -> RPC
    pub async fn send_with_fallback(&self, tx: &Transaction) -> BroadcastResult {
        // Intentar Jito primero si está habilitado
        if self.config.jito_enabled {
            if let Some(ref _jito_url) = self.config.jito_url {
                match self.send_via_jito(tx).await {
                    Ok(sig) => {
                        return BroadcastResult::Success {
                            signature: sig,
                            via: "jito".to_string(),
                        };
                    }
                    Err(e) => {
                        println!("⚠️ [BROADCAST] Jito falló: {}, intentando RPC...", e);
                    }
                }
            }
        }

        // Fallback a RPC normal
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

    /// Envía por RPC estándar
    pub async fn send_via_rpc(&self, tx: &Transaction) -> Result<Signature> {
        let sig = self
            .rpc_client
            .send_transaction(tx)
            .await
            .map_err(|e| anyhow!("RPC send failed: {}", e))?;

        println!("📤 [RPC] TX enviada: {}", sig);
        Ok(sig)
    }

    /// Envía por Jito bundle (placeholder - requiere implementación real)
    pub async fn send_via_jito(&self, _tx: &Transaction) -> Result<Signature> {
        // TODO: Implementar envío real a Jito
        // Esto requiere:
        // 1. Conectar al endpoint de Jito (block-engine)
        // 2. Crear bundle con tip
        // 3. Enviar y esperar confirmación
        //
        // Formato aproximado:
        // POST /api/v1/bundles
        // {
        //   "jsonrpc": "2.0",
        //   "id": 1,
        //   "method": "sendBundle",
        //   "params": [[serialized_tx_base64]]
        // }

        Err(anyhow!("Jito not implemented yet"))
    }

    /// Confirma que la transacción llegó a la cadena
    pub async fn confirm_transaction(&self, sig: &Signature) -> Result<bool> {
        // Esperamos hasta 30 segundos por confirmación
        let confirmed = self
            .rpc_client
            .confirm_transaction(sig)
            .await
            .map_err(|e| anyhow!("Confirm failed: {}", e))?;

        Ok(confirmed)
    }
}
