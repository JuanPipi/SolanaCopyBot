//! Jupiter Swap API v6 integration
//! Docs: https://station.jup.ag/docs/apis/swap-api

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::VersionedTransaction,
};

const JUP_BASE: &str = "https://api.jup.ag";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Jupiter client para quotes y swaps
pub struct JupiterClient {
    http: reqwest::Client,
    default_slippage_bps: u16,
}

/// Respuesta del quote (guardamos el JSON completo para reenviarlo al swap)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteResponse {
    #[serde(flatten)]
    pub raw: serde_json::Value,
}

impl QuoteResponse {
    /// Extrae el output amount esperado (en atomic units)
    pub fn out_amount(&self) -> Option<u64> {
        self.raw.get("outAmount")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
    }
    
    /// Extrae el price impact en porcentaje
    pub fn price_impact_pct(&self) -> Option<f64> {
        self.raw.get("priceImpactPct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
    }
}

/// Request para el endpoint /swap
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapRequest {
    quote_response: serde_json::Value,
    user_public_key: String,
    wrap_and_unwrap_sol: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    dynamic_compute_unit_limit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prioritization_fee_lamports: Option<PriorityFee>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PriorityFee {
    priority_level_with_max_lamports: PriorityLevel,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PriorityLevel {
    max_lamports: u64,
    priority_level: String, // "medium", "high", "veryHigh"
}

/// Respuesta del swap endpoint
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwapResponse {
    swap_transaction: String, // base64 encoded VersionedTransaction
    last_valid_block_height: Option<u64>,
}

/// Resultado de construir un swap
#[derive(Debug)]
pub struct SwapResult {
    pub transaction: VersionedTransaction,
    pub last_valid_block_height: Option<u64>,
    pub quote: QuoteResponse,
}

/// Parámetros opcionales para el quote (evitar rutas raras / tx demasiado grande)
#[derive(Debug, Clone, Default)]
pub struct QuoteParams {
    pub only_direct_routes: bool,
    pub max_accounts: Option<u32>,
    pub restrict_intermediate_tokens: bool,
}

impl JupiterClient {
    /// Crear nuevo cliente Jupiter
    /// api_key es opcional (sin él funciona pero con rate limits más bajos)
    pub fn new(api_key: Option<String>, default_slippage_bps: u16) -> Result<Self> {
        let mut headers = HeaderMap::new();
        
        if let Some(key) = api_key {
            if !key.is_empty() {
                headers.insert("x-api-key", HeaderValue::from_str(&key)
                    .map_err(|e| anyhow!("Invalid API key header: {}", e))?);
            }
        }
        
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;
        
        Ok(Self {
            http,
            default_slippage_bps,
        })
    }

    /// Obtener quote para un swap
    /// amount_in: cantidad en atomic units (lamports para SOL, etc)
    pub async fn get_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        slippage_bps: Option<u16>,
        params: Option<&QuoteParams>,
    ) -> Result<QuoteResponse> {
        let slippage = slippage_bps.unwrap_or(self.default_slippage_bps);
        
        let mut url = format!(
            "{}/swap/v1/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}",
            JUP_BASE, input_mint, output_mint, amount_in, slippage
        );

        let default_params = QuoteParams::default();
        let p = params.unwrap_or(&default_params);
        if p.only_direct_routes {
            url.push_str("&onlyDirectRoutes=true");
        }
        if let Some(max) = p.max_accounts {
            url.push_str(&format!("&maxAccounts={}", max));
        }
        if p.restrict_intermediate_tokens {
            url.push_str("&restrictIntermediateTokens=true");
        }

        let resp = self.http.get(&url)
            .send()
            .await
            .map_err(|e| anyhow!("Quote request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Quote failed ({}): {}", status, body));
        }

        let raw: serde_json::Value = resp.json().await
            .map_err(|e| anyhow!("Quote parse failed: {}", e))?;

        // Check for error in response
        if let Some(error) = raw.get("error") {
            return Err(anyhow!("Jupiter quote error: {}", error));
        }

        Ok(QuoteResponse { raw })
    }

    /// Construir transacción de swap a partir de un quote
    /// Retorna la transacción lista para firmar
    pub async fn build_swap_tx(
        &self,
        quote: &QuoteResponse,
        user_pubkey: &Pubkey,
        priority_fee_lamports: Option<u64>,
    ) -> Result<SwapResult> {
        let url = format!("{}/swap/v1/swap", JUP_BASE);

        let priority_fee = priority_fee_lamports.map(|max_lamports| PriorityFee {
            priority_level_with_max_lamports: PriorityLevel {
                max_lamports,
                priority_level: "high".to_string(),
            },
        });

        let swap_req = SwapRequest {
            quote_response: quote.raw.clone(),
            user_public_key: user_pubkey.to_string(),
            wrap_and_unwrap_sol: true,
            dynamic_compute_unit_limit: Some(true),
            prioritization_fee_lamports: priority_fee,
        };

        let resp = self.http.post(&url)
            .json(&swap_req)
            .send()
            .await
            .map_err(|e| anyhow!("Swap request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Swap build failed ({}): {}", status, body));
        }

        let swap_resp: SwapResponse = resp.json().await
            .map_err(|e| anyhow!("Swap response parse failed: {}", e))?;

        // Deserializar la transacción
        let tx_bytes = general_purpose::STANDARD
            .decode(&swap_resp.swap_transaction)
            .map_err(|e| anyhow!("Failed to decode swap tx base64: {}", e))?;

        let transaction: VersionedTransaction = bincode::deserialize(&tx_bytes)
            .map_err(|e| anyhow!("Failed to deserialize swap tx: {}", e))?;

        Ok(SwapResult {
            transaction,
            last_valid_block_height: swap_resp.last_valid_block_height,
            quote: quote.clone(),
        })
    }

    /// Params por defecto: rutas más simples, evitar tx demasiado grande
    pub fn default_quote_params() -> QuoteParams {
        QuoteParams {
            only_direct_routes: false,
            max_accounts: Some(32),
            restrict_intermediate_tokens: true,
        }
    }

    /// SNIPER Strict (FAST): rutas simples y rápidas
    pub fn strict_quote_params() -> QuoteParams {
        QuoteParams {
            only_direct_routes: false,
            max_accounts: Some(16),
            restrict_intermediate_tokens: true,
        }
    }

    /// SNIPER Relaxed (FALLBACK): más rutas permitidas cuando strict falla con NO_ROUTE
    pub fn relaxed_quote_params() -> QuoteParams {
        QuoteParams {
            only_direct_routes: false,
            max_accounts: Some(32),
            restrict_intermediate_tokens: false,
        }
    }

    /// Params para retry cuando tx es "too large" (legacy SELL path)
    pub fn fallback_quote_params() -> QuoteParams {
        QuoteParams {
            only_direct_routes: true,
            max_accounts: Some(24),
            restrict_intermediate_tokens: true,
        }
    }

    /// Quote + Build en un solo paso (conveniencia)
    pub async fn quote_and_build(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        user_pubkey: &Pubkey,
        slippage_bps: Option<u16>,
        priority_fee_lamports: Option<u64>,
        params: Option<&QuoteParams>,
    ) -> Result<SwapResult> {
        let quote = self.get_quote(input_mint, output_mint, amount_in, slippage_bps, params).await?;
        self.build_swap_tx(&quote, user_pubkey, priority_fee_lamports).await
    }

    /// Firmar una transacción de swap
    pub fn sign_swap_tx(
        &self,
        mut swap_result: SwapResult,
        keypair: &Keypair,
    ) -> Result<VersionedTransaction> {
        // Para VersionedTransaction de Jupiter:
        // - Jupiter devuelve la TX parcialmente firmada (o sin firmar)
        // - Necesitamos firmar el mensaje serializado con nuestro keypair
        
        let message_bytes = swap_result.transaction.message.serialize();
        let signature = keypair.sign_message(&message_bytes);
        
        // Reemplazar la primera firma (que corresponde al fee payer / user)
        // Jupiter pone un placeholder para nuestra firma
        if swap_result.transaction.signatures.is_empty() {
            swap_result.transaction.signatures.push(signature);
        } else {
            swap_result.transaction.signatures[0] = signature;
        }

        Ok(swap_result.transaction)
    }
}

/// Helper: convierte SOL a lamports
pub fn sol_to_lamports(sol: f64) -> u64 {
    (sol * 1_000_000_000.0) as u64
}

/// Helper: convierte lamports a SOL
pub fn lamports_to_sol(lamports: u64) -> f64 {
    lamports as f64 / 1_000_000_000.0
}

/// Constantes útiles
pub mod mints {
    pub const WSOL: &str = "So11111111111111111111111111111111111111112";
    pub const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    pub const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quote() {
        let client = JupiterClient::new(None, 100).unwrap();
        let quote = client.get_quote(
            mints::WSOL,
            mints::USDC,
            10_000_000, // 0.01 SOL
            None,
            None,
        ).await;
        
        // Just check it doesn't panic
        println!("Quote result: {:?}", quote);
    }
}
