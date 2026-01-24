use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding, UiTransactionTokenBalance,
};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

use crate::signals::{Side, TradeSignal};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn token_amount(tb: &UiTransactionTokenBalance) -> f64 {
    // ui_amount puede venir None en algunos casos
    if let Some(v) = tb.ui_token_amount.ui_amount {
        return v;
    }

    // fallback: amount string / decimals
    // amount viene como string entero en unidades mínimas
    let raw: f64 = tb.ui_token_amount.amount.parse::<f64>().unwrap_or(0.0);
    let decimals = tb.ui_token_amount.decimals as i32;
    raw / 10_f64.powi(decimals)
}

fn build_map_for_owner(
    balances: &[UiTransactionTokenBalance],
    owner_wallet: &str,
) -> HashMap<String, f64> {
    let mut m = HashMap::new();
    for b in balances {
        // Solo balances del owner que estamos siguiendo
        let owner_opt: Option<String> = b.owner.clone().into();
        if let Some(owner) = owner_opt {
            if owner != owner_wallet {
                continue;
            }
        } else {
            // Si no hay owner, lo ignoramos (evita ruido)
            continue;
        }

        let mint = b.mint.clone();
        *m.entry(mint).or_insert(0.0) += token_amount(b);
    }
    m
}

fn choose_primary_delta(pre_map: &HashMap<String, f64>, post_map: &HashMap<String, f64>) -> Option<(String, f64)> {
    let mut mints: HashSet<String> = HashSet::new();
    for k in pre_map.keys() {
        mints.insert(k.clone());
    }
    for k in post_map.keys() {
        mints.insert(k.clone());
    }

    let mut best: Option<(String, f64)> = None;

    for mint in mints {
        // Filtrar WSOL (ruido típico)
        if mint == WSOL_MINT {
            continue;
        }

        let pre_amt = *pre_map.get(&mint).unwrap_or(&0.0);
        let post_amt = *post_map.get(&mint).unwrap_or(&0.0);
        let delta = post_amt - pre_amt;

        // Ignorar dust
        if delta.abs() < 0.0000001 {
            continue;
        }

        match &best {
            None => best = Some((mint, delta)),
            Some((_m, best_delta)) => {
                if delta.abs() > best_delta.abs() {
                    best = Some((mint, delta));
                }
            }
        }
    }

    best
}

/// Acorta la wallet para mostrar en logs
fn short_wallet(wallet: &str) -> &str {
    &wallet[..std::cmp::min(wallet.len(), 6)]
}

fn now_unix_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Calcula el delta de SOL (en lamports) para una wallet específica
/// Retorna Some(delta_lamports) si encuentra la wallet, None si no
fn calculate_sol_delta(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet: &str,
) -> Option<i128> {
    let meta = tx.transaction.meta.as_ref()?;
    
    // pre_balances y post_balances son Vec<u64> directamente
    let pre_balances = &meta.pre_balances;
    let post_balances = &meta.post_balances;

    if pre_balances.is_empty() || post_balances.is_empty() {
        return None;
    }

    // Necesitamos encontrar el índice de la wallet en accountKeys
    // Como account_keys puede no ser accesible directamente, usamos serde_json para acceder
    let wallet_index = match &tx.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(ui_tx) => {
            match &ui_tx.message {
                solana_transaction_status::UiMessage::Parsed(parsed) => {
                    // Serializamos account_keys a JSON y lo parseamos para acceder a los strings
                    if let Ok(keys_json) = serde_json::to_value(&parsed.account_keys) {
                        if let Some(keys_array) = keys_json.as_array() {
                            keys_array.iter().position(|k| {
                                if let Some(s) = k.as_str() {
                                    s == wallet
                                } else if let Some(obj) = k.as_object() {
                                    // Puede ser un objeto con campo "pubkey" o similar
                                    obj.values().any(|v| v.as_str() == Some(wallet))
                                } else {
                                    false
                                }
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                solana_transaction_status::UiMessage::Raw(_) => return None,
            }
        }
        _ => return None,
    }?;

    // Obtener los balances en ese índice
    let pre_balance = *pre_balances.get(wallet_index)? as i128;
    let post_balance = *post_balances.get(wallet_index)? as i128;
    
    Some(post_balance - pre_balance)
}

/// Determina si un error es recuperable (red, timeout, tx no lista)
fn is_retryable_error(msg: &str) -> bool {
    // Errores de TX no lista todavía
    let not_ready = msg.contains("invalid type: null")
        || msg.contains("not found")
        || msg.contains("-32015")
        || msg.contains("Transaction version")
        || msg.contains("maxSupportedTransactionVersion");

    // Errores de red
    let network_error = msg.contains("connection reset")
        || msg.contains("os error 10054")
        || msg.contains("os error 10053")
        || msg.contains("connection refused")
        || msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("connection closed")
        || msg.contains("broken pipe")
        || msg.contains("reset by peer")
        || msg.contains("dns error")
        || msg.contains("hyper::Error");

    not_ready || network_error
}

/// Analiza una transacción y extrae señal de trade si la hay
/// IMPORTANTE: Recibe RpcClient por referencia para no recrearlo cada vez
pub async fn analyze_transaction_async(
    client: &RpcClient,
    wallet: &str,
    signature: &str,
) -> Result<Option<TradeSignal>> {
    let signature = signature.trim(); // IMPORTANTÍSIMO
    let sig: Signature = signature.parse()?;

    // Backoff más agresivo: 300 -> 600 -> 1000 -> 1400 -> 1800 -> ...
    let delays_ms = [300u64, 600, 1000, 1400, 1800, 2200, 2600, 3000, 3000, 3000];

    // Reintentos porque logsSubscribe puede llegar antes de que getTransaction esté disponible
    for attempt in 1..=10 {
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            commitment: Some(CommitmentConfig::confirmed()), // confirmed reduce nulls significativamente
            max_supported_transaction_version: Some(0), // <- CLAVE
        };

        let res = client.get_transaction_with_config(&sig, cfg).await;

        match res {
            Ok(tx) => {
                // Calcular SOL delta ANTES de mover meta (para poder usar tx después)
                let sol_delta_lamports_opt = calculate_sol_delta(&tx, wallet);
                let sol_delta_opt =
                    sol_delta_lamports_opt.map(|lamports| lamports as f64 / 1_000_000_000.0);

                let ts = tx.block_time.unwrap_or_else(now_unix_ts);

                let meta = match tx.transaction.meta {
                    Some(m) => m,
                    None => return Ok(None),
                };

                // 1) Intento por SPL deltas
                let pre: Vec<_> = match meta.pre_token_balances.into() {
                    Some(p) => p,
                    None => vec![],
                };
                let post: Vec<_> = match meta.post_token_balances.into() {
                    Some(p) => p,
                    None => vec![],
                };

                let pre_map = build_map_for_owner(&pre, wallet);
                let post_map = build_map_for_owner(&post, wallet);

                if let Some((mint, delta)) = choose_primary_delta(&pre_map, &post_map) {
                    // Opción B: mostrar mint principal + SOL delta siempre
                    if let Some(sol_delta) = sol_delta_opt {
                        if delta > 0.0 {
                            println!("🟢 [{}] BUY principal | mint: {} | +{} tokens | SOL delta: {:.6} SOL | sig={}",
                                short_wallet(wallet), mint, delta, sol_delta, signature
                            );
                        } else {
                            println!("🔴 [{}] SELL principal | mint: {} | {} tokens | SOL delta: {:.6} SOL | sig={}",
                                short_wallet(wallet), mint, delta, sol_delta, signature
                            );
                        }
                    } else {
                        // Fallback si no pudimos calcular SOL delta
                        if delta > 0.0 {
                            println!("🟢 [{}] BUY principal | mint: {} | +{}", short_wallet(wallet), mint, delta);
                        } else {
                            println!("🔴 [{}] SELL principal | mint: {} | {}", short_wallet(wallet), mint, delta);
                        }
                    }
                    let side = if delta > 0.0 { Side::Buy } else { Side::Sell };
                    let signal = TradeSignal {
                        leader_wallet: wallet.to_string(),
                        side,
                        mint,
                        leader_sol_delta: sol_delta_opt.unwrap_or(0.0),
                        sig: signature.to_string(),
                        ts,
                    };
                    return Ok(Some(signal));
                }

                // 2) Fallback: calcular delta de SOL cuando no hay SPL principal
                if let Some(sol_delta) = sol_delta_opt {
                    // Ignorar cambios muy pequeños (probablemente solo fees)
                    if sol_delta.abs() < 0.000005 {
                        println!(
                            "ℹ️ [{}] TX sin SPL principal | SOL delta: {:.6} SOL (solo fees/operación no-swap) | sig={}",
                            short_wallet(wallet),
                            sol_delta,
                            signature
                        );
                    } else if sol_delta < 0.0 {
                        // SOL bajó = probable BUY (gastó SOL)
                        println!(
                            "🟡 [{}] TX sin SPL principal | SOL↓ {:.6} SOL (probable BUY/gasto) | sig={}",
                            short_wallet(wallet),
                            sol_delta.abs(),
                            signature
                        );
                    } else {
                        // SOL subió = probable SELL (recibió SOL)
                        println!(
                            "🟡 [{}] TX sin SPL principal | SOL↑ +{:.6} SOL (probable SELL/ingreso) | sig={}",
                            short_wallet(wallet),
                            sol_delta,
                            signature
                        );
                    }
                    return Ok(None);
                }

                // 3) Último fallback: no pudimos calcular nada
                println!(
                    "ℹ️ [{}] TX sin SPL principal ni SOL delta calculable (probable approve/wrap/fees/tx previa al swap) | sig={}",
                    short_wallet(wallet),
                    signature
                );

                return Ok(None);
            }
            Err(e) => {
                let msg = e.to_string();

                if attempt < 10 && is_retryable_error(&msg) {
                    // Solo loggear cada 3 intentos para no llenar la consola
                    if attempt % 3 == 0 {
                        println!(
                            "⏳ [{}] Retry {}/10 | sig={}...{}",
                            short_wallet(wallet),
                            attempt,
                            &signature[..8.min(signature.len())],
                            &signature[signature.len().saturating_sub(4)..]
                        );
                    }
                    let delay = delays_ms.get(attempt - 1).copied().unwrap_or(3000);
                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }

                return Err(anyhow!("get_transaction falló: {}", e));
            }
        }
    }

    Err(anyhow!("TX no disponible tras reintentos: {}", signature))
}
