//! Inferencia de fees del líder (priority fee, Jito tip) para guía de configuración

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::UiTransactionEncoding;

const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

#[derive(Debug, Default)]
pub struct LeaderFeeInfo {
    pub cu_price_micro_lamports: Option<u64>,
    pub cu_limit: Option<u32>,
    pub jito_tip_lamports: Option<u64>,
    pub slot: Option<u64>,
}

/// Obtiene la tx del líder e infiere fees (en background, no bloquea)
pub fn fetch_and_log_leader_fees_spawn(
    rpc: std::sync::Arc<RpcClient>,
    sig: String,
    our_cu_price_micro_lamports: u64,
) {
    tokio::spawn(async move {
        if let Ok(info) = infer_leader_fees(&rpc, &sig).await {
            log_leader_fee_guidance(&info, our_cu_price_micro_lamports, &sig);
        }
    });
}

async fn infer_leader_fees(rpc: &RpcClient, sig: &str) -> anyhow::Result<LeaderFeeInfo> {
    let sig_parsed: solana_sdk::signature::Signature = sig.parse()?;
    let cfg = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::JsonParsed),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let tx = rpc.get_transaction_with_config(&sig_parsed, cfg).await?;
    let mut info = LeaderFeeInfo::default();
    info.slot = Some(tx.slot);

    let tx_trans = &tx.transaction.transaction;
    let instructions = match tx_trans {
        solana_transaction_status::EncodedTransaction::Json(ui) => {
            match &ui.message {
                solana_transaction_status::UiMessage::Parsed(p) => &p.instructions,
                solana_transaction_status::UiMessage::Raw(_) => return Ok(info),
            }
        }
        _ => return Ok(info),
    };

    let instructions_json = match serde_json::to_value(instructions) {
        Ok(v) => v,
        Err(_) => return Ok(info),
    };
    let arr = match instructions_json.as_array() {
        Some(a) => a,
        None => return Ok(info),
    };
    for ix in arr {
        let parsed = ix.get("parsed");
        if let Some(serde_json::Value::Object(m)) = parsed {
                let t = m.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if t == "setComputeUnitPrice" {
                    if let Some(info_val) = m.get("info") {
                        let lamports = info_val.get("microLamports")
                            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
                        if let Some(v) = lamports {
                            info.cu_price_micro_lamports = Some(v);
                        }
                    }
                } else if t == "setComputeUnitLimit" {
                    if let Some(info_val) = m.get("info") {
                        let units = info_val.get("units")
                            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
                        if let Some(v) = units {
                            info.cu_limit = Some(v as u32);
                        }
                    }
                }
            }
    }

    // Jito tip: buscar en inner_instructions transferencias a tip accounts
    if let Some(meta) = &tx.transaction.meta {
        let inner_opt: Option<Vec<_>> = meta.inner_instructions.clone().into();
        if let Some(inner_slice) = inner_opt {
            let tip_set: std::collections::HashSet<&str> = JITO_TIP_ACCOUNTS.iter().copied().collect();
            for inner_ix in &inner_slice {
                for ix in &inner_ix.instructions {
                    if let Ok(ix_json) = serde_json::to_value(ix) {
                        if let Some(parsed) = ix_json.get("parsed") {
                            if let Some(m) = parsed.as_object() {
                                if m.get("type").and_then(|t| t.as_str()) == Some("transfer") {
                                    if let Some(info_val) = m.get("info") {
                                        if let Some(dest) = info_val.get("destination").and_then(|d| d.as_str()) {
                                            if tip_set.contains(dest) {
                                                if let Some(lamports) = info_val
                                                    .get("lamports")
                                                    .and_then(|l| l.as_u64().or_else(|| l.as_str().and_then(|s| s.parse().ok())))
                                                {
                                                    info.jito_tip_lamports =
                                                        Some(info.jito_tip_lamports.unwrap_or(0) + lamports);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(info)
}

fn log_leader_fee_guidance(info: &LeaderFeeInfo, ours: u64, sig: &str) {
    let verbose = std::env::var("DEBUG").map(|v| !v.is_empty()).unwrap_or(false);
    if info.cu_price_micro_lamports.is_none() && info.jito_tip_lamports.is_none() && !verbose {
        return;
    }
    let cu = info.cu_price_micro_lamports
        .map(|v| format!("{}", v))
        .unwrap_or_else(|| "unknown".to_string());
    let limit = info.cu_limit
        .map(|v| format!("{}", v))
        .unwrap_or_else(|| "unknown".to_string());
    let jito = info.jito_tip_lamports
        .map(|v| format!("{}", v))
        .unwrap_or_else(|| "unknown".to_string());
    let slot = info.slot
        .map(|s| format!("slot={}", s))
        .unwrap_or_default();

    println!(
        "[FEE] leader sig={} {} cu_price={} cu_limit={} jito_tip={} ours={}",
        &sig[..sig.len().min(12)],
        slot,
        cu,
        limit,
        jito,
        ours
    );
    if let Some(leader_cu) = info.cu_price_micro_lamports {
        if leader_cu > ours {
            println!("[FEE] leader_cu_price={} > ours={} -> consider increasing", leader_cu, ours);
        }
    }
}
