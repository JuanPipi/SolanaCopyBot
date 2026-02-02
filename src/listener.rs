use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::time::{sleep, Duration, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use anyhow::{Context, Result};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

pub async fn handle_websocket_session(
    wss_url: &str,
    _rpc_http: &str,
    wallets: &[String],
    txq: mpsc::Sender<(String, String)>,
) -> Result<()> {
    let (ws_stream, _) = connect_async(wss_url)
        .await
        .context("Error conectando al WebSocket")?;

    println!("✅ Conectado al WebSocket");
    let (write, mut read) = ws_stream.split();
    let write = Arc::new(Mutex::new(write));

    // Dedupe de signatures
    let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Mapa: subscription_id -> wallet address
    let mut subid_to_wallet: HashMap<u64, String> = HashMap::new();

    // Suscribimos
    {
        let mut w = write.lock().await;
        for (id, wallet) in wallets.iter().enumerate() {
            let sub = json!({
                "jsonrpc": "2.0",
                "id": id + 1,
                "method": "logsSubscribe",
                "params": [
                    { "mentions": [wallet] },
                    { "commitment": "confirmed" }
                ]
            });

            w.send(Message::Text(sub.to_string()))
                .await
                .context("Error enviando suscripción")?;
            println!("📡 Escuchando wallet: {}", wallet);
        }
    }

    // Clonar wallets para usarlas en el mapeo
    let wallets_vec: Vec<String> = wallets.to_vec();

    // Interval para ping (25 segundos)
    let mut ping_interval = interval(Duration::from_secs(25));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Loop principal con select! para manejar ping y mensajes juntos
    loop {
        tokio::select! {
            // Tick de ping
            _ = ping_interval.tick() => {
                let mut w = write.lock().await;
                if let Err(e) = w.send(Message::Ping(vec![])).await {
                    return Err(anyhow::anyhow!("Error enviando ping: {}", e));
                }
            }

            // Mensaje del WebSocket
            msg_opt = read.next() => {
                let msg = match msg_opt {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        return Err(anyhow::anyhow!("WebSocket recv error: {}", e));
                    }
                    None => {
                        return Err(anyhow::anyhow!("WebSocket stream terminado"));
                    }
                };

                match msg {
                    Message::Text(txt) => {
                        let v: Value = match serde_json::from_str(&txt) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("⚠️ JSON inválido: {} | {}", e, txt);
                                continue;
                            }
                        };

                        // Confirmación de suscripción: guardamos el mapeo subscription_id -> wallet
                        if let Some(id_req) = v.get("id").and_then(|x| x.as_u64()) {
                            if let Some(sub_id) = v.get("result").and_then(|x| x.as_u64()) {
                                // id_req empieza en 1 y coincide con wallets[index]
                                let wallet_idx = (id_req as usize).saturating_sub(1);
                                if wallet_idx < wallets_vec.len() {
                                    let wallet = wallets_vec[wallet_idx].clone();
                                    subid_to_wallet.insert(sub_id, wallet.clone());
                                    println!("✅ Suscripción confirmada: ID {} -> {}", sub_id, &wallet[..std::cmp::min(wallet.len(), 8)]);
                                }
                                continue;
                            }
                        }

                        // Error server
                        if let Some(error) = v.get("error") {
                            eprintln!("❌ Error del servidor: {}", error);
                            continue;
                        }

                        // Notificación de logs: extraemos subscription + signature
                        if let (Some(sub_id), Some(sig)) = (
                            v.get("params").and_then(|p| p.get("subscription")).and_then(|s| s.as_u64()),
                            v.get("params")
                                .and_then(|p| p.get("result"))
                                .and_then(|r| r.get("value"))
                                .and_then(|val| val.get("signature"))
                                .and_then(|s| s.as_str()),
                        ) {
                            let sig = sig.trim().to_string();
                            let wallet = subid_to_wallet.get(&sub_id).cloned().unwrap_or_else(|| "UNKNOWN".to_string());

                            // dedupe
                            {
                                let mut s = seen.lock().await;
                                if !s.insert(sig.clone()) {
                                    continue;
                                }
                                // limpieza básica para no crecer infinito
                                if s.len() > 5000 {
                                    s.clear();
                                }
                            }

                            println!("🧾 [RAW] TX: {} | wallet={}", sig, &wallet[..std::cmp::min(wallet.len(), 6)]);
                            // Enviamos con tag |WS para identificar origen
                            let tagged = format!("{}|WS", wallet);
                            let _ = txq.send((tagged, sig)).await;
                        }
                    }
                    Message::Ping(data) => {
                        // responder
                        let mut w = write.lock().await;
                        let _ = w.send(Message::Pong(data)).await;
                    }
                    Message::Pong(_) => {
                        // Respuesta a nuestro ping, todo OK
                    }
                    Message::Close(frame) => {
                        println!("🔌 Conexión cerrada: {:?}", frame);
                        return Err(anyhow::anyhow!("Conexión cerrada por servidor"));
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Función principal con reconexión automática permanente
pub async fn listen_wallets(
    wss_url: &str,
    rpc_http: &str,
    wallets: &[String],
    txq: mpsc::Sender<(String, String)>,
) -> Result<()> {
    let mut retry_delay = Duration::from_secs(1);
    let max_retry_delay = Duration::from_secs(15);
    
    loop {
        let session_started = std::time::Instant::now();
        match handle_websocket_session(wss_url, rpc_http, wallets, txq.clone()).await {
            Ok(_) => {
                // No debería llegar aquí, pero por si acaso
                println!("ℹ️ Sesión terminó normalmente");
                break;
            }
            Err(e) => {
                println!("❌ Error en la conexión WebSocket: {}", e);
                // Si la sesión estuvo viva un buen rato, reseteamos backoff para reconectar rápido.
                if session_started.elapsed() > Duration::from_secs(30) {
                    retry_delay = Duration::from_secs(1);
                }
                println!("🔄 Reintentando conexión en {} segundos...", retry_delay.as_secs());
                
                sleep(retry_delay).await;
                
                // Backoff exponencial con límite máximo
                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                
                println!("🔄 Intentando reconectar...");
            }
        }
    }

    Ok(())
}
