use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub async fn poll_signatures(
    rpc_http: String,
    wallet: String,
    sender: mpsc::Sender<(String, String)>,
) -> Result<()> {
    let client = RpcClient::new(rpc_http);
    let pubkey: Pubkey = wallet.parse()?;

    let mut last_seen: Option<String> = None;

    loop {
        // Usamos el método simple sin config para evitar problemas de tipos
        match client.get_signatures_for_address(&pubkey).await {
            Ok(sigs) => {
                // Al iniciar, solo marcamos la más reciente sin procesar historial
                if last_seen.is_none() {
                    if let Some(first) = sigs.first() {
                        last_seen = Some(first.signature.clone());
                    }
                    sleep(Duration::from_millis(900)).await;
                    continue;
                }

                // vienen más nuevas primero
                for s in sigs.iter() {
                    let sig = s.signature.clone();

                    // cortamos cuando llegamos a lo ya visto
                    if let Some(ls) = &last_seen {
                        if &sig == ls {
                            break;
                        }
                    }

                    // mandamos a analizar con tag |POLL
                    let tagged = format!("{}|POLL", wallet);
                    let _ = sender.send((tagged, sig)).await;
                }

                // actualizamos "last seen" con la más reciente
                if let Some(first) = sigs.first() {
                    last_seen = Some(first.signature.clone());
                }
            }
            Err(e) => {
                eprintln!("⚠️ Error obteniendo signatures para {}: {}", &wallet[..std::cmp::min(wallet.len(), 6)], e);
            }
        }

        // 900ms = 1.1 req/s aprox → re bien con tu límite
        sleep(Duration::from_millis(900)).await;
    }
}
