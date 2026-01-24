use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::Action;
use crate::signals::{Side, TradeSignal};

pub struct CsvLogger {
    signals_writer: BufWriter<std::fs::File>,
    decisions_writer: BufWriter<std::fs::File>,
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl CsvLogger {
    pub fn new(signals_path: &str) -> std::io::Result<Self> {
        // Archivo de señales
        let signals_exists = Path::new(signals_path).exists();
        let signals_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(signals_path)?;
        let mut signals_writer = BufWriter::new(signals_file);

        if !signals_exists {
            writeln!(
                signals_writer,
                "ts,wallet,side,mint,leader_sol_delta,sig,source"
            )?;
        }

        // Archivo de decisiones
        let decisions_path = "decisions.csv";
        let decisions_exists = Path::new(decisions_path).exists();
        let decisions_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(decisions_path)?;
        let mut decisions_writer = BufWriter::new(decisions_file);

        if !decisions_exists {
            writeln!(
                decisions_writer,
                "ts,wallet,side,mint,leader_sol_delta,sig,action,reason"
            )?;
        }

        Ok(Self {
            signals_writer,
            decisions_writer,
        })
    }

    /// Log de señal raw (antes de decision engine)
    pub fn log_signal(&mut self, s: &TradeSignal, source: &str) {
        let side = match s.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };

        let _ = writeln!(
            self.signals_writer,
            "{},{},{},{},{:.9},{},{}",
            s.ts, s.leader_wallet, side, s.mint, s.leader_sol_delta, s.sig, source
        );
        let _ = self.signals_writer.flush();
    }

    /// Log de decisión (después de decision engine)
    /// Puede recibir un TradeSignal opcional (para decisiones normales) o None (para acciones forzadas)
    pub fn log_decision(&mut self, s: Option<&TradeSignal>, action: &Action) {
        let ts = s.map(|sig| sig.ts).unwrap_or_else(now_ts);
        let wallet = s.map(|sig| sig.leader_wallet.as_str()).unwrap_or("SYSTEM");
        let side = s.map(|sig| match sig.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }).unwrap_or("-");
        let mint_from_signal = s.map(|sig| sig.mint.as_str()).unwrap_or("-");
        let sol_delta = s.map(|sig| sig.leader_sol_delta).unwrap_or(0.0);
        let sig_hash = s.map(|sig| sig.sig.as_str()).unwrap_or("-");

        let (action_str, reason, mint_used) = match action {
            Action::Buy { mint, sol_amount, reason } => {
                ("COPY_BUY".to_string(), format!("sol={} {}", sol_amount, reason), mint.as_str())
            }
            Action::Sell { mint, reason } => {
                ("COPY_SELL".to_string(), reason.clone(), mint.as_str())
            }
            Action::Skip { reason } => {
                ("SKIP".to_string(), reason.clone(), mint_from_signal)
            }
        };

        let _ = writeln!(
            self.decisions_writer,
            "{},{},{},{},{:.9},{},{},\"{}\"",
            ts, wallet, side, mint_used, sol_delta, sig_hash, action_str, reason
        );
        let _ = self.decisions_writer.flush();
    }
}
