//! Logger para missed_trades.jsonl

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;

use serde::Serialize;

use crate::exec_outcome::ExecOutcome;

/// Evento para missed_trades.jsonl
#[derive(Debug, Serialize)]
pub struct MissedTradeEvent {
    pub ts: i64,
    pub mint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader_sig: Option<String>,
    pub leader_delta: f64,
    pub my_amount_sol: f64,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

pub struct MissedLogger {
    path: String,
    file: Mutex<std::fs::File>,
}

impl MissedLogger {
    pub fn new(path: impl Into<String>) -> std::io::Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Loggear cualquier ExecOutcome
    pub fn log_outcome(
        &self,
        mint: &str,
        leader_sig: Option<&str>,
        leader_delta: f64,
        my_amount_sol: f64,
        outcome: &ExecOutcome,
    ) {
        let (outcome_str, reason, stage, details) = match outcome {
            ExecOutcome::Filled { .. } => ("Filled".to_string(), None, None, None),
            ExecOutcome::Missed { reason, stage, details, .. } => (
                "Missed".to_string(),
                Some(format!("{:?}", reason)),
                Some(format!("{:?}", stage)),
                details.clone(),
            ),
            ExecOutcome::Failed { err, stage, .. } => (
                "Failed".to_string(),
                None,
                Some(format!("{:?}", stage)),
                Some(err.clone()),
            ),
        };

        let evt = MissedTradeEvent {
            ts: outcome.ts(),
            mint: mint.to_string(),
            leader_sig: leader_sig.map(|s| s.to_string()),
            leader_delta,
            my_amount_sol,
            outcome: outcome_str,
            reason,
            stage,
            details,
        };

        if let Ok(json) = serde_json::to_string(&evt) {
            if let Ok(mut f) = self.file.lock() {
                let _ = writeln!(f, "{}", json);
                let _ = f.flush();
            }
        }
    }
}
