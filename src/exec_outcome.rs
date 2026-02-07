//! Resultado de ejecución BUY: Filled / Missed / Failed

use serde::Serialize;

/// Razón de MISSED (no reintentar)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum MissedReason {
    NoRoute,
    InsufficientLiquidity,
    AmountTooSmall,
    LatencyTooHigh,
    LeaderAlreadyExited,
    QuoteExpired,
    MissRisk,
}

/// Etapa donde ocurrió el fallo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecStage {
    Quote,
    Build,
    Send,
    Confirm,
}

/// Resultado de ejecución BUY (sniper single-shot)
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "PascalCase")]
pub enum ExecOutcome {
    Filled {
        sig: String,
        in_amount_sol: f64,
        out_amount: u64,
        price_impact_bps: Option<u32>,
        ts: i64,
    },
    Missed {
        reason: MissedReason,
        stage: ExecStage,
        details: Option<String>,
        ts: i64,
    },
    Failed {
        err: String,
        stage: ExecStage,
        ts: i64,
    },
}

impl ExecOutcome {
    pub fn ts(&self) -> i64 {
        match self {
            ExecOutcome::Filled { ts, .. } => *ts,
            ExecOutcome::Missed { ts, .. } => *ts,
            ExecOutcome::Failed { ts, .. } => *ts,
        }
    }

    pub fn is_filled(&self) -> bool {
        matches!(self, ExecOutcome::Filled { .. })
    }

    pub fn is_missed(&self) -> bool {
        matches!(self, ExecOutcome::Missed { .. })
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, ExecOutcome::Failed { .. })
    }
}
