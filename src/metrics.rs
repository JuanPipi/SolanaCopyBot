use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub struct TxMetrics {
    pub t_detect_ms: u64,
    pub t_build_ms: u64,
    pub t_send_ms: u64,
    pub t_confirm_ms: u64,
    pub total_ms: u64,
    pub success: bool,
    pub error: Option<String>,
}

pub struct MetricsTracker {
    start: Instant,
    detect_done: Option<Instant>,
    build_done: Option<Instant>,
    send_done: Option<Instant>,
}

impl MetricsTracker {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            detect_done: None,
            build_done: None,
            send_done: None,
        }
    }

    pub fn mark_detect_done(&mut self) {
        self.detect_done = Some(Instant::now());
    }

    pub fn mark_build_done(&mut self) {
        self.build_done = Some(Instant::now());
    }

    pub fn mark_send_done(&mut self) {
        self.send_done = Some(Instant::now());
    }

    pub fn finalize(self, success: bool, error: Option<String>) -> TxMetrics {
        let now = Instant::now();

        let t_detect_ms = self
            .detect_done
            .map(|d| d.duration_since(self.start).as_millis() as u64)
            .unwrap_or(0);

        let t_build_ms = match (self.detect_done, self.build_done) {
            (Some(d), Some(b)) => b.duration_since(d).as_millis() as u64,
            _ => 0,
        };

        let t_send_ms = match (self.build_done, self.send_done) {
            (Some(b), Some(s)) => s.duration_since(b).as_millis() as u64,
            _ => 0,
        };

        let t_confirm_ms = self
            .send_done
            .map(|s| now.duration_since(s).as_millis() as u64)
            .unwrap_or(0);

        let total_ms = now.duration_since(self.start).as_millis() as u64;

        TxMetrics {
            t_detect_ms,
            t_build_ms,
            t_send_ms,
            t_confirm_ms,
            total_ms,
            success,
            error,
        }
    }
}

impl TxMetrics {
    pub fn log(&self, action: &str, mint: &str) {
        if self.success {
            println!(
                "📊 [METRICS] {} | mint={} | detect={}ms build={}ms send={}ms confirm={}ms | TOTAL={}ms",
                action, mint, self.t_detect_ms, self.t_build_ms, self.t_send_ms, self.t_confirm_ms, self.total_ms
            );
        } else {
            println!(
                "📊 [METRICS] {} FAILED | mint={} | error={:?} | TOTAL={}ms",
                action,
                mint,
                self.error,
                self.total_ms
            );
        }
    }
}
