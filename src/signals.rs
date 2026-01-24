#[derive(Debug, Clone)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub leader_wallet: String,
    pub side: Side,
    pub mint: String,
    pub leader_sol_delta: f64,
    pub sig: String,
    pub ts: i64,
}
