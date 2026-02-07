//! Clasificador de errores para decidir acción (MISS/INFRA_FAIL/INSUFFICIENT/ALREADY_PROCESSED)


/// Categoría del error para decisión de ejecución
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Liquidez / no route / price impact / slippage => no retry en sniper
    Miss,
    /// Blockhash not found / RPC timeout / nodo caído => cooldown corto
    InfraFail,
    /// Fondos insuficientes => hard stop
    InsufficientFunds,
    /// Señal ya procesada o duplicada => ignorar
    AlreadyProcessed,
    /// Error no clasificado
    Unknown,
}

/// Clasifica el string de error en una categoría
pub fn classify_error(err_str: &str) -> ErrorCategory {
    let s = err_str.to_lowercase();

    // LIQUIDITY / NO_ROUTE / PRICE_IMPACT / SLIPPAGE => MISS
    let miss_patterns = [
        "insufficient liquidity",
        "no route",
        "could_not_find_any_route",
        "price impact",
        "slippage",
        "0x1771",
        "slippage tolerance exceeded",
        "quote failed",
        "invalid mint",
        "0x2",
        "tokenzqd",
        "mint invalid",
    ];
    if miss_patterns.iter().any(|p| s.contains(p)) {
        return ErrorCategory::Miss;
    }

    // BLOCKHASH / NODE / RPC_TIMEOUT => INFRA_FAIL
    let infra_patterns = [
        "blockhash not found",
        "block hash not found",
        "node unhealthy",
        "rpc",
        "timeout",
        "connection",
        "rate limited",
        "-32097",
    ];
    if infra_patterns.iter().any(|p| s.contains(p)) {
        return ErrorCategory::InfraFail;
    }

    // INSUFFICIENT_FUNDS => hard stop
    if s.contains("insufficient") && (s.contains("lamports") || s.contains("funds") || s.contains("sol"))
        || s.contains("insufficient_sol")
    {
        return ErrorCategory::InsufficientFunds;
    }

    // ALREADY_PROCESSED / DUPLICATE
    if s.contains("already processed") || s.contains("duplicate") || s.contains("already in block") {
        return ErrorCategory::AlreadyProcessed;
    }

    ErrorCategory::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_miss() {
        assert_eq!(classify_error("COULD_NOT_FIND_ANY_ROUTE"), ErrorCategory::Miss);
        assert_eq!(classify_error("custom program error: 0x1771"), ErrorCategory::Miss);
        assert_eq!(classify_error("Invalid Mint (0x2)"), ErrorCategory::Miss);
        assert_eq!(classify_error("slippage tolerance exceeded"), ErrorCategory::Miss);
        assert_eq!(classify_error("price impact too high"), ErrorCategory::Miss);
    }

    #[test]
    fn test_classify_infra() {
        assert_eq!(classify_error("Blockhash not found"), ErrorCategory::InfraFail);
        assert_eq!(classify_error("RPC timeout"), ErrorCategory::InfraFail);
        assert_eq!(classify_error("rate limited"), ErrorCategory::InfraFail);
    }

    #[test]
    fn test_classify_insufficient() {
        assert_eq!(classify_error("insufficient_sol: balance=..."), ErrorCategory::InsufficientFunds);
        assert_eq!(classify_error("Transfer: insufficient lamports"), ErrorCategory::InsufficientFunds);
    }

    #[test]
    fn test_classify_already_processed() {
        assert_eq!(classify_error("Transaction already processed"), ErrorCategory::AlreadyProcessed);
    }
}
