# Solana Copy Bot

A high-performance copy trading bot for Solana written in Rust. Monitors wallet transactions in real-time and mirrors trades with dynamic position sizing.

## Features

- **Real-time monitoring** via WebSocket (logsSubscribe)
- **Dynamic position sizing** based on leader's trade size
- **Risk management**:
  - Exposure cap
  - Reserve protection
  - Rate limiting
  - Cooldown after orphan sells
  - Max hold time with forced sell
- **State persistence** (survives restarts)
- **CSV logging** for signals and decisions
- **Dry-run mode** for testing
- **SNIPER mode** (default): single-shot execution, maximum inclusion probability, no retries on liquidity/slippage

## Architecture

```
Signal Ingest (WS) -> Decoder -> Decision Engine -> Executor -> Broadcaster
                                      |
                                 State Machine
                              (positions, cooldowns)
```

## Setup

### Prerequisites

- Rust 1.70+
- [Helius](https://helius.dev) API key (free tier works)

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/solana_copy_bot.git
cd solana_copy_bot
cp .env.example .env
# Edit .env with your API keys and wallets
cargo build --release
```

### Configuration

Edit `.env`:

```env
HELIUS_HTTP=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
HELIUS_WSS=wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY
WALLETS=WALLET1,WALLET2,WALLET3
```

### Risk Configuration

Edit `src/main.rs` to adjust risk parameters:

```rust
RiskConfig {
    min_trade_sol: 0.02,         // Minimum trade size
    max_trade_sol: 0.10,         // Maximum trade size
    k_leader_scale: 0.035,       // Scaling factor: my_trade = k * leader_delta
    min_leader_sol_delta: 0.15,  // Minimum leader trade to copy
    exposure_cap_sol: 0.35,      // Maximum total exposure
    reserve_sol: 0.20,           // Untouchable reserve
    total_capital_sol: 1.0,      // Your total capital
    min_buy_interval_secs: 15,   // Rate limit between buys
    cooldown_secs: 60,           // Cooldown after orphan sell
    max_hold_secs: 21600,        // 6 hours max hold
}
```

## Usage

### Dry Run (default)

```bash
cargo run
```

### Production

1. Set `dry_run: false` in `src/main.rs`
2. Add your wallet keypair (not implemented yet)
3. Run with `cargo run --release`

## Output Files

| File | Description |
|------|-------------|
| `state.json` | Persisted state (positions, cooldowns) |
| `signals.csv` | All detected trade signals |
| `decisions.csv` | Engine decisions (copy/skip/reason) |

## How It Works

1. **Signal Detection**: Monitors wallet transactions via WebSocket
2. **Transaction Analysis**: Extracts token deltas and SOL changes
3. **Decision Engine**: Applies risk rules and sizing
4. **Execution**: Sends transactions (dry-run by default)

### Position Sizing Formula

```
my_trade = clamp(k * |leader_sol_delta|, min_trade, max_trade)
```

Example with k=0.035:
- Leader trades 2 SOL → You trade 0.07 SOL
- Leader trades 5 SOL → You trade 0.10 SOL (capped)

## Sniper Mode (Default)

El bot usa por defecto `EXECUTION_MODE=sniper`:

- **1 intento** por señal: ultra rápido, máxima probabilidad de inclusión
- **Sin reintentos** por liquidez insuficiente, slippage o price impact
- **Estado consistente**: NO se guarda posición hasta confirmación on-chain real
- **Dedupe**: `signal_id` determinístico evita doble envío
- **Reconciliación**: txs con timeout se guardan en `pending_actions` y se verifican periódicamente

### Variables de entorno (Sniper)

| Variable | Default | Descripción |
|----------|---------|-------------|
| EXECUTION_MODE | sniper | `sniper` o `retry` |
| SNIPER_SINGLE_SHOT | true | Un solo intento por señal (sin reintentos) |
| MAX_SLIPPAGE_BPS_SNIPER | 1500 | Slippage máximo (15%) en sniper |
| JITO_TIP_LAMPORTS_SNIPER | 20000 | Tip Jito para sniper |
| COMPUTE_UNIT_LIMIT | 1400000 | Límite de compute units |
| COOLDOWN_MISS_MS | 30000 | Cooldown tras MISS (30s) |
| CONFIRM_TIMEOUT_SECS | 10 | Timeout de confirmación on-chain |
| MAX_LEADER_LAG_MS | 1200 | Lag máximo líder (ms) - Missed{LatencyTooHigh} si excede |
| MIN_TRADE_SOL_SNIPER | 0.01 | Mínimo SOL por trade - Missed{AmountTooSmall} si menor |

### ExecOutcome y missed_trades.jsonl

- **Filled**: trade ejecutado y confirmado on-chain
- **Missed**: NoRoute, InsufficientLiquidity, AmountTooSmall, LatencyTooHigh, QuoteExpired (no reintentar)
- **Failed**: errores de infra (RPC, timeout) - no reintentar en sniper

Se registra cada evento en `missed_trades.jsonl` (una línea JSON por evento).

### Ejemplos de logs esperados

**1. Filled (éxito)**
```
🎯 [SNIPER] BUY 1 intento | mint=AbC12345 | sol=0.0100 | slippage=1500bps
   ✓ Quote: 10000000 lamports -> 500000 tokens
✅ [SNIPER] FILLED | sig=3xYz... | balance=500000 tokens
```

**2. Missed NoRoute**
```
🎯 [SNIPER] BUY 1 intento | mint=GdWo7jwq | sol=0.0100 | slippage=1500bps
   ❌ [SNIPER] MISS NoRoute | stage=Quote | Jupiter quote error: ...
```

**3. Missed LatencyTooHigh**
```
   ❌ [SNIPER] MISS LatencyTooHigh | lag=1500ms > max=1200ms
```

## Security Notes

- Never commit `.env` or API keys
- The bot runs in dry-run mode by default
- No private keys are stored in the codebase
- State files may contain transaction signatures

## License

Unlicense - Public Domain

## Disclaimer

This software is for educational purposes only. Trading cryptocurrencies involves significant risk. Use at your own risk.
