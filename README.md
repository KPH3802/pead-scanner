# PEAD Scanner

Nightly earnings scanner for **Post-Earnings Announcement Drift (PEAD)** signals. Runs on PythonAnywhere at 01:30 UTC and feeds BUY and SHORT signals to the IB AutoTrader via email.

## Signal Logic

- **BULL**: EPS actual beat estimate by ≥5% → long signal, 28-day hold
- **BEAR**: EPS actual missed estimate by ≥5% → short signal, 28-day hold
- **Q4 EXCLUDED**: Reports in January or February (Q4 earnings season) are skipped — signal reverses during Q4

Entry is placed 2 trading days after the report date, matching backtest methodology.

## Backtest Results

| Signal | Alpha (4w) | t-stat | Notes |
|--------|-----------|--------|-------|
| BULL (beat ≥5%) | +1.77%/trade | 17.08*** | Best Q3: +4.17% t=18.67 |
| BEAR (miss ≤-5%) | -1.74%/trade | 5.76*** | Short signal |
| Larger surprises (30%+) | +2.75%/trade | — | Scale effect confirmed |

Universe: S&P 500, 2018–2026. Q4 (Jan/Feb reports) excluded — signal reverses to -1.12%.

## Architecture

```
pead_scanner/
├── pead_scanner.py       # Main scanner — runs nightly on PythonAnywhere
├── config.py             # Credentials and thresholds (not committed)
└── config_example.py     # Template — copy to config.py and fill in values
```

The scanner reads from FMP's earning calendar API, detects beats/misses, deduplicates via SQLite, and sends an HTML email with a subject the IB AutoTrader can parse:

- `PEAD BULL: AAPL, MSFT` → autotrader places BUY orders
- `PEAD BEAR: NFLX` → autotrader places SHORT orders
- `PEAD BULL: AAPL | BEAR: NFLX` → both

## Setup

### 1. Install dependencies
```bash
pip install yfinance requests
```

### 2. Configure
```bash
cp config_example.py config.py
# Edit config.py with your FMP API key, email credentials
```

### 3. Test
```bash
python3 pead_scanner.py --test-email    # Verify email config
python3 pead_scanner.py --dry-run       # Detect signals, skip email
python3 pead_scanner.py --backfill 14   # Scan last 14 days
python3 pead_scanner.py --status        # Show DB stats
```

### 4. Deploy to PythonAnywhere
Schedule `python3 pead_scanner.py` at **01:30 UTC** daily.

Working directory must be set to the `pead_scanner/` folder so SQLite writes to the correct path.

## IB AutoTrader Integration

The `ib-autotrader` repo parses PEAD email subjects via `query_pead_signals_from_email()`. Detected tickers are sized at full position (\$5,000) and tracked in `positions.db` with:
- 28-day time exit
- -40% catastrophic circuit breaker

## Configuration Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `BEAT_THRESHOLD` | 5.0 | % EPS beat required for BULL signal |
| `MISS_THRESHOLD` | 5.0 | % EPS miss required for BEAR signal |
| `ENABLE_BEAR` | True | Set False to disable short signals |
| `Q4_EXCLUDE_MONTHS` | [1, 2] | Months to skip (Jan/Feb = Q4 reports) |

## Disclaimer

For research and educational purposes. Not investment advice. Past backtest performance does not guarantee future results. All trading involves risk.

---

MIT License

