#!/usr/bin/env python3
"""
PEAD Scanner -- Live Signal Generator
======================================
Post-Earnings Announcement Drift scanner. Runs nightly on PythonAnywhere (01:30 UTC).
Detects EPS beats/misses from yesterday's earnings calendar and fires BUY (BULL) or
SHORT (BEAR) signals to the IB AutoTrader via email.

Backtest results (S&P 500, 2018-2026, 4-week hold):
  BULL (beat >=5%):  +1.77% alpha, t=17.08***   Best quarter: Q3 +4.17%
  BEAR (miss <=-5%): -1.74% alpha, t=5.76***    (short signal)
  Q4 EXCLUDED: Jan/Feb reports reverse (-1.12%)

Signal entry timing: 2 trading days after report date (matches backtest)
Scanner fires at 01:30 UTC each morning, targeting reports from 2 trading days prior.

Usage:
  python3 pead_scanner.py              # Normal daily run
  python3 pead_scanner.py --test-email # Send test email
  python3 pead_scanner.py --status     # Show DB stats
  python3 pead_scanner.py --backfill 7 # Scan last 7 days (for catch-up)
  python3 pead_scanner.py --dry-run    # Detect signals, skip email
"""

import os
import sys
import json
import sqlite3
import smtplib
import traceback
import argparse
from datetime import datetime, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import config

# ============================================================
# CONSTANTS
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, config.DB_NAME)
FMP_BASE = config.FMP_BASE_URL.rstrip('/')

BEAT_THRESHOLD = config.BEAT_THRESHOLD   # EPS beat % -> BULL
MISS_THRESHOLD = config.MISS_THRESHOLD   # EPS miss % -> BEAR
MIN_ABS_EPS    = config.MIN_ABS_EPS
Q4_MONTHS      = set(config.Q4_EXCLUDE_MONTHS)
ENABLE_BEAR    = config.ENABLE_BEAR

# ---------------------------------------------------------------------------
# Signal Intelligence — live logging
# ---------------------------------------------------------------------------
def log_signal_intelligence(scan_date, scanner, ticker, direction, fired,
                             signal_strength=None, signal_bucket=None,
                             regime_filter_passed=None, regime_value=None,
                             score=None):
    try:
        import sqlite3 as _sl
        db = os.path.expanduser('~/signal_intelligence.db')
        c = _sl.connect(db)
        c.execute('CREATE TABLE IF NOT EXISTS signal_log (id INTEGER PRIMARY KEY AUTOINCREMENT, scan_date TEXT, scanner TEXT, ticker TEXT, direction TEXT, fired INTEGER, signal_strength REAL, signal_bucket TEXT, regime_filter_passed INTEGER, regime_value REAL, score INTEGER, autotrader_acted INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP)')
        c.execute('INSERT INTO signal_log (scan_date,scanner,ticker,direction,fired,signal_strength,signal_bucket,regime_filter_passed,regime_value,score) VALUES (?,?,?,?,?,?,?,?,?,?)',
                  (scan_date,scanner,ticker,direction,fired,signal_strength,signal_bucket,regime_filter_passed,regime_value,score))
        c.commit(); c.close()
    except Exception:
        pass

# ============================================================
# TRADING DAY UTILITIES
# ============================================================

def n_trading_days_ago(n):
    """Return the date that was exactly n trading days (Mon-Fri) ago."""
    d = datetime.utcnow().date()
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # 0=Mon, 4=Fri
            count += 1
    return d

def date_range_str(start_date, end_date):
    """Return (from_str, to_str) in YYYY-MM-DD format."""
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# ============================================================
# DATABASE
# ============================================================

def init_db():
    """Create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pead_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker          TEXT NOT NULL,
            report_date     TEXT NOT NULL,
            eps_actual      REAL,
            eps_estimated   REAL,
            surprise_pct    REAL NOT NULL,
            direction       TEXT NOT NULL,
            entry_price     REAL,
            sector          TEXT,
            company_name    TEXT,
            detected_date   TEXT NOT NULL,
            emailed         INTEGER DEFAULT 0,
            UNIQUE(ticker, report_date)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS scan_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date       TEXT,
            target_date     TEXT,
            reports_found   INTEGER,
            signals_found   INTEGER,
            new_signals     INTEGER,
            email_sent      INTEGER,
            errors          TEXT
        )
    """)
    conn.commit()
    return conn

# ============================================================
# FMP API
# ============================================================

def fmp_fetch(endpoint, params=None):
    """Fetch JSON from FMP API with error handling."""
    if not config.FMP_API_KEY or config.FMP_API_KEY == 'YOUR_FMP_API_KEY_HERE':
        print('ERROR: FMP_API_KEY not configured')
        return None
    url = f'{FMP_BASE}/{endpoint}?apikey={config.FMP_API_KEY}'
    if params:
        for k, v in params.items():
            url += f'&{k}={v}'
    try:
        req = Request(url)
        req.add_header('User-Agent', 'PEADScanner/1.0')
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        print(f'  FMP HTTP {e.code}: {endpoint}')
        return None
    except URLError as e:
        print(f'  FMP URL Error: {e.reason}')
        return None
    except Exception as e:
        print(f'  FMP Error: {e}')
        return None

def get_edgar_earnings_tickers(from_date, to_date):
    """
    Use SEC EDGAR full-text search to find tickers that filed 8-K Item 2.02
    (Results of Operations = earnings release) in the given date range.
    Parses ticker from display_names field: 'COMPANY (TICK) (CIK XXXXXXXX)'.
    Returns list of (ticker, file_date) tuples.
    """
    import re as _re
    url = (
        f'https://efts.sec.gov/LATEST/search-index?q=%22item+2.02%22'
        f'&dateRange=custom&startdt={from_date}&enddt={to_date}&forms=8-K'
        f'&hits.hits.total.value=true'
    )
    try:
        req = Request(url)
        req.add_header('User-Agent', 'PEADScanner/1.0 kph3802@gmail.com')
        with urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        hits = data.get('hits', {}).get('hits', [])
        results = []
        seen = set()
        ticker_pat = _re.compile(r'\(([A-Z]{1,5})\)\s*\(CIK')
        for hit in hits:
            src_data = hit.get('_source', {})
            file_date = src_data.get('file_date', from_date)
            # Filter: must contain item 2.02
            items = src_data.get('items', [])
            if '2.02' not in items:
                continue
            for display in src_data.get('display_names', []):
                m = ticker_pat.search(display)
                if m:
                    ticker = m.group(1)
                    if ticker not in seen and len(ticker) <= 5:
                        seen.add(ticker)
                        results.append((ticker, file_date[:10] if file_date else from_date))
        total = data.get('hits', {}).get('total', {}).get('value', 0)
        print(f'  EDGAR: {total} Item 2.02 filings, {len(results)} unique tickers extracted')
        return results
    except Exception as e:
        print(f'  EDGAR search failed: {e}')
        return []

def get_fmp_earnings_for_ticker(ticker, target_date):
    """
    Fetch the most recent earnings entry for a ticker from FMP stable/earnings.
    Returns a single dict or None.
    """
    data = fmp_fetch('stable/earnings', {'symbol': ticker})
    if not data or not isinstance(data, list):
        return None
    # Find the entry closest to (and on or before) target_date
    target = target_date if isinstance(target_date, str) else target_date.strftime('%Y-%m-%d')
    candidates = [
        e for e in data
        if e.get('date', '') <= target
        and e.get('epsActual') is not None
        and e.get('epsEstimated') is not None
    ]
    if not candidates:
        return None
    # Most recent
    best = max(candidates, key=lambda x: x.get('date', ''))
    return {
        'ticker':        ticker,
        'report_date':   best.get('date', target),
        'eps_actual':    best.get('epsActual'),
        'eps_estimated': best.get('epsEstimated'),
        'revenue_actual':    best.get('revenueActual'),
        'revenue_estimated': best.get('revenueEstimated'),
    }


def get_earning_calendar(from_date, to_date):
    """
    Get recent earnings reporters using SEC EDGAR Item 2.02 filings,
    then fetch actual/estimated EPS from FMP per-ticker.
    Falls back gracefully if EDGAR or FMP calls fail.
    """
    import time
    ticker_dates = get_edgar_earnings_tickers(from_date, to_date)
    if not ticker_dates:
        return []
    results = []
    for ticker, file_date in ticker_dates:
        entry = get_fmp_earnings_for_ticker(ticker, file_date)
        if entry and entry.get('eps_actual') is not None:
            results.append(entry)
        time.sleep(0.2)   # Rate limit: 300/min FMP starter
    print(f'  Enriched {len(results)} of {len(ticker_dates)} EDGAR tickers with FMP EPS data')
    return results

def get_current_price(ticker):
    """Fetch current price via yfinance (preferred) or FMP quote."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        hist = t.history(period='1d')
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
    except Exception:
        pass
    # Fallback: FMP quote
    data = fmp_fetch(f'api/v3/quote/{ticker}')
    if data and isinstance(data, list) and data:
        return data[0].get('price')
    return None

def get_sector(ticker):
    """Fetch sector and company name via yfinance with fallback."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return (
            info.get('sector', 'Unknown'),
            info.get('shortName') or info.get('longName', ticker)
        )
    except Exception:
        return 'Unknown', ticker

# ============================================================
# SIGNAL DETECTION
# ============================================================

def is_valid_ticker(ticker):
    """Filter out OTC, preferred, warrants. Keep NYSE/NASDAQ style."""
    if not ticker:
        return False
    if len(ticker) > 5:
        return False
    if '.' in ticker or '-' in ticker or '+' in ticker:
        return False
    return True

def compute_surprise(eps_actual, eps_estimated, min_abs_eps):
    """Return surprise_pct or None if inputs are invalid."""
    try:
        a = float(eps_actual)
        e = float(eps_estimated)
    except (TypeError, ValueError):
        return None
    if abs(e) < min_abs_eps:
        return None
    return (a - e) / abs(e) * 100.0

def is_q4_excluded(report_date_str):
    """
    Returns True if report_date falls in Jan or Feb (Q4 earnings season).
    Signal REVERSES in Q4 -- must exclude.
    """
    try:
        month = datetime.strptime(report_date_str, '%Y-%m-%d').month
        return month in Q4_MONTHS
    except ValueError:
        return False

def assign_pead_score(surprise_pct, direction):
    """
    Path B scoring: EPS surprise magnitude -> Score 2/3 for position sizing.
    BULL: 5-9.9% beat -> Score 2 (3%),  10%+ beat -> Score 3 (5%)
    BEAR: all buckets -> Score 2 (valid signal, smaller alpha than BULL)
    Based on pead_scoring.py backtest (2018-2026, 9,653 trades).
    """
    abs_surp = abs(surprise_pct)
    if direction == 'BULL':
        return 2 if abs_surp < 10.0 else 3
    else:  # BEAR
        return 2


def detect_signals(calendar_entries):
    """
    Filter calendar entries into BULL and BEAR signals.
    Returns list of signal dicts with score field (Path B scoring).
    """
    signals = []
    for entry in calendar_entries:
        ticker      = entry['ticker']
        report_date = entry['report_date']
        eps_actual  = entry['eps_actual']
        eps_est     = entry['eps_estimated']
        if not is_valid_ticker(ticker):
            continue
        if not report_date:
            continue
        # Q4 exclusion: Jan/Feb reports reverse the signal
        if is_q4_excluded(report_date):
            print(f'  {ticker}: Q4 exclusion ({report_date}) -- skipped')
            continue
        # Compute surprise
        surprise = compute_surprise(eps_actual, eps_est, MIN_ABS_EPS)
        if surprise is None:
            continue
        # Bucket for logging
        abs_s = abs(surprise)
        if surprise <= -10:
            _bucket = '<=-10'
        elif surprise < 0:
            _bucket = '-10-0'
        elif surprise < 10:
            _bucket = '0-10'
        else:
            _bucket = '10+'
        # Apply thresholds
        if surprise >= BEAT_THRESHOLD:
            direction = 'BULL'
        elif surprise <= -MISS_THRESHOLD:
            if not ENABLE_BEAR:
                log_signal_intelligence(report_date, 'PEAD_BEAR', ticker, 'SHORT', 0,
                                        signal_strength=round(surprise, 2), signal_bucket=_bucket)
                continue
            direction = 'BEAR'
        else:
            # Below threshold — log as not fired
            _dir = 'BUY' if surprise >= 0 else 'SHORT'
            _scanner = 'PEAD_BULL' if surprise >= 0 else 'PEAD_BEAR'
            log_signal_intelligence(report_date, _scanner, ticker, _dir, 0,
                                    signal_strength=round(surprise, 2), signal_bucket=_bucket)
            continue
        score = assign_pead_score(round(surprise, 2), direction)
        # Log fired signal
        _ib_dir = 'BUY' if direction == 'BULL' else 'SHORT'
        _scanner = 'PEAD_BULL' if direction == 'BULL' else 'PEAD_BEAR'
        log_signal_intelligence(report_date, _scanner, ticker, _ib_dir, 1,
                                signal_strength=round(surprise, 2), signal_bucket=_bucket,
                                score=score)
        signals.append({
            'ticker':        ticker,
            'report_date':   report_date,
            'eps_actual':    eps_actual,
            'eps_estimated': eps_est,
            'surprise_pct':  round(surprise, 2),
            'direction':     direction,
            'score':         score,
        })
    return signals


def filter_new_signals(conn, signals):
    """Remove signals already in the database (already emailed)."""
    c = conn.cursor()
    new = []
    for s in signals:
        c.execute(
            'SELECT id FROM pead_signals WHERE ticker=? AND report_date=?',
            (s['ticker'], s['report_date'])
        )
        if c.fetchone() is None:
            new.append(s)
    return new

def enrich_signal(signal):
    """Add entry price, sector, and company name to a signal dict."""
    print(f"  Enriching {signal['ticker']}...")
    price = get_current_price(signal['ticker'])
    sector, company = get_sector(signal['ticker'])
    signal['entry_price'] = price
    signal['sector']      = sector
    signal['company_name'] = company
    return signal

def store_signal(conn, signal):
    """Write signal to DB. Returns True if inserted, False if duplicate."""
    c = conn.cursor()
    today = datetime.utcnow().strftime('%Y-%m-%d')
    try:
        c.execute("""
            INSERT INTO pead_signals
            (ticker, report_date, eps_actual, eps_estimated, surprise_pct,
             direction, entry_price, sector, company_name, detected_date, emailed)
            VALUES (?,?,?,?,?,?,?,?,?,?,0)
        """, (
            signal['ticker'], signal['report_date'],
            signal['eps_actual'], signal['eps_estimated'],
            signal['surprise_pct'], signal['direction'],
            signal.get('entry_price'), signal.get('sector', 'Unknown'),
            signal.get('company_name', signal['ticker']),
            today
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def mark_emailed(conn, signal_ids):
    """Mark a list of signal IDs as emailed."""
    if not signal_ids:
        return
    c = conn.cursor()
    c.executemany(
        'UPDATE pead_signals SET emailed=1 WHERE id=?',
        [(i,) for i in signal_ids]
    )
    conn.commit()

# ============================================================
# EMAIL
# ============================================================

DIRECTION_COLOR = {
    'BULL': '#00c853',
    'BEAR': '#f44336',
}
DIRECTION_LABEL = {
    'BULL': 'LONG',
    'BEAR': 'SHORT',
}

def build_email_subject(new_signals):
    """Build email subject parseable by IB autotrader.
    Score suffix (:2 or :3) sets position size via Path B scoring.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    bulls = [s for s in new_signals if s["direction"] == "BULL"]
    bears = [s for s in new_signals if s["direction"] == "BEAR"]
    parts = []
    if bulls:
        tickers = ", ".join(str(s["ticker"]) + ":" + str(s.get("score", 3)) for s in bulls)
        parts.append("PEAD BULL: " + tickers)
    if bears:
        tickers = ", ".join(str(s["ticker"]) + ":" + str(s.get("score", 2)) for s in bears)
        parts.append("PEAD BEAR: " + tickers)
    if parts:
        return " | ".join(parts)
    return "PEAD Scanner -- No signals (" + today + ")"

def build_email_html(new_signals, recent_signals):
    """Build rich HTML email showing BULL and BEAR signals."""
    today = datetime.utcnow().strftime('%Y-%m-%d')
    bulls = [s for s in new_signals if s['direction'] == 'BULL']
    bears = [s for s in new_signals if s['direction'] == 'BEAR']

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background:#1a1a2e; color:#e0e0e0; margin:0; padding:0; }}
        .wrap {{ max-width:700px; margin:0 auto; padding:20px; }}
        h1 {{ color:#00e676; font-size:22px; border-bottom:2px solid #333; padding-bottom:10px; margin-top:0; }}
        h2 {{ font-size:17px; margin-top:28px; margin-bottom:12px; }}
        .bull-head {{ color:#00c853; }}
        .bear-head {{ color:#f44336; }}
        .summary {{ background:#16213e; border-radius:8px; padding:14px; margin:14px 0; font-size:14px; }}
        .card {{ background:#16213e; border-radius:8px; padding:14px; margin:10px 0; }}
        .card-bull {{ border-left:4px solid #00c853; }}
        .card-bear {{ border-left:4px solid #f44336; }}
        .ticker {{ font-size:22px; font-weight:bold; color:#fff; }}
        .badge {{ display:inline-block; padding:3px 10px; border-radius:12px;
                  font-size:12px; font-weight:bold; color:#fff; margin-left:10px; vertical-align:middle; }}
        .metrics {{ display:flex; gap:18px; margin:10px 0; flex-wrap:wrap; }}
        .metric {{ text-align:center; }}
        .mv {{ font-size:16px; font-weight:bold; color:#fff; }}
        .ml {{ font-size:11px; color:#888; }}
        .meta {{ font-size:12px; color:#aaa; margin-top:6px; }}
        .backtest {{ background:#0d2137; border:1px solid #1a5276; border-radius:8px;
                     padding:12px; margin:20px 0; font-size:12px; color:#7fb3d8; }}
        table {{ width:100%; border-collapse:collapse; margin-top:8px; }}
        th {{ background:#0f3460; color:#e0e0e0; padding:7px; text-align:left; font-size:12px; }}
        td {{ padding:7px; border-bottom:1px solid #333; font-size:12px; }}
        .footer {{ color:#555; font-size:11px; margin-top:28px; border-top:1px solid #333; padding-top:10px; }}
        .no-signal {{ color:#888; font-style:italic; padding:12px 0; }}
      </style>
    </head>
    <body>
    <div class='wrap'>
      <h1>PEAD SCANNER &mdash; {today}</h1>
    """

    if new_signals:
        html += f"""
      <div class='summary'>
        <strong>{len(new_signals)} new signal(s) detected</strong>
        &nbsp;|&nbsp; BULL (long): {len(bulls)}
        &nbsp;|&nbsp; BEAR (short): {len(bears)}
        &nbsp;|&nbsp; Target report date: {new_signals[0].get('report_date','N/A')}
      </div>
        """
    else:
        html += "<div class='summary'>No new PEAD signals today.</div>"

    # BULL section
    if bulls:
        html += "<h2 class='bull-head'>&#9650; BULL SIGNALS (Long)</h2>"
        for s in sorted(bulls, key=lambda x: -abs(x['surprise_pct'])):
            price_str = f"${s['entry_price']:.2f}" if s.get('entry_price') else 'N/A'
            eps_str   = f"{s['eps_actual']:.3f} vs est {s['eps_estimated']:.3f}" if s.get('eps_estimated') else f"{s['eps_actual']:.3f}"
            html += f"""
      <div class='card card-bull'>
        <span class='ticker'>{s['ticker']}</span>
        <span class='badge' style='background:#00c853;'>BULL &bull; LONG</span>
        <div class='metrics'>
          <div class='metric'>
            <div class='mv' style='color:#00c853;'>+{s['surprise_pct']:.1f}%</div>
            <div class='ml'>EPS BEAT</div>
          </div>
          <div class='metric'>
            <div class='mv'>{price_str}</div>
            <div class='ml'>ENTRY PRICE</div>
          </div>
          <div class='metric'>
            <div class='mv'>28d</div>
            <div class='ml'>HOLD PERIOD</div>
          </div>
        </div>
        <div class='meta'>
          {s.get('company_name', s['ticker'])} &bull; {s.get('sector','Unknown')} &bull;
          EPS: {eps_str} &bull; Report: {s['report_date']}
        </div>
      </div>
            """

    # BEAR section
    if bears:
        html += "<h2 class='bear-head'>&#9660; BEAR SIGNALS (Short)</h2>"
        for s in sorted(bears, key=lambda x: x['surprise_pct']):
            price_str = f"${s['entry_price']:.2f}" if s.get('entry_price') else 'N/A'
            eps_str   = f"{s['eps_actual']:.3f} vs est {s['eps_estimated']:.3f}" if s.get('eps_estimated') else f"{s['eps_actual']:.3f}"
            html += f"""
      <div class='card card-bear'>
        <span class='ticker'>{s['ticker']}</span>
        <span class='badge' style='background:#f44336;'>BEAR &bull; SHORT</span>
        <div class='metrics'>
          <div class='metric'>
            <div class='mv' style='color:#f44336;'>{s['surprise_pct']:.1f}%</div>
            <div class='ml'>EPS MISS</div>
          </div>
          <div class='metric'>
            <div class='mv'>{price_str}</div>
            <div class='ml'>ENTRY PRICE</div>
          </div>
          <div class='metric'>
            <div class='mv'>28d</div>
            <div class='ml'>HOLD PERIOD</div>
          </div>
        </div>
        <div class='meta'>
          {s.get('company_name', s['ticker'])} &bull; {s.get('sector','Unknown')} &bull;
          EPS: {eps_str} &bull; Report: {s['report_date']}
        </div>
      </div>
            """

    # Backtest reference
    html += """
      <div class='backtest'>
        <strong>Backtest Reference (S&amp;P 500, 2018-2026, 4-week hold):</strong><br>
        BULL (beat &ge;5%): +1.77% alpha/trade, t=17.08*** &nbsp;|&nbsp;
        Best Q3: +4.17% t=18.67*** &nbsp;|&nbsp;
        Q4 EXCLUDED (signal reverses)<br>
        BEAR (miss &le;-5%): -1.74% alpha/trade, t=5.76*** (short)<br>
        Larger surprises drift more (30%+ beat: +2.75%)
      </div>
    """

    # Recent history
    if recent_signals:
        html += """
      <h2 style='color:#64b5f6;'>Recent Signal History (last 14 days)</h2>
      <table>
        <tr>
          <th>Report Date</th><th>Ticker</th><th>Direction</th>
          <th>Surprise</th><th>Price</th><th>Sector</th>
        </tr>
        """
        for r in recent_signals[:20]:
            d_color = '#00c853' if r[3] == 'BULL' else '#f44336'
            price_str = f'${r[5]:.2f}' if r[5] else 'N/A'
            html += f"""
        <tr>
          <td>{r[0]}</td>
          <td><strong>{r[1]}</strong></td>
          <td style='color:{d_color};'>{r[3]}</td>
          <td style='color:{d_color};'>{r[2]:+.1f}%</td>
          <td>{price_str}</td>
          <td>{r[4] or 'N/A'}</td>
        </tr>
            """
        html += '</table>'

    html += f"""
      <div class='footer'>
        PEAD Scanner v1.0 &nbsp;|&nbsp;
        Signal: EPS beat/miss &ge;5%, entry +2 trading days, 28-day hold &nbsp;|&nbsp;
        Q4 excluded (Jan/Feb reports) &nbsp;|&nbsp;
        IB AutoTrader parses subject: 'PEAD BULL: ...' and 'PEAD BEAR: ...'<br>
        Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
      </div>
    </div>
    </body>
    </html>
    """
    return html

def send_email(subject, html_body):
    """Send HTML email via SMTP."""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = config.EMAIL_SENDER
    msg['To']      = config.EMAIL_RECIPIENT
    msg.attach(MIMEText(html_body, 'html'))
    try:
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as srv:
            srv.starttls()
            srv.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
            srv.sendmail(config.EMAIL_SENDER, config.EMAIL_RECIPIENT, msg.as_string())
        print('  Email sent successfully')
        return True
    except Exception as e:
        print(f'  ERROR sending email: {e}')
        return False

# ============================================================
# DATABASE QUERIES
# ============================================================

def get_recent_signals(conn, days=14):
    """Fetch recent signals for the email history table."""
    c = conn.cursor()
    c.execute("""
        SELECT report_date, ticker, surprise_pct, direction, sector, entry_price
        FROM pead_signals
        WHERE detected_date >= date('now', ?)
        ORDER BY report_date DESC, detected_date DESC
    """, (f'-{days} days',))
    return c.fetchall()

def log_scan(conn, target_date, reports_found, signals_found, new_signals, email_sent, errors=''):
    """Log scan metadata."""
    c = conn.cursor()
    c.execute("""
        INSERT INTO scan_log
        (scan_date, target_date, reports_found, signals_found, new_signals, email_sent, errors)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().strftime('%Y-%m-%d %H:%M'),
        target_date, reports_found, signals_found, new_signals,
        1 if email_sent else 0, errors
    ))
    conn.commit()

# ============================================================
# MAIN SCAN LOGIC
# ============================================================

def run_scan(backfill_days=None, dry_run=False):
    """
    Main scan loop.
    Targets reports from exactly 2 trading days ago (matches backtest entry_delay=2).
    backfill_days: if set, scan the last N calendar days (dedup prevents re-emailing).
    """
    print(f"{'='*60}")
    print(f"PEAD SCANNER -- {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    conn = init_db()

    # Determine target date
    if backfill_days:
        target_date  = None  # scan all within window
        from_date_dt = datetime.utcnow().date() - timedelta(days=backfill_days)
        to_date_dt   = datetime.utcnow().date() - timedelta(days=1)
        print(f'Backfill mode: scanning {from_date_dt} to {to_date_dt}')
    else:
        target_date  = n_trading_days_ago(2)
        from_date_dt = target_date - timedelta(days=1)   # 1-day buffer
        to_date_dt   = target_date
        print(f'Target report date: {target_date}  (2 trading days ago)')

    from_str, to_str = date_range_str(from_date_dt, to_date_dt)
    print(f'Fetching calendar: {from_str} to {to_str}')

    # Fetch calendar
    calendar = get_earning_calendar(from_str, to_str)
    print(f'Calendar entries with actuals: {len(calendar)}')

    if not calendar:
        print('No earnings data found. Sending status email.')
        recent = get_recent_signals(conn)
        subj = f'PEAD Scanner -- No earnings data ({to_str})'
        html = build_email_html([], recent)
        if not dry_run:
            send_email(subj, html)
        log_scan(conn, str(target_date), 0, 0, 0, not dry_run)
        conn.close()
        return

    # Filter to target date in normal mode
    if target_date and not backfill_days:
        target_str = target_date.strftime('%Y-%m-%d')
        calendar = [e for e in calendar if e['report_date'] == target_str]
        print(f'Entries matching target date {target_str}: {len(calendar)}')

    # Detect signals
    print('Detecting BULL/BEAR signals...')
    signals = detect_signals(calendar)
    print(f'Signals found: {len(signals)}')

    if not signals:
        print('No qualifying signals. Sending status email.')
        recent = get_recent_signals(conn)
        subj = f'PEAD Scanner -- {len(calendar)} reports checked, no signals ({to_str})'
        html = build_email_html([], recent)
        if not dry_run:
            send_email(subj, html)
        log_scan(conn, str(target_date), len(calendar), 0, 0, not dry_run)
        conn.close()
        return

    # Dedup: only process NEW signals
    new_signals = filter_new_signals(conn, signals)
    print(f'New signals (not previously seen): {len(new_signals)}')

    if not new_signals:
        print('All signals already emailed. Sending status email.')
        recent = get_recent_signals(conn)
        subj = f'PEAD Scanner -- {len(signals)} signal(s) (all previously sent) ({to_str})'
        html = build_email_html([], recent)
        if not dry_run:
            send_email(subj, html)
        log_scan(conn, str(target_date), len(calendar), len(signals), 0, not dry_run)
        conn.close()
        return

    # Enrich: add price, sector, company name
    print(f'Enriching {len(new_signals)} new signal(s)...')
    enriched = []
    for s in new_signals:
        try:
            s = enrich_signal(s)
        except Exception as e:
            print(f'  WARNING: enrichment failed for {s["ticker"]}: {e}')
        enriched.append(s)

    # Store in DB
    stored_ids = []
    for s in enriched:
        if store_signal(conn, s):
            # Retrieve the new row ID
            c = conn.cursor()
            c.execute('SELECT id FROM pead_signals WHERE ticker=? AND report_date=?',
                      (s['ticker'], s['report_date']))
            row = c.fetchone()
            if row:
                stored_ids.append(row[0])

    # Build and send email
    print(f'Building email for {len(enriched)} signal(s)...')
    recent = get_recent_signals(conn)
    subject = build_email_subject(enriched)
    html    = build_email_html(enriched, recent)

    email_sent = False
    if dry_run:
        print(f'  DRY RUN: email skipped. Subject would be: {subject}')
    else:
        print(f'  Subject: {subject}')
        email_sent = send_email(subject, html)
        if email_sent:
            mark_emailed(conn, stored_ids)

    # Log scan
    log_scan(conn, str(target_date), len(calendar), len(signals), len(new_signals), email_sent)

    # Print summary
    bulls = [s for s in enriched if s['direction'] == 'BULL']
    bears = [s for s in enriched if s['direction'] == 'BEAR']
    print(f"{'='*60}")
    print(f'SCAN COMPLETE: {len(enriched)} new signal(s) -- BULL:{len(bulls)} BEAR:{len(bears)}')
    for s in enriched:
        price_str = f"${s['entry_price']:.2f}" if s.get('entry_price') else 'N/A'
        print(f"  {s['direction']:4s}  {s['ticker']:<6s}  surprise:{s['surprise_pct']:+.1f}%  price:{price_str}  sector:{s.get('sector','?')}")
    print(f"{'='*60}")

    conn.close()

# ============================================================
# CLI
# ============================================================

def show_status():
    """Print DB statistics."""
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM pead_signals')
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM pead_signals WHERE direction='BULL'")
    bulls = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM pead_signals WHERE direction='BEAR'")
    bears = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM scan_log')
    scans = c.fetchone()[0]
    c.execute('SELECT * FROM scan_log ORDER BY id DESC LIMIT 5')
    recent_scans = c.fetchall()
    c.execute("""
        SELECT report_date, ticker, surprise_pct, direction, entry_price
        FROM pead_signals ORDER BY detected_date DESC LIMIT 10
    """)
    recent_sigs = c.fetchall()
    print(f"{'='*50}")
    print('PEAD SCANNER STATUS')
    print(f"{'='*50}")
    print(f'Total signals:  {total}  (BULL:{bulls}  BEAR:{bears})')
    print(f'Total scans:    {scans}')
    if recent_scans:
        print('Last 5 scans:')
        for s in recent_scans:
            print(f'  {s[1]} | target:{s[2]} | reports:{s[3]} | signals:{s[4]} | new:{s[5]} | emailed:{s[6]}')
    if recent_sigs:
        print('Recent signals:')
        for r in recent_sigs:
            price_str = f'${r[4]:.2f}' if r[4] else 'N/A'
            print(f'  {r[0]}  {r[1]:<6s}  {r[3]:4s}  {r[2]:+.1f}%  {price_str}')
    conn.close()

def send_test_email():
    """Send test email to verify configuration."""
    html = f"""
    <html><body style='font-family:Arial; background:#1a1a2e; color:#e0e0e0; padding:20px;'>
      <h1 style='color:#00e676;'>PEAD Scanner -- Test Email</h1>
      <p>Configuration is working correctly.</p>
      <ul>
        <li>FMP API Key: {'Set' if config.FMP_API_KEY != 'YOUR_FMP_API_KEY_HERE' else 'NOT SET'}</li>
        <li>Beat threshold: {BEAT_THRESHOLD}%</li>
        <li>Miss threshold: {MISS_THRESHOLD}%</li>
        <li>Q4 excluded months: {sorted(Q4_MONTHS)}</li>
        <li>BEAR signals enabled: {ENABLE_BEAR}</li>
      </ul>
      <p>IB AutoTrader email subjects:<br>
         BULL: 'PEAD BULL: TICK1, TICK2'<br>
         BEAR: 'PEAD BEAR: TICK1, TICK2'<br>
         Both: 'PEAD BULL: TICK1 | BEAR: TICK2'
      </p>
      <p style='color:#666;'>Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
    </body></html>
    """
    send_email('PEAD Scanner -- Test Email', html)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PEAD Live Signal Scanner')
    parser.add_argument('--test-email', action='store_true', help='Send test email')
    parser.add_argument('--status',     action='store_true', help='Show DB stats')
    parser.add_argument('--backfill',   type=int, metavar='N', help='Scan last N calendar days')
    parser.add_argument('--dry-run',    action='store_true', help='Detect signals, skip email')
    args = parser.parse_args()

    if args.test_email:
        print('Sending test email...')
        send_test_email()
    elif args.status:
        show_status()
    elif args.backfill:
        run_scan(backfill_days=args.backfill, dry_run=args.dry_run)
    else:
        run_scan(dry_run=args.dry_run)

