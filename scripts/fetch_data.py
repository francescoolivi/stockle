#!/usr/bin/env python3
"""
Stockle data pipeline
─────────────────────
Reads  : data/italy_static.json  and  data/usa_static.json
         (stable fields: ticker, name, market, sector, ipo, revenue)
Fetches: mktCap and ytd via yfinance (free, no API key needed)
Writes : data/italy.json  and  data/usa.json  (consumed by the frontend)

Run manually:   python scripts/fetch_data.py
GitHub Actions: triggered automatically via .github/workflows/update-data.yml
"""

import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    sys.exit("Missing dependency: pip install yfinance")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stockle")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
DATA_DIR    = ROOT / "data"

MARKETS = {
    "italy": {
        "static_file": DATA_DIR / "italy_static.json",
        "out_file":    DATA_DIR / "italy.json",
        # yfinance suffixes for Borsa Italiana
        "yf_suffix":  ".MI",
    },
    "usa": {
        "static_file": DATA_DIR / "usa_static.json",
        "out_file":    DATA_DIR / "usa.json",
        "yf_suffix":  "",
    },
}

# ── YTD helper ────────────────────────────────────────────────────────────────
def _ytd_pct(hist) -> float | None:
    """Return YTD % return rounded to 1 decimal, or None on failure."""
    today = date.today()
    year_start = date(today.year, 1, 1)
    # Filter to this year
    df = hist[hist.index.date >= year_start]
    if df.empty or len(df) < 2:
        return None
    first_close = float(df["Close"].iloc[0])
    last_close  = float(df["Close"].iloc[-1])
    if first_close == 0:
        return None
    return round((last_close - first_close) / first_close * 100, 1)


def _mkt_cap_bn(info: dict) -> float | None:
    """Return market cap in billions (rounded to 1 decimal), or None."""
    raw = info.get("marketCap")
    if raw is None:
        return None
    return round(raw / 1e9, 1)


# ── Core fetch ────────────────────────────────────────────────────────────────
def fetch_dynamic(ticker_bare: str, suffix: str) -> dict:
    """
    Returns {"mktCap": float|None, "ytd": float|None}
    Uses yfinance Ticker object.
    """
    sym = ticker_bare + suffix
    try:
        t    = yf.Ticker(sym)
        info = t.info or {}

        mkt_cap = _mkt_cap_bn(info)

        hist = t.history(period="ytd", auto_adjust=True)
        ytd  = _ytd_pct(hist)

        log.info(f"  {sym:<12}  mktCap={mkt_cap}B  ytd={ytd}%")
        return {"mktCap": mkt_cap, "ytd": ytd}

    except Exception as exc:
        log.warning(f"  {sym:<12}  ERROR: {exc}")
        return {"mktCap": None, "ytd": None}


# ── Pipeline ──────────────────────────────────────────────────────────────────
def process_market(market_key: str, cfg: dict):
    log.info(f"\n{'═'*50}")
    log.info(f"Processing market: {market_key.upper()}")
    log.info(f"{'═'*50}")

    static_path = cfg["static_file"]
    out_path    = cfg["out_file"]
    suffix      = cfg["yf_suffix"]

    if not static_path.exists():
        log.error(f"Static file not found: {static_path}")
        return

    with open(static_path, encoding="utf-8") as f:
        static_stocks = json.load(f)

    out_stocks = []
    for s in static_stocks:
        dynamic = fetch_dynamic(s["ticker"], suffix)

        merged = {**s}  # copy all static fields

        # Overwrite dynamic fields only if fetch succeeded
        if dynamic["mktCap"] is not None:
            merged["mktCap"] = dynamic["mktCap"]
        if dynamic["ytd"] is not None:
            merged["ytd"] = dynamic["ytd"]

        out_stocks.append(merged)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "stocks": out_stocks,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    log.info(f"\nWrote {len(out_stocks)} stocks → {out_path}")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Stockle data pipeline")
    parser.add_argument(
        "--market", choices=list(MARKETS.keys()), default=None,
        help="Process only one market (default: all)"
    )
    args = parser.parse_args()

    keys = [args.market] if args.market else list(MARKETS.keys())
    for k in keys:
        process_market(k, MARKETS[k])

    log.info("\nDone.")


if __name__ == "__main__":
    main()
