#!/usr/bin/env python3
"""
Stockle data pipeline — investing.com scraper
─────────────────────────────────────────────
Reads  : data/{market}_static.json
         (stable fields + inv_slug for investing.com URL)
Fetches: price, mktCap, ytd, revenue from investing.com via Playwright
Writes : data/{market}.json  (consumed by the frontend)

Run manually:
  pip install playwright beautifulsoup4
  playwright install chromium
  python scripts/fetch_data.py [--market usa|italy]
"""

import argparse
import json
import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    import sys
    sys.exit("Missing dependency: pip install playwright && playwright install chromium")

# ── Config ─────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

MARKETS = {
    "italy": {
        "static_file": DATA_DIR / "italy_static.json",
        "out_file":    DATA_DIR / "italy.json",
    },
    "usa": {
        "static_file": DATA_DIR / "usa_static.json",
        "out_file":    DATA_DIR / "usa.json",
    },
}

BASE_URL = "https://www.investing.com/equities"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stockle")


# ── Helpers ────────────────────────────────────────────────────────────────
def parse_number(text: str) -> float | None:
    """Parse strings like '1.23T', '186.4B', '$186.40', '-8.2%' → float (in billions for cap/revenue)."""
    if not text:
        return None
    text = str(text).strip().replace(",", "").replace("\xa0", "")
    text = re.sub(r"[$€£¥]", "", text).strip()
    match = re.match(r"^(-?\d+\.?\d*)\s*([TBMKk])?%?$", text)
    if not match:
        return None
    val = float(match.group(1))
    suffix = (match.group(2) or "").upper()
    if suffix == "T":
        val *= 1_000      # → billions
    elif suffix == "M":
        val /= 1_000      # → billions
    elif suffix == "K":
        val /= 1_000_000  # → billions
    return round(val, 2)


def to_billions(raw) -> float | None:
    """Convert a raw numeric value (possibly absolute) to billions."""
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if val > 1e9:
        val = val / 1e9   # absolute → billions
    return round(val, 1)


def deep_search(obj, keys: list[str], max_depth: int = 8, _depth: int = 0):
    """Recursively find the first matching key in a nested dict/list."""
    if _depth > max_depth or obj is None:
        return None
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
        for v in obj.values():
            found = deep_search(v, keys, max_depth, _depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj[:30]:
            found = deep_search(item, keys, max_depth, _depth + 1)
            if found is not None:
                return found
    return None


def get_next_data(page) -> dict:
    """Extract __NEXT_DATA__ JSON embedded by Next.js."""
    try:
        raw = page.evaluate(
            "() => { const e = document.getElementById('__NEXT_DATA__'); return e ? e.textContent : null; }"
        )
        if raw:
            return json.loads(raw)
    except Exception as e:
        log.debug(f"__NEXT_DATA__ error: {e}")
    return {}


# ── Scraping ────────────────────────────────────────────────────────────────
def scrape_overview(page, slug: str) -> dict:
    """Fetch price, mktCap, ytd from the stock overview page."""
    url = f"{BASE_URL}/{slug}"
    result = {}

    for attempt in range(3):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(random.randint(1_800, 2_800))
            break
        except PlaywrightTimeout:
            if attempt == 2:
                log.warning(f"  Timeout {url}")
                return {}
            time.sleep(3)

    # ── Strategy 1: __NEXT_DATA__ ──────────────────────────────────────────
    nd = get_next_data(page)
    if nd:
        price_raw = deep_search(nd, ["last", "price", "currentPrice", "closePrice", "lastPrice", "bid"])
        if price_raw is not None:
            try:
                result["price"] = round(float(price_raw), 2)
            except (ValueError, TypeError):
                pass

        mktcap_raw = deep_search(nd, ["marketCap", "mktCap", "market_cap", "marketCapitalization"])
        val = to_billions(mktcap_raw)
        if val:
            result["mktCap"] = val

        ytd_raw = deep_search(nd, ["ytd", "ytdChange", "ytdReturn", "yearToDateChange", "changeYtd", "ytdPercent"])
        if ytd_raw is not None:
            try:
                result["ytd"] = round(float(ytd_raw), 1)
            except (ValueError, TypeError):
                pass

    # ── Strategy 2: DOM selectors ──────────────────────────────────────────
    if "price" not in result:
        for sel in [
            '[data-test="instrument-price-last"]',
            ".last-price-value",
            '[class*="lastPrice"]',
            '[class*="last-price"]',
            '[class*="text-5xl"]',
        ]:
            try:
                el = page.locator(sel).first
                if el.count() and el.is_visible(timeout=1_500):
                    val = parse_number(el.inner_text())
                    if val and val > 0:
                        result["price"] = val
                        break
            except Exception:
                continue

    if "mktCap" not in result:
        try:
            text = page.evaluate("""
                () => {
                    for (const el of document.querySelectorAll('dt,th,span,[class*="label"],[class*="key"]')) {
                        if (/^market\s*cap/i.test(el.textContent.trim())) {
                            const next = el.nextElementSibling
                                      || el.closest('tr')?.querySelector('td:last-child');
                            return next?.textContent.trim() ?? null;
                        }
                    }
                    return null;
                }
            """)
            val = parse_number(text)
            if val:
                result["mktCap"] = val
        except Exception:
            pass

    if "ytd" not in result:
        try:
            text = page.evaluate("""
                () => {
                    for (const el of document.querySelectorAll('dt,th,span,[class*="label"]')) {
                        if (/^ytd/i.test(el.textContent.trim())) {
                            const next = el.nextElementSibling
                                      || el.closest('tr')?.querySelector('td:last-child');
                            return next?.textContent.trim() ?? null;
                        }
                    }
                    return null;
                }
            """)
            if text:
                val = parse_number(text.replace("%", ""))
                if val is not None:
                    result["ytd"] = val
        except Exception:
            pass

    log.info(
        f"  overview   {slug:<36}  price={result.get('price')}  "
        f"mktCap={result.get('mktCap')}B  ytd={result.get('ytd')}%"
    )
    return result


def scrape_financials(page, slug: str) -> dict:
    """Fetch annual revenue from the financial summary page."""
    url = f"{BASE_URL}/{slug}-financial-summary"
    result = {}

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(random.randint(1_500, 2_200))
    except PlaywrightTimeout:
        log.warning(f"  Timeout {url}")
        return {}

    # ── Strategy 1: __NEXT_DATA__ ──────────────────────────────────────────
    nd = get_next_data(page)
    if nd:
        rev_raw = deep_search(nd, ["revenue", "totalRevenue", "netRevenue", "sales", "totalSales"])
        val = to_billions(rev_raw)
        if val:
            result["revenue"] = val

    # ── Strategy 2: revenue row in financial table ─────────────────────────
    if "revenue" not in result:
        try:
            text = page.evaluate("""
                () => {
                    for (const row of document.querySelectorAll('tr')) {
                        const cells = [...row.querySelectorAll('td,th')];
                        if (cells.length >= 2) {
                            const label = cells[0].textContent.trim().toLowerCase();
                            if (['total revenue','revenue','net revenue','total revenues'].includes(label)) {
                                // cells[1] = most recent year
                                return cells[1].textContent.trim();
                            }
                        }
                    }
                    return null;
                }
            """)
            val = parse_number(text)
            if val:
                result["revenue"] = val
        except Exception:
            pass

    log.info(f"  financials {slug:<36}  revenue={result.get('revenue')}B")
    return result


# ── Pipeline ───────────────────────────────────────────────────────────────
def process_market(market_key: str, cfg: dict, browser):
    log.info(f"\n{'═' * 54}")
    log.info(f"  Market: {market_key.upper()}")
    log.info(f"{'═' * 54}")

    with open(cfg["static_file"], encoding="utf-8") as f:
        stocks = json.load(f)

    context = browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="America/New_York",
    )
    page = context.new_page()

    out_stocks = []
    for s in stocks:
        slug = s.get("inv_slug")
        if not slug:
            log.warning(f"  No inv_slug for {s['ticker']}, keeping static values")
            out_stocks.append(s)
            continue

        log.info(f"\n── {s['ticker']} ({slug})")
        merged = dict(s)

        overview = scrape_overview(page, slug)
        merged.update({k: v for k, v in overview.items() if v is not None})

        time.sleep(random.uniform(1.5, 2.5))

        fin = scrape_financials(page, slug)
        merged.update({k: v for k, v in fin.items() if v is not None})

        time.sleep(random.uniform(1.0, 2.0))
        out_stocks.append(merged)

    context.close()

    payload = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "stocks": out_stocks,
    }
    cfg["out_file"].parent.mkdir(parents=True, exist_ok=True)
    with open(cfg["out_file"], "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    log.info(f"\n  Wrote {len(out_stocks)} stocks → {cfg['out_file']}")


def main():
    parser = argparse.ArgumentParser(description="Stockle pipeline — investing.com scraper")
    parser.add_argument("--market", choices=list(MARKETS.keys()), default=None,
                        help="Process only one market (default: all)")
    args = parser.parse_args()
    keys = [args.market] if args.market else list(MARKETS.keys())

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        try:
            for k in keys:
                process_market(k, MARKETS[k], browser)
        finally:
            browser.close()

    log.info("\nDone.")


if __name__ == "__main__":
    main()
