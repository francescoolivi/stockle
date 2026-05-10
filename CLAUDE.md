# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stockle is a Wordle-style daily game where you guess a stock instead of a word. Players have 8 attempts to identify the target stock (USA or Italy market) by receiving directional clues on 7 attributes: Market, Mkt Cap, Sector, IPO, Revenue, Price, YTD%.

## Commands

**Update stock data:**
```bash
pip install playwright beautifulsoup4
playwright install chromium
python scripts/fetch_data.py                # all markets
python scripts/fetch_data.py --market usa   # USA only
python scripts/fetch_data.py --market italy # Italy only
```

**Deploy to GitHub Pages:**
```bash
./push.sh
```

There is no build step, no bundler, and no test suite. Open `index.html` directly in a browser to develop.

## Architecture

The entire frontend lives in a single file: `index.html` (~780 lines of HTML + CSS + JS). There is no framework, no transpilation, and no npm.

**Data flow:**
1. `scripts/fetch_data.py` reads `data/{market}_static.json` (stable stock metadata + `inv_slug`), scrapes live data from investing.com via Playwright, and writes `data/{market}.json` with an `updated_at` timestamp.
2. GitHub Actions (`.github/workflows/update-data.yml`) runs the pipeline Mon–Fri at 23:00 UTC and auto-commits changes back to the repo.
3. The browser fetches `data/{market}.json` at game load. GitHub Pages serves the repo root.

**Daily stock selection** uses a deterministic seed: `hash(today + market)` → index into the stock array. This is the mechanism ensuring every player gets the same stock each day without a server.

**Game state** is stored in `localStorage` per market per day (`stockle-{market}-{YYYY-MM-DD}`) and stats across sessions (`stockle-stats-{market}`).

**Stock data schema:**
```json
{
  "ticker": "AAPL",
  "name": "Apple",
  "market": "NASDAQ",
  "sector": "Technology",
  "ipo": 1980,
  "inv_slug": "apple-computer-inc",
  "revenue": 383.3,   // billions — scraped from financial summary page
  "mktCap": 3400,     // billions — scraped daily
  "price": 186.4,     // price per share — scraped daily
  "ytd": -8.2         // percent — scraped daily
}
```

**Scraping strategy (investing.com):** For each stock the pipeline loads two pages — `investing.com/equities/{inv_slug}` (price, mktCap, ytd) and `investing.com/equities/{inv_slug}-financial-summary` (revenue). Each page tries `__NEXT_DATA__` JSON extraction first (more stable), then falls back to DOM selectors. Static file values are kept as fallback if scraping fails for a field.

**Feedback logic:** Each cell is either exact match (green ✓/✗) or directional (amber ↑/↓ meaning the target is higher/lower than the guess). YTD% uses an exact match threshold of ±0.5%.

**7-cell grid layout:** CSS uses `repeat(12, 1fr)` with `nth-child` span rules — first 4 cells span 3 columns each (row 1: Market, Mkt Cap, Sector, IPO), last 3 cells span 4 columns each (row 2: Revenue, Price, YTD). Price displays with currency symbol ($ for USA, € for Italy) derived from `currentMarket`.

## Key Files

- `index.html` — entire frontend (screens, game logic, autocomplete, stats, countdown timer, FALLBACK data)
- `data/{market}_static.json` — stable stock metadata including `inv_slug`; edit here to add/remove stocks or fix a wrong slug
- `data/{market}.json` — generated; do not edit manually
- `scripts/fetch_data.py` — investing.com scraper (Playwright); reads static, writes dynamic
- `.github/workflows/update-data.yml` — scheduled data refresh + auto-commit

## Fixing a wrong inv_slug

If a stock fails to scrape (logged as `price=None`), find the correct slug by visiting `investing.com/equities/{slug}` manually, then update `inv_slug` in the relevant `data/{market}_static.json`.
