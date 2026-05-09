# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stockle is a Wordle-style daily stock guessing game. Players have 8 attempts to identify a target stock (USA or Italy market) by receiving directional clues on 6 attributes: Market, Market Cap, Sector, IPO year, Revenue, and YTD%.

## Commands

**Update stock data (requires `pip install yfinance`):**
```bash
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

The entire frontend lives in a single file: `index.html` (~768 lines of HTML + CSS + JS). There is no framework, no transpilation, and no npm.

**Data flow:**
1. `scripts/fetch_data.py` reads `data/{market}_static.json` (stable stock metadata), fetches live `mktCap` and `ytd` via yfinance, and writes `data/{market}.json` with an `updated_at` timestamp.
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
  "revenue": 383.3,   // billions, static
  "mktCap": 3400,     // billions, updated daily
  "ytd": -8.2         // percent, updated daily
}
```

**Feedback logic:** Each cell is either exact match (green ✓/✗) or directional (amber ↑/↓ meaning the target is higher/lower than the guess). YTD% uses an exact match threshold of ±0.5%.

## Key Files

- `index.html` — entire frontend (screens, game logic, autocomplete, stats, countdown timer)
- `data/{market}_static.json` — stable stock metadata; edit here to add/remove stocks
- `data/{market}.json` — generated; do not edit manually
- `scripts/fetch_data.py` — data pipeline; reads static, writes dynamic
- `.github/workflows/update-data.yml` — scheduled data refresh + auto-commit
