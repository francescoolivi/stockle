# Stockle

A Wordle-style daily game where you guess a stock instead of a word.

**[Play at francescoolivi.github.io/stockle](https://francescoolivi.github.io/stockle)**

## How to play

Choose a market — **USA** (NYSE / NASDAQ) or **Italy** (FTSE MIB) — then guess today's stock in up to **8 attempts**. Type a ticker or company name; the autocomplete will help.

After each guess, six cells reveal how close you are:

| Cell | Match | Hint |
|------|-------|------|
| Market | ✓ same exchange | ✗ different |
| Mkt Cap | ✓ exact | ↑ target is larger · ↓ target is smaller |
| Sector | ✓ same sector | ✗ different |
| IPO | ✓ exact year | ↑ target is later · ↓ target is earlier |
| Revenue | ✓ exact | ↑ target is higher · ↓ target is lower |
| YTD % | ✓ exact | ↑ target is higher · ↓ target is lower |

A new stock is selected every day at midnight. Come back tomorrow for the next one.

## Stock pools

- **USA** — 35 large-cap stocks from NYSE and NASDAQ
- **Italy** — 39 stocks from the FTSE MIB
- **Europe** — coming soon
