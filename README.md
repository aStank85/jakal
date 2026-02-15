# Jakal - Rainbow Six Siege Analytics Engine

Jakal is a terminal-based analytics platform for Rainbow Six Siege. v0.5.0 introduces automated data sync via Playwright scraping while keeping manual paste as fallback.

## Current Status

- Current version: `v0.5.0` (released)
- Next milestone: `v0.5.1` (short-term plateau detector)
- Completed milestones:
  - `v0.1.1` Parser MVP
  - `v0.2.0` Database Integration
  - `v0.3.0` Insight Generation
  - `v0.4.0` Stack Analysis
  - `v0.4.1` 5v5 Matchup Analysis
  - `v0.5.0` Auto-Scraper Integration

## Core Features

- Auto-sync player data from R6 Tracker (season, maps, operators, match history)
- Copy/paste manual fallback workflow
- SQLite persistence for snapshots, computed metrics, and scraped datasets
- Rule-based insight generation with thresholds and quality guards
- Stack management and 5v5 matchup analysis

## Requirements

- Python 3.9+
- Runtime dependencies in `requirements.txt`

## Install

```bash
pip install -r requirements.txt
pip install playwright beautifulsoup4
playwright install chromium
```

## Run

```bash
python main.py
```

## CLI Workflow

### Sync Player (Auto-Scrape)

1. Choose option `1`.
2. Enter username when prompted.
3. Jakal scrapes and saves:
   - Season stats
   - Map stats
   - Operator stats
   - Match history
4. Metrics and insights are recalculated from the saved snapshot.

### Add Stats Manually (Fallback)

1. Choose option `2`.
2. Paste drawer stats and type `END`.
3. Enter metadata.
4. Snapshot + metrics are saved.

### Sync All Players

- Choose option `3`.
- Jakal syncs each stored player sequentially with rate limiting.

## Roadmap Snapshot

- `v0.5.0`: Complete (web scraper + auto-sync)
- `v0.5.1`: Next (short-term plateau detector)
- `v0.5.2`: Player evolution plugin set
- `v1.0.0`: Stage 0 complete

## One-Sentence Direction

JAKAL starts as a copy/paste stats parser, evolves into an automated analytics platform, and then becomes an AI-powered coaching engine for R6 players.
