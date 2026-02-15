# Jakal - Rainbow Six Siege Analytics Engine

Jakal is a terminal-based analytics platform for Rainbow Six Siege. It started as a copy/paste parser and is now in Stage 0 expansion with stack analysis completed.

## Current Status

- Current version: `v0.4.0` (released)
- Active milestone: `v0.4.1` (5v5 matchup analysis)
- Completed milestones:
  - `v0.1.1` Parser MVP
  - `v0.2.0` Database Integration
  - `v0.3.0` Insight Generation
- Stage target: `v1.0.0` Stage 0 complete (full copy/paste analytics platform)

## Core Features Available Now

- Copy/paste import from R6 Tracker season drawer
- SQLite persistence for players, snapshots, and computed metrics
- Derived metrics for combat, clutch, entry, teamplay, and impact
- Rule-based insight generation with thresholds and quality guards
- Stack management (named, quick, tagged)
- Team composition analysis
- 5v5 stack matchup analysis

## Requirements

- Python 3.9+
- No runtime dependencies outside Python standard library
- Optional for tests: `pytest`

## Run

```bash
python main.py
```

## CLI Workflow

### Add New Stats Snapshot (Manual Paste)

1. Open [R6 Tracker](https://r6.tracker.network/) and copy season drawer stats.
2. In Jakal, choose `1`.
3. Paste stats and type `END`.
4. Enter metadata:
   - Username
   - Device tag (`pc`, `xbox`, `playstation`)
   - Season (default shown by app)
5. Snapshot date/time are auto-recorded.

### View and Analyze

- Option `2`: list all players
- Option `3`: compare players
- Option `4`: view detailed player snapshot + metrics + insights
- Option `5`: stack management
- Option `6`: stack analysis
- Option `7`: 5v5 stack matchup analysis

## Project Structure

```text
jakal/
├── src/
│   ├── parser.py
│   ├── database.py
│   ├── calculator.py
│   ├── comparator.py
│   ├── analyzer.py
│   ├── stack_manager.py
│   ├── team_analyzer.py
│   ├── matchup_analyzer.py
│   └── ui.py
├── tests/
├── docs/
├── data/
├── main.py
└── CHANGELOG.md
```

## Roadmap Snapshot

### Stage 0 - Foundation

- `v0.1.1` Parser MVP (complete)
- `v0.2.0` Database Integration (complete)
- `v0.3.0` Insight Generation (complete)
- `v0.4.0` Stack Analysis (complete)
- `v0.4.1` 5v5 Matchup Analysis (in progress)
- `v0.4.2` Map Data Integration (V2 plugins)
- `v0.5.0` Trajectory Analysis
- `v0.5.1` Short Term Plateau Detector
- `v0.5.2` Player Evolution Plugins
- `v1.0.0` Stage 0 complete

### Stage 1 - Automation

- `v1.1.0` Web Scraper
- `v1.2.0` Match Overview Integration (V3 plugins)
- `v1.3.0` Auto-Update System
- `v1.4.0` Profile Routing
- `v2.0.0` Stage 1 complete

### Stage 2+

- `v2.x` Individual match depth (V4 plugins)
- `v3.x` Round-level analysis (V5 plugins + replay)
- `v4.x` AI engine and first ML models
- `v5.x` Platformization (desktop/web/API)
- `v6.0.0` Full platform stage complete

## One-Sentence Direction

JAKAL starts as a copy/paste stats parser, evolves into an automated analytics platform, and then becomes an AI-powered coaching engine for R6 players.
