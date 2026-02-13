<<<<<<< HEAD
# jakal
=======
# Jakal - Rainbow Six Siege Stats Analyzer

Jakal is a terminal-based stats analyzer for Rainbow Six Siege that parses R6 Tracker season drawer data, stores historical snapshots, computes derived metrics, and compares players over time.

## Current Status

- Current release: `v0.3.2` (database-backed CLI with metrics, comparison, and expanded insight generation)
- Next major target: `v0.4` export/reporting and scrape ingestion pipeline
- Test status: full suite passing (`76` tests)

## Core Features

- Copy-paste import from R6 Tracker season drawer
- SQLite persistence for players, snapshots, and computed metrics
- Derived metrics (entry/clutch/teamplay/impact/win-efficiency)
- Role classification (primary + secondary confidence)
- Multi-player side-by-side comparison with tie-safe winner handling
- Automated rule-based insight generation with evidence and recommended actions
- Device classification per player (`pc`, `xbox`, `playstation`) for future scraper targeting
- Automatic snapshot timestamp capture (`YYYY-MM-DD` + `HH:MM:SS`)
- Automatic additive schema migration for older local databases

## Requirements

- Python 3.7+
- No runtime dependencies outside Python standard library
- Optional for tests: `pytest`

## Run

```bash
python main.py
```

## CLI Workflow

### Add New Stats Snapshot

1. Open [R6 Tracker](https://r6.tracker.network/) and copy the season drawer stats.
2. In Jakal, choose `1`.
3. Paste stats and type `END`.
4. Enter:
   - Username
   - Device tag (`pc`, `xbox`, or `playstation`)
   - Season (default `Y10S4`)
5. Date/time are auto-recorded by the app.

### View / Compare

- Option `2`: list all players (includes device + tag)
- Option `3`: compare selected players
- Option `4`: detailed snapshot + metric breakdown + generated insights for latest snapshot

## Project Structure

```text
jakal-mvp/
├── src/
│   ├── parser.py
│   ├── database.py
│   ├── calculator.py
│   ├── comparator.py
│   ├── ui.py
│   └── analyzer.py
├── tests/
│   ├── test_parser.py
│   ├── test_calculator.py
│   ├── test_database.py
│   ├── test_integration.py
│   └── test_analyzer.py
├── docs/
│   ├── v0.1-notes.md
│   ├── v0.2-notes.md
│   └── v0.3-notes.md
├── data/
│   └── jakal.db
├── main.py
└── CHANGELOG.md
```

## Database Overview

### `players`

- `player_id`, `username`
- `device_tag` (`pc`/`xbox`/`playstation`)
- `tag`, `notes`, `created_at`

### `stats_snapshots`

- Snapshot metadata (`snapshot_date`, `snapshot_time`, `season`)
- Raw parsed stat fields
- `clutches_data` JSON payload

### `computed_metrics`

- Derived metrics for each snapshot
- Role outputs (`primary_role`, `secondary_role`, confidence scores)

## Testing

```bash
pytest -q -p no:cacheprovider
```

## Formula Reference

- Full variable and formula index: `docs/metrics-reference.md`

## Roadmap Snapshot

- `v0.4`: export/reporting features and saved insight reports
- `v0.5+`: web-scrape ingestion pipeline and profile sync workflows

## License

This project is for personal use. Rainbow Six Siege and R6 Tracker are properties of their respective owners.
>>>>>>> 7d94de4 (Initial Commit - JAKAL v0.2.0)
