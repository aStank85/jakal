# SESSION.md — Last updated: 2026-04-08

## What this project is
R6 Siege analytics — scrape tracker.gg, store in SQLite, generate insights
for competitive improvement. Web UI at localhost:8000.

## Current version: v0.5.1
Working on: Phase 2/3 refactor

## Last session
Completed Phase 1 cleanup and Phase 2 helper/cache extraction, plus the first big Phase 3 split.
- Removed _match_scan_package (duplicate)
- Moved loose scripts to scripts/
- Archived CODEX task files to docs/tasks/
- Extracted helpers from web/app.py into src/utils.py (88 lines removed)
- Extracted workspace cache helpers from web/app.py into src/cache.py
- Extracted WebSocket handlers into src/ws_handlers/network_scan.py and src/ws_handlers/match_scrape.py

## Known broken things
- None

## Next logical step
Phase 3: keep shrinking web/app.py by moving large compute and route blocks out.
Best next target: _compute_matchup_block into src/analytics/workspace.py, then workspace API handlers.
