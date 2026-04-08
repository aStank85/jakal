# SESSION.md — Last updated: 2026-04-08

## What this project is
R6 Siege analytics — scrape tracker.gg, store in SQLite, generate insights
for competitive improvement. Web UI at localhost:8000.

## Current version: v0.5.1
Working on: Phase 1 cleanup, then Phase 2 refactor

## Last session
Completed Phase 1 cleanup — removed duplicate _match_scan_package,
moved loose scripts to scripts/, archived CODEX task files to docs/tasks/

## Known broken things
- None currently

## Next logical step
Phase 2: Extract helper functions from web/app.py into src/utils.py
Start with _parse_iso_datetime, _wilson_ci, _pctile_abs_bound
