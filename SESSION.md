# SESSION.md — Last updated: 2026-04-08

## What this project is
R6 Siege analytics — scrape tracker.gg, store in SQLite, generate insights
for competitive improvement. Web UI at localhost:8000.

## Current version: v0.5.1
Working on: Phase 2/3 refactor

## Last session
Completed Phase 1 cleanup and Phase 2 helper extraction.
- Removed _match_scan_package (duplicate)
- Moved loose scripts to scripts/
- Archived CODEX task files to docs/tasks/
- Extracted helpers from web/app.py into src/utils.py (88 lines removed)

## Known broken things
- None

## Next logical step
Phase 2 continued: extract cache functions from web/app.py into src/cache.py
Functions: _workspace_sql_cache_get/set, _workspace_scope_cache_get/set,
_workspace_team_cache_get/set, _workspace_insights_cache_get/set,
_ensure_workspace_cache_tables
