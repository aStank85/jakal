# Changelog

All notable changes to Jakal are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-02-15

### Added
- Web scraper integration via Playwright for automated player sync.
- Auto-sync for season stats, map stats, operator stats, and match history.
- Four new scraped-data tables:
  - `map_stats`
  - `operator_stats`
  - `match_history`
  - `match_players`
- New menu flows:
  - Sync single player
  - Sync all players
  - Manual paste as fallback path
- Anti-bot aware navigation flow:
  - domcontentloaded navigation
  - modal dismissal
  - drawer close before tab changes
  - staged waits/rate limiting
  - match-history pagination/scroll loading
- New scraper exceptions:
  - `ScraperBlockedError`
  - `PlayerNotFoundError`

### Changed
- Main workflow now prioritizes auto-scrape sync over manual copy/paste.
- Snapshot ingest path now supports saving scraped section data alongside computed metrics.

## [0.4.1] - 2026-02-15

### Added
- 5v5 matchup analysis enhancements and strategic output tuning.

## [0.4.0] - 2026-02-15

### Added
- Stack management foundations (`named`, `quick`, `tagged` stack modes).
- Team composition analysis flow with role-distribution and carry-dependency signals.
- 5v5 matchup analysis baseline for stack-vs-stack comparison.

### Changed
- Project roadmap/status aligned to Stage 0 timeline.
- Version line advanced from `0.3.x` to `0.4.0` (stack analysis release milestone).

## [0.3.0] - 2026-02-13

### Added
- Rule-based insight generation engine.
- Thresholds module for reliability and guardrails.
- Data-quality flags and time reliability gate integration.
- Analyzer + UI integration for snapshot-level recommendations.

## [0.2.0] - 2026-02-12

### Added
- SQLite persistence (`players`, `stats_snapshots`, `computed_metrics`).
- Player management and snapshot history support.
- Core computed metrics and role classification outputs.

## [0.1.1] - 2024-02-12

### Added
- Parser MVP for copy/paste R6 Tracker season drawer data.
- Parse/validate flow across all primary stat sections.
- Terminal output formatting for quick verification.
