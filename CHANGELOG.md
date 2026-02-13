# Changelog

All notable changes to Jakal will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2] - 2026-02-13

### Added
- **Metrics Expansion Pack (v1)** across calculator outputs:
  - Combat involvement and efficiency (`engagement_rate`, `wcontrib_per_round`, `net_kills_per_round`, `assist_to_kill`, `frag_share`)
  - Opening duel detail (`first_blood_rate`, `first_death_rate`, `opening_net_per_round`, `opening_kill_share`)
  - Clutch depth (`clutch_attempts_per_100`, `clutch_choke_rate`, `clutch_1v2..1v5_success`, `high_pressure_success`, `disadv_attempt_share`, `avg_clutch_difficulty`, dropoff chain)
  - Survival/risk (`survival_rate`, `risk_index`)
  - Time-normalized productivity (`rounds_per_hour`, kills/deaths/assists/clutch per hour)
  - Discipline and confidence (`tk_per_kill`, `clean_play_index`, `overall_conf`)
- **Insight Rule Expansion** in `src/analyzer.py`:
  - Opening control signal
  - High-pressure clutch signal
  - Risk profile signal
  - Clean-play signal
  - Confidence signal when `overall_conf` is available

### Changed
- Player details now recalculate metrics from latest snapshot directly to ensure newest formulas are always available.
- Teamkill severity remains banded by rate and is now complemented by clean-play scoring.

### Fixed
- Analyzer confidence insight no longer appears unless confidence metrics are present.
- Integration persistence test now validates schema-backed metric keys correctly when calculator output set grows.

## [0.3.1] - 2026-02-13

### Added
- **Insight Generation Engine**: New `src/analyzer.py` rule-based analyzer with deterministic output schema:
  - `severity`
  - `category`
  - `message`
  - `evidence`
  - `action`
- **Insight Rules**:
  - Sample-size caution
  - High K/D and low win conversion
  - Entry efficiency vs aggression mismatch
  - Clutch attempt/conversion quality
  - Teamplay and assist contribution flags
  - Teamkill discipline checks
  - Wins-per-hour efficiency signal
  - Impact vs match outcome signal
  - Baseline fallback insight when no flags trigger
- **Analyzer Test Suite**: Added `tests/test_analyzer.py` with rule and schema coverage.

### Changed
- Player details flow now generates and displays insights from latest snapshot + metrics.
- Snapshot insert flow now prints top generated insight after successful processing.

### Fixed
- Documentation version drift corrected for release line and roadmap status.

## [0.2.1] - 2026-02-13

### Added
- **Automatic Schema Migration**: Startup migration for legacy databases to keep local `jakal.db` files compatible with current code.
- **Device Classification**: Added `device_tag` (`pc`, `xbox`, `playstation`) to player records and CLI metadata flow.
- **Auto Timestamp Capture**: Snapshot date/time are now auto-captured during import (`YYYY-MM-DD`, `HH:MM:SS`).
- **Regression Coverage**:
  - Legacy schema auto-migration test
  - Backfilled snapshot metric binding test
  - Partial clutch payload safety test
  - Tie-handling comparison test

### Changed
- Player list and details views now show device classification.
- Snapshot insert flow now computes metrics from the exact inserted snapshot ID.
- Comparison winner logic now treats ties as no winner instead of biasing first player.

### Fixed
- Prevented metrics/snapshot mismatch when inserting historical backfilled snapshots.
- Prevented `KeyError` during metric calculation when clutch JSON is partial.
- Resolved `no such column: tag`-type runtime failures by auto-migrating older schemas.

## [0.2.0] - 2026-02-12

### Added
- **Database Integration**: Full SQLite database with three-table schema (players, stats_snapshots, computed_metrics)
- **Player Management**: Add, retrieve, update, and delete players with tags and notes
- **Snapshot Management**: Store and retrieve multiple stat snapshots per player across different dates and seasons
- **Computed Metrics Storage**: Persist all calculated metrics to database
- **Additional Metrics**:
  - overall_clutch_success
  - clutch_dropoff_rate
  - clutch_efficiency_score
  - impact_rating
  - wins_per_hour
  - kd_win_gap
- **View Player Details**: New menu option (4) to view comprehensive player stats and metrics
- **Database Methods**:
  - `get_player()` - Retrieve single player
  - `update_player_tag()` - Update player tags
  - `delete_player()` - Remove player and cascading snapshots
  - `get_all_snapshots()` - Get all snapshots for a player
  - `get_snapshot_by_id()` - Get specific snapshot
  - `delete_snapshot()` - Remove snapshot
  - `get_computed_metrics()` - Retrieve metrics for snapshot
  - `get_latest_metrics()` - Get metrics for player's most recent snapshot
  - `player_exists()` - Check if player exists
  - `snapshot_count()` - Count snapshots for player
  - `get_all_seasons()` - List all seasons in database
- **UI Enhancements**:
  - `show_player_details()` - Display comprehensive player information
  - `show_error()` and `show_success()` - Formatted messages
  - Player tags display in player lists
- **Comprehensive Test Suite**:
  - 20 database tests
  - 5 integration tests
  - All tests for new database functionality

### Changed
- Database schema now includes player tags and notes
- Database schema includes additional combat stats (kills_per_game, headshots_per_round)
- `get_all_players()` now returns list of dicts with full player information instead of just usernames
- `add_computed_metrics()` now requires player_id parameter
- Calculator handles None values gracefully with `_safe_get()` helper method
- Main application uses try/finally block to ensure database closes properly
- Player comparison retrieves metrics from database when available instead of always recalculating

### Fixed
- Database field mapping for rounds (rounds_played, rounds_wins, rounds_losses)
- Rank points correctly pulled from uncategorized section instead of ranked
- Database connection properly closed on application exit
- Indentation errors in database.py schema creation

## [0.1.1] - 2024-02-12

### Added
- Initial MVP release
- R6 Tracker stats parser
- Basic metrics calculation
- Role detection system
- Multi-player comparison
- Terminal UI

### Fixed
- Parser field mapping inconsistencies
- Input validation for user inputs
- Error handling in database operations

[0.2.0]: https://github.com/yourusername/jakal/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/yourusername/jakal/releases/tag/v0.1.1
[0.2.1]: https://github.com/yourusername/jakal/compare/v0.2.0...v0.2.1
[0.3.1]: https://github.com/yourusername/jakal/compare/v0.2.1...v0.3.1
[0.3.2]: https://github.com/yourusername/jakal/compare/v0.3.1...v0.3.2
