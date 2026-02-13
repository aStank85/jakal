# Changelog

All notable changes to Jakal will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
