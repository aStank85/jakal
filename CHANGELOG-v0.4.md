# Changelog

## v0.3.5.0 - Stack Analysis

### Added
- Stack Management - Create, manage, and organize player stacks (named, quick, tagged)
- Team Analysis - Full 5-stack analysis with role distribution, composition scoring, carry dependency, clutch hierarchy, entry stats, and team insights
- 5v5 Matchup Analysis - Head-to-head stack comparison with category breakdowns, role matchups, outcome prediction, strategic recommendations, and key battlegrounds
- Player Details - View individual player stats and role classification (menu option 4)
- New database tables: stacks, stack_members, stack_analyses, matchup_analyses
- stack_manager.py - Stack CRUD and three stack modes (named, quick, tagged)
- team_analyzer.py - Team-level analysis engine with insight generation
- matchup_analyzer.py - 5v5 comparison engine with prediction and strategy
- add_computed_metrics() method in database.py
- Comprehensive test suite with 20+ tests per module
- Integration tests covering full stack creation, analysis, and matchup flows
- Updated main menu (8 options) with Stack Management submenu

### Changed
- Menu expanded from 5 to 8 options with stack section
- Exit now cleans up temporary (quick) stacks
- UI updated with team analysis display, matchup display, role matchup tables

## v0.3.0 - Insight Generation

### Added
- Player metrics calculator with 6-role classification system
- Player comparison across 10 stat categories
- Role identification: Fragger, Entry, Support, Anchor, Clutch, Carry

## v0.2.0 - Database

### Added
- SQLite database for player and snapshot storage
- Stats snapshot persistence

## v0.1.0 - Parser

### Added
- R6 Tracker stats parser
- Basic terminal UI
