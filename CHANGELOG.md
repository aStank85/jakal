# Changelog

All notable changes to Jakal are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - In Progress

### Added
- Stack management foundations (`named`, `quick`, `tagged` stack modes).
- Team composition analysis flow with role-distribution and carry-dependency signals.
- 5v5 matchup analysis scaffolding for stack-vs-stack comparison.

### Changed
- Project roadmap/status aligned to Stage 0 timeline.
- Version line advanced from `0.3.x` to `0.4.0` (active development milestone).

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
