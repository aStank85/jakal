# Jakal Metrics Reference

This document tracks all variables currently used in metric and insight calculations.

## Raw Snapshot Variables

These come from parsed/stored snapshot fields.

- `first_bloods`
- `first_deaths`
- `rounds_played`
- `assists`
- `kills`
- `deaths`
- `kd`
- `kills_per_round`
- `deaths_per_round`
- `assists_per_round`
- `match_win_pct`
- `wins`
- `time_played_hours`
- `teamkills`
- `clutches_data` (JSON)

## Clutches JSON Variables

Keys inside `clutches_data`:

- `total`
- `lost_total`
- `1v1`
- `lost_1v1`
- `1v2`
- `lost_1v2`
- `1v3`
- `lost_1v3`
- `1v4`
- `lost_1v4`
- `1v5`
- `lost_1v5`

## Derived Metric Formulas

All formulas are implemented in `src/calculator.py`.

- `entry_efficiency = first_bloods / (first_bloods + first_deaths)`
- `aggression_score = (first_bloods + first_deaths) / rounds_played`
- `clutch_attempt_rate = (total + lost_total) / rounds_played`
- `clutch_1v1_success = 1v1 / (1v1 + lost_1v1)`
- `clutch_disadvantaged_success = (1v2+1v3+1v4+1v5) / ((1v2+1v3+1v4+1v5) + (lost_1v2+lost_1v3+lost_1v4+lost_1v5))`
- `overall_clutch_success = total / (total + lost_total)`
- `clutch_dropoff_rate = clutch_1v1_success - clutch_1v2_success`
- `clutch_efficiency_score = (1v1*1) + (1v2*2) + (1v3*5) + (1v4*10) + (1v5*25)`
- `teamplay_index = assists / (assists + kills)`
- `fragger_score = (kd*30) + (kills_per_round*40) + ((first_bloods/rounds_played)*300)`
- `entry_score = (entry_efficiency*40) + (aggression_score*60)`
- `support_score = (assists_per_round*150) + (teamplay_index*50)`
- `anchor_score = (clutch_attempt_rate*40) + (clutch_1v1_success*40) + ((1 - deaths_per_round)*20)`
- `clutch_specialist_score = ((total/rounds_played)*100) + (clutch_1v1_success*30) + (clutch_disadvantaged_success*70)`
- `carry_score = (kd*25) + (match_win_pct*0.3) + (kills_per_round*30) + (clutch_1v1_success*20)`
- `impact_rating = ((kills*1.0) + (assists*0.5) + (total*2.0)) / rounds_played`
- `rounds_per_hour = rounds_played / time_played_hours`
- `time_played_unreliable = rounds_per_hour < MIN_RELIABLE_ROUNDS_PER_HOUR`
- `wins_per_hour = wins / time_played_hours` (suppressed to `None` when `time_played_unreliable`)
- `kd_win_gap = kd - (match_win_pct / 50)`
- `clean_play_index = 1.0 - min(1.0, (teamkills / rounds_played) / CLEAN_PLAY_NORMALIZATION_RATE)`
- `clutch_1v2_success = 1v2 / (1v2 + lost_1v2)`
- `clutch_1v3_success = 1v3 / (1v3 + lost_1v3)`
- `clutch_1v4_success = 1v4 / (1v4 + lost_1v4)`
- `clutch_1v5_success = 1v5 / (1v5 + lost_1v5)`
- `high_pressure_attempts = (1v3+lost_1v3) + (1v4+lost_1v4) + (1v5+lost_1v5)`
- `high_pressure_wins = 1v3 + 1v4 + 1v5`
- `disadv_attempt_share = ((1v2+lost_1v2)+(1v3+lost_1v3)+(1v4+lost_1v4)+(1v5+lost_1v5)) / clutch_attempts`
- `extreme_attempts = (1v4+lost_1v4) + (1v5+lost_1v5)`
- `clutch_totals_mismatch` / `clutch_lost_totals_mismatch` / `clutch_totals_unreliable` (data-quality flags)

Role outputs:

- `primary_role`, `primary_confidence`
- `secondary_role`, `secondary_confidence`

## Insight Variables and Thresholds

Insights are generated in `src/analyzer.py` from snapshot + derived metrics.

Primary inputs:

- Snapshot: `rounds_played`, `match_win_pct`, `kd`, `assists_per_round`, `teamkills`, `time_played_hours`, `clutches_data`
- Metrics: `entry_efficiency`, `aggression_score`, `clutch_attempt_rate`, `overall_clutch_success`, `teamplay_index`, `wins_per_hour`, `impact_rating`, `primary_role`

Current rule thresholds (centralized in `src/thresholds.py`):

- Small sample caution: `rounds_played < 120`
- High K/D but low win conversion: `kd >= 1.2` and `match_win_pct < 50.0`
- Low K/D but strong win rate: `kd < 0.9` and `match_win_pct >= 52.0`
- Weak opening conversion: `entry_efficiency < 0.45` and `aggression_score >= 0.20`
- Strong opening profile: `entry_efficiency >= 0.58` and `aggression_score >= 0.18`
- Clutch volume but low conversion: `clutch_attempt_rate >= 0.12` and `overall_clutch_success < 0.22`
- Strong clutch profile: `overall_clutch_success >= 0.30` and `(total + lost_total) >= 20`
- Low teamplay signal: `teamplay_index < 0.12` and `assists_per_round < 0.18`
- Teamkill severity bands:
  - `teamkills / rounds_played >= 0.02` -> `medium`
  - `teamkills / rounds_played >= 0.01` and `< 0.02` -> `low`
- Low efficiency volume: `time_played_hours >= 50` and `wins_per_hour < 0.12` (only when time is reliable)
- High-pressure clutch insight requires both: `high_pressure_attempts >= 10` and `high_pressure_attempt_rate >= 0.03`
- High impact, low conversion: `impact_rating >= 1.10` and `match_win_pct < 50.0`
- Clutch burden high: `clutch_attempt_rate >= 0.14`, `disadv_attempt_share >= 0.70`, `clutch_attempts >= 40`
- Extreme clutch frequency: `extreme_attempts >= 20` and `clutch_attempts >= 40`
- Fallback baseline insight if no rule triggers

## Notes for Formula Iteration

- Most calculations use safe defaults when values are missing.
- Division-by-zero paths resolve to `0.0` in calculator methods.`r`n- Per-hour metrics are suppressed to `None` when time scope is unreliable and shown as `N/A` in the UI.
- Clutch JSON is normalized defensively:
  - Missing keys default to `0`.
  - `total` and `lost_total` are validated against per-level sums and replaced by computed sums when mismatched.
- Compare and details flows both recalculate metrics from latest snapshots for consistent formulas in UI output and comparison.
- If you change thresholds/formulas, update tests in:
  - `tests/test_calculator.py`
  - `tests/test_analyzer.py`






