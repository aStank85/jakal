# Data Integrity Repair Report

## Root Cause(s)
- H1 false: round-level ingestion was not broadly missing in the current dataset.
  - `player_rounds` coverage: `277/277` matches (100.0%)
  - `round_outcomes` coverage: `277/277` matches (100.0%)
  - Newest missing matches lists were empty.
- H2 true: queue normalization logic could misclassify values (notably `Unranked` matching `ranked` by substring order), and queue filtering depended on mixed raw labels.
- H3 true: min-round thresholds could hide all operator entries for sparse map/side slices, creating empty views.
- H4 true: operator names included inconsistent raw labels/diacritics (`Jäger`, `Tubarão`, `Unknown`) and required canonicalization for stable grouping.
- H5 not the primary issue: operator aggregation grain in `/api/operators/map-breakdown` was already `operator, side, map`.
- H6 partially true: frontend filtering could produce "empty" output without enough visibility into threshold filtering.

## Fixes Implemented
- Queue normalization and persistence:
  - Added canonical queue key mapping (`ranked|standard|quickmatch|event|other`) in ingest/migrations.
  - Canonicalizer now uses token boundaries instead of substring matching (`superrankedmode` no longer maps to `ranked`).
  - Added/maintained `mode_key` and `match_type_key` columns across relevant tables.
  - Fixed canonicalizer ordering so `Unranked` does not map to `ranked`.
- Operator normalization:
  - Added operator canonicalization with accent-stripping, alias handling, known-operator registry, and `UNKNOWN` fallback.
  - Added stable ASCII `operator_key` for grouping; `operator` is now a display value derived from that key.
  - Added `player_rounds.operator_raw` to preserve original labels.
  - Backfill migration now canonicalizes existing `player_rounds.operator_key` + `player_rounds.operator`.
- Incomplete ingest guardrails:
  - Added/maintained `scraped_match_cards.has_rounds` and `has_outcomes`.
  - `unpack_pending_scraped_match_cards` now targets incomplete cards directly using those flags.
  - Added ingest completeness warning logs when parsed rounds/outcomes are zero.
- Operators analytics behavior:
  - `/api/operators/map-breakdown` now returns all operator rows with `n` and `meets_min_rounds` instead of backend-dropping by threshold.
  - Added filtered-out counts per map (`filtered_out_by_min_rounds`) and warning logs.
  - Added dev diagnostics endpoint: `/api/dev/operators/diagnostics`.
- Frontend guardrails:
  - Added `Include low sample` toggle in Operators UI.
  - Added visible empty-state hint + one-click `Lower threshold to 3` action.
  - Added client logs for map/operator counts and threshold-filtered entries.
- Maintenance tooling:
  - Added diagnostics CLI: `python -m src.tools.data_integrity_check`.
  - Added backfill CLI: `python -m src.tools.backfill_rounds --queue ranked --since 2025-01-01`.
- Regression checks:
  - Added tests in `tests/test_data_integrity_normalization.py` for queue/operator normalization behavior.
  - Added assertions that canonical operator outputs are constrained to registry values + `UNKNOWN`.

## How To Verify Locally
1. Run diagnostics:
   - `python -m src.tools.data_integrity_check --show-queues --show-operators --operator-limit 40`
2. Run normalization tests:
   - `pytest -q -p no:cacheprovider tests/test_data_integrity_normalization.py`
3. Run DB regression tests:
   - `pytest -q -p no:cacheprovider tests/test_database.py`
4. Run backfill dry-run:
   - `python -m src.tools.backfill_rounds --queue ranked --since 2025-01-01 --dry-run`
5. Run backfill execution (idempotent):
   - `python -m src.tools.backfill_rounds --queue ranked --since 2025-01-01 --batch-size 100 --max-batches 5`
6. Start app and validate Operators UI:
   - `uvicorn web.app:app --reload`
   - Operators tab: verify non-silent threshold behavior and `Include low sample`.

## Evidence Snapshot
- DB diagnostics on `data/jakal_fresh.db`:
  - total matches: `277`, ranked: `204`
  - total `player_rounds`: `15965`
  - ranked `player_rounds`: `12223`
  - distinct `(match_id, round_id)`: `1605`
  - missing `player_rounds` matches: none
  - missing `round_outcomes` matches: none
- Backfill CLI run (ranked, since 2025-01-01):
  - pending before: `0`, scanned: `0`, errors: `0`, pending after: `0`
- Example populated operator slices (ranked) for a high-volume player (`saucedzyn`):
  - `Clubhouse defender`: `119` rounds
  - `Clubhouse attacker`: `113` rounds
  - multiple operators with `n >= 10` across map/side.

## Canonicalization Contract (Stability)
- `operator_raw`: exact observed source label (for audit/debug).
- `operator_key`: stable ASCII canonical key (used for grouping/filtering).
- `operator`: display label derived from `operator_key` via registry.
- Idempotency requirement:
  - Re-running canonicalization must not change `operator_key`.
  - Display changes in future versions must not split historical buckets because aggregation keys stay on `operator_key`.
