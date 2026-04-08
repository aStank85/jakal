# CODEX_DATA_INTEGRITY_REPAIR_TASK.md — Operator Data Missing/Sparse (Investigate + Repair)

## Goal
Operator analytics sometimes show “not enough data” or appear empty even when many ranked matches exist.
Investigate and repair the full pipeline end-to-end so operator analytics accurately reflect stored data.

This task is BOTH:
1) diagnostics (prove where the data disappears)
2) remediation (repair/normalize/backfill, and prevent regression)

## Hard Constraints
- Do NOT break existing `/api/*` endpoints or websocket behavior.
- Do NOT change the semantics of stored data without writing a migration/backfill.
- Keep changes incremental and safe. Prefer adding missing normalization/backfill rather than destructive edits.
- Do NOT silently discard data. Log when filtering removes data and why.
- Ensure local server still runs at each milestone.

---

## High-Level Hypotheses To Test (In Order)
H1) Matches are saved, but round-level rows (`player_rounds`) are missing for many matches.
H2) Ranked queue filtering mismatches stored values (e.g. 'ranked' vs 'Ranked' vs 'pvp_ranked').
H3) Min-round thresholds hide most operator entries at map/side granularity.
H4) Operator names are inconsistent (case, whitespace, aliases), breaking grouping/joining.
H5) Aggregation queries group too narrowly (over-fragmentation), producing low `n`.
H6) Backend returns data but frontend filters/renders incorrectly.

---

## Required Deliverables
1) A clear written diagnosis: which hypothesis was true, with evidence (counts and examples).
2) A repair: normalization/backfill or query fixes so operator analytics populate correctly.
3) Guardrails:
   - data integrity checks
   - logging/metrics for “filtered out” reasons
   - a maintenance/backfill command (CLI) to reprocess old matches safely

---

## Milestone 1 — Inventory DB Schema + Data Shape
### 1.1 Locate DB file and schema definitions
- Identify where SQLite DB lives (path and naming).
- Confirm tables exist: `matches` (or equivalent), `player_rounds`, `round_outcomes`, match-level aggregates.
- Print schema for relevant tables and document it briefly in comments or docs.

### 1.2 Produce DB sanity counts (must be runnable locally)
Create a small debug script (or add a CLI command) that prints:

A) Total matches:
- total matches
- ranked matches

B) Round-level rows:
- total `player_rounds` rows
- `player_rounds` rows linked to ranked matches
- distinct (match_id, round_id) pairs in `player_rounds`
- distinct operators in `player_rounds`

C) Coverage:
- % of matches that have any `player_rounds`
- % of matches that have any `round_outcomes`
- list of newest 10 matches missing `player_rounds` (by date or match_id)
- list of newest 10 matches missing `round_outcomes`

The output should immediately reveal if H1 is true.

---

## Milestone 2 — Validate Ranked Queue Normalization (H2)
### 2.1 Enumerate queue values stored
Run a query (or script) to list distinct queue/mode labels used in DB:
- distinct match queue values and counts

### 2.2 Normalize queue labels
If there are multiple variants:
- implement a normalization function in backend ingest path:
  - store canonical queue values: `ranked`, `standard`, `quickmatch`, `event`, `other`
- If existing DB contains non-canonical values, implement a migration/backfill:
  - update queue fields to canonical (or add a derived canonical column)

Add a unit-style test or sanity check that rejects unknown queues or maps them to `other`.

---

## Milestone 3 — Operator Name Normalization (H4)
### 3.1 Audit distinct operator strings
List distinct `operator` values from `player_rounds` with counts.
Look for:
- case differences
- whitespace
- aliases (e.g., "Zero" vs "Sam Fisher" if that exists)
- unknown placeholders (NULL, '', 'unknown')

### 3.2 Implement canonical operator normalization
Add a function used at ingest time:
- trim
- canonical case (recommend: store canonical display name used everywhere)
- validate against a known operator registry list
- map known aliases → canonical
- if unknown, store as `UNKNOWN` but keep raw in a separate field if available

### 3.3 Backfill existing operator strings
Write a migration script:
- update `player_rounds.operator` to canonical
- produce a report of any operators mapped to UNKNOWN

---

## Milestone 4 — Confirm Operator Analytics Aggregation (H3, H5)
### 4.1 Verify aggregation query grain
Locate the backend function/endpoint that serves operator analytics.
Inspect the GROUP BY:
- It should group by: player_id, map, side, operator (and optionally queue)
- Ensure it is NOT accidentally grouping by match_id, round_id, or too-granular fields.

### 4.2 Verify min-round filter behavior
Confirm where min-round filtering occurs:
- backend filtering, frontend filtering, or both

Fix rules:
- backend should return `n` for every operator entry
- frontend should decide display thresholds, but must also provide an “Include low sample” toggle
- Never “drop everything” silently; show a message:
  - “0 operators meet min_rounds=X on this map/side. Try lowering the threshold.”

### 4.3 Add baseline clarity
Confirm baseline computation uses the same filtered dataset and queue filters.
If baseline uses a different dataset than the operator points, that will cause confusion.

---

## Milestone 5 — Prove End-to-End With Evidence
### 5.1 Add a debug endpoint or mode (dev-only)
Add a dev-only diagnostics endpoint or CLI mode that can answer:
- For a given map+side, what is:
  - baseline n
  - top 5 operators by n
  - sample operator row example (match_id/round_id)
This ensures your UI’s “not enough data” isn’t lying.

### 5.2 Frontend verification
In the Operators page JS:
- log counts from the API response:
  - how many maps returned
  - how many operator entries returned
  - how many are filtered out by min_rounds
If UI shows empty while API is populated, fix frontend filtering/rendering (H6).

---

## Milestone 6 — Repair Missing Round Data (H1) If Needed
If many matches exist without `player_rounds` and/or `round_outcomes`:

### 6.1 Identify the ingest pipeline
- Determine whether matches were saved without round parsing (older versions).
- Determine whether a parse step is optional and skipped.

### 6.2 Implement a Backfill Command (must exist)
Add a CLI command, e.g.:
- `python -m src.tools.backfill_rounds --queue ranked --since 2025-01-01`
or similar.

It should:
- iterate matches missing `player_rounds` and parse/import rounds
- be idempotent (safe to run multiple times)
- run in batches with transactions
- log progress and skip failures gracefully

### 6.3 Add “ingest completeness” flags
Optionally add:
- `matches.has_rounds` boolean
- `matches.has_outcomes` boolean
Maintain these during ingest and backfill.

---

## Regression Prevention
Add these guardrails:

1) DB constraints/indexes:
- indexes on `(match_id, round_id)`, `(player_id_tracker)`, `(operator)`, `(map)`, `(queue)`
- unique constraints if appropriate for idempotency

2) Runtime sanity checks:
- When ingesting a match, verify:
  - expected round count > 0
  - player_rounds inserted > 0
  - round_outcomes inserted > 0
If not, mark match incomplete and warn.

3) UI guardrails:
- If min_rounds filters everything, show a visible hint and a one-click “lower threshold” action.

---

## Completion Criteria
This task is done only when:
- Operator analytics show populated results for common maps with your dataset
- Ranked filtering matches stored data (no mismatched queue labels)
- Old matches are backfilled or clearly marked incomplete
- Lowering min_rounds reveals low-sample operators (no silent empty states)
- A CLI/backfill tool exists to repair incomplete records
- A short report is written in `docs/` describing the root cause and fix

---

## Output Format
At the end, provide:
- “Root cause(s)” section
- “Fixes implemented” section
- “How to verify locally” steps
- “Follow-ups” (optional improvements)