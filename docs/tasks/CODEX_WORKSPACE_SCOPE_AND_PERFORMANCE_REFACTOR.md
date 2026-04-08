# CODEX_WORKSPACE_SCOPE_AND_PERFORMANCE_REFACTOR.md
## Objective
Refactor workspace data loading to remove current performance bottlenecks:
- `_load_workspace_rows` is heavy because it joins round-level tables and uses a window-function CTE for latest scraped card per match.
- It then applies queue/playlist/map/stack filtering in Python, causing wasted work and inconsistent semantics across panels.

Implement a scope-first architecture with SQL-first filtering, cheaper “latest card” logic, and optional match-level last_scraped_at caching.

---

## Hard Constraints
- Do NOT break existing endpoints; add new helpers and migrate incrementally.
- Keep the system working on existing DBs (migrations must be safe).
- Do NOT require a frontend build pipeline.
- Prefer adding columns/indexes + backfill scripts over expensive live recomputation.

---

## Required Outcomes (Definition of Done)
1) Team panel no longer requires `_load_workspace_rows`.
2) Operators/Matchups/Team can all be driven by a shared `scope_key` + `match_ids`.
3) Replace `ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY id DESC)` with a cheaper approach:
   - Preferred: `matches.last_scraped_at` maintained at ingest time
   - Acceptable: CTE `SELECT match_id, MAX(scraped_at) ... GROUP BY match_id`
4) Push core filters into SQL (queue/map/window), minimize Python filtering.
5) Add the minimum indexes required so these queries are fast.
6) Add timings/logs around each phase.

---

# Step 1 — Add/Backfill `matches.last_scraped_at` (Preferred)
## 1.1 Schema change
Add nullable column:
- `matches.last_scraped_at` (TEXT or INTEGER depending on your timestamp convention)

## 1.2 Ingest maintenance
When scraping/inserting `scraped_match_cards`, also update:
- `matches.last_scraped_at = MAX(matches.last_scraped_at, new_card.scraped_at)` for that match

## 1.3 Backfill migration
Backfill existing matches:
- `UPDATE matches SET last_scraped_at = (SELECT MAX(scraped_at) FROM scraped_match_cards WHERE match_id = matches.match_id)`
Add a script or migration step that runs safely on older DBs.

---

# Step 2 — Replace Window Function Latest Card Query
If you cannot add `matches.last_scraped_at` immediately, replace the window CTE with:

```sql
WITH latest_card AS (
  SELECT match_id, MAX(scraped_at) AS scraped_at
  FROM scraped_match_cards
  GROUP BY match_id
)
```

This is generally far cheaper than ROW_NUMBER. Ensure supporting index exists (Step 5).

---

# Step 3 — Implement Scope Builder (SQL-first)
Create:
- `build_workspace_scope(params) -> ScopeResult`

Where ScopeResult includes:
- `scope_key`
- `match_ids` (preferred)
- `time_min` / `time_max`
- `filters_applied`

## 3.1 SQL should apply:
- username -> player_id
- ws_days window:
  - prefer `matches.last_scraped_at` for filtering
- queue filter using canonical `mode_key`
- map filter using match-level `map` column (if exists)
- playlist filter using match-level keys (if exists)

## 3.2 Python filtering allowed ONLY for:
- complex stack-only logic (if not easily representable yet)
- free-text search (until indexed)
But you must document what is filtered in Python and why.

---

# Step 4 — Refactor `_load_workspace_rows` to Use Scope
Change `_load_workspace_rows` to:
- call `build_workspace_scope`
- join `player_rounds` + `round_outcomes` only for `match_ids` in scope
- select only the columns needed by the requesting panel

Add a parameter:
- `columns_profile` or `panel` to select minimal columns

Examples:
- Operators panel: needs operator_key, side, map, team_won, and n
- Matchups panel: needs operator_key sets / matchup dimensions
- Team panel: should NOT use this loader (handled separately), but if used, must be minimal

---

# Step 5 — Add Required Indexes
Add indexes (or ensure they exist):
- `scraped_match_cards(match_id, scraped_at)`
- `matches(match_id)` (PK)
- `matches(last_scraped_at)`
- `matches(mode_key)`
- `matches(map)` (if filtered)
- `player_rounds(match_id, round_id)`
- `player_rounds(player_id_tracker)`
- `round_outcomes(match_id, round_id)`

---

# Step 6 — Add Caching
Cache scope result:
- `scope_key -> match_ids` (or round ids)
- store in SQLite table `workspace_scope_cache` or similar (preferred), with TTL and invalidation on ingest

Add caching for “expensive aggregates”:
- team pairs
- lineup aggregates (optional later)

---

# Step 7 — Logging + Guardrails
Add timing logs (dev-friendly) for:
- scope build time
- row load time
- Python filter time (should be near-zero for core filters)

Add a guardrail:
- if a query exceeds X seconds, return partial/fallback and set `is_partial=true`.

---

# Verification
- Team panel does not call `_load_workspace_rows`.
- Operators panel loads faster after refactor.
- Date window filtering works and matches expected counts.
- Queue filtering uses canonical keys (no raw label dependence).
- Scope caching hit/miss is visible in logs and metadata.

---

# Completion Output
Provide:
- summary of schema/index changes
- summary of query changes (before/after shape)
- how to verify locally (commands)
- any follow-ups
