# CODEX_WORKSPACE_TEAM_TIMEOUT_AND_NAV_TASK.md (v2)
## Scope
Fix two problems:
1) Workspace “Team” view triggers heavy preload and can time out (75s+) even though Team currently returns a shell.
2) UI navigation is too nested (tab within tab within tab). Workspace needs to be discoverable and 2-click max.

This task is product-quality: performance + UX + maintainability.

---

## Hard Constraints (Non-Negotiable)
- Do NOT break existing backend API endpoints or websocket behavior.
- Do NOT add a frontend build pipeline (no React/Vite/Webpack).
- Maintain feature parity: Workspace features must still exist and work.
- Keep changes incremental and testable.
- Never run combinatorial “team” analytics on every request without caching.
- Never block the UI with a spinner for >5s without progress or fallback.

---

## Definitions
### “Workspace”
A scoped analytics view defined by parameters:
- username/user
- queue (ranked/standard/quickmatch/all)
- playlist/map filter
- time window (ws_days)
- stack_only toggle
- normalization/confidence/view settings (do NOT affect DB filtering)

### “Team View”
Analytics about teammate combinations (at minimum: pairs) within the current workspace scope.

---

## Primary Deliverables
1) Workspace becomes a top-level navigation tab (not nested under Dashboard).
2) Team view no longer pays the cost of full `_load_workspace_rows` preload.
3) Team view is fast and stable (pairs-first, cached / aggregated).
4) A single shared “Scope Bar” (filters) is reused across Workspace subviews.
5) Logging + guardrails exist so future changes can’t silently reintroduce 75s timeouts.

---

## Definition of Done
✅ UX
- Workspace accessible directly from top navigation (1 click).
- “Workspace → Team” reachable in ≤2 clicks.
- Scope bar shows queue/window/map/stack-only clearly and produces active-filter chips.

✅ Performance
- Team panel initial load returns fast (<1s) because it does NOT call heavy preload.
- Team data endpoint returns:
  - <2s for cache hit
  - <10s for cache miss WITH progress indication OR partial fallback
- No request hard-times out at 75s.

✅ Maintainability
- Workspace scope parameters centralized (single parser + single scope builder).
- Team computations live in a module, not embedded in route handlers.
- Operators/Matchups/Team share the same scope semantics.

---

# Part A — Navigation Unnesting (Workspace becomes top-level)

## A1) Make Workspace a top-level tab
- Add “Workspace” to the main navbar (same level as Dashboard, Operators, etc.)
- Route/URL supports deep link parameters, e.g.:
  - `/?panel=workspace&ws_view=team&ws_days=90&ws_queue=all&ws_stack_only=false`
- Preserve existing params for backward compatibility where practical.

## A2) Workspace page structure
Workspace page contains:
- Scope Bar (filters)
- Sub-tabs: Overview | Operators | Matchups | Team

Dashboard remains “high-level highlights,” not the home of Workspace.

---

# Part B — Stop Team Panel From Paying Heavy Preload

## B1) Identify current behavior
- `GET /api/dashboard-workspace/{username}` with `panel=team` currently calls `_load_workspace_rows` (heavy) even though it returns a shell response.

## B2) Required change
- When `panel=team`, do NOT call `_load_workspace_rows`.
- Return the shell response immediately, OR (preferred) call a lightweight scope builder (see Part C) that returns quickly.

This single change eliminates the current 75s timeout pain immediately.

---

# Part C — Implement Workspace Scope Builder + Caching (required for Team, recommended for all)

You MUST implement a shared scope builder used by Operators/Matchups/Team to eliminate duplicated heavy joins and Python-side filtering.

## C1) Implement `build_workspace_scope(params)`
Output either:
- a list of `match_id`s in scope (preferred), OR
- a list of `(match_id, round_id)` in scope

Scope must apply in SQL:
- username → player_id
- canonical queue key (`mode_key`) or equivalent
- date window (prefer match timestamp; fallback to last_scraped_at)
- map/playlist filters if stored at match level

Avoid Python-side filtering for core dimensions when possible.

## C2) Cache scope results
- Compute a stable `scope_key` hash from (username, ws_days, ws_queue, playlist/map, stack_only, etc.)
- Cache `scope_key -> match_ids` (or round_ids) with TTL.
- Store cache in SQLite table or in-memory + SQLite for persistence.

---

# Part D — Team View: Pairs-First, Cached

## D1) Create team endpoint
Add endpoint (if not present):
- `GET /api/workspace/team/{username}?ws_days=...&ws_queue=...&...`

It must:
- Use `build_workspace_scope` (C1)
- Use caching
- Compute PAIRS ONLY by default

## D2) Output contract
Return:
- list of teammate pairs with:
  - rounds_n, wins_n, win_rate
  - delta vs user baseline (optional)
  - confidence / sample warnings (optional)
- include metadata:
  - scope_key, cache_hit, compute_ms
  - is_partial and reason if fallback triggered

---

# Part E — Frontend Loading UX for Team

- Replace “Loading workspace team…” with staged progress or a clear message:
  - “Building scope…”
  - “Loading cached results…” or “Computing pairs…”
- If backend returns `is_partial`, show a banner and quick actions:
  - Narrow window (90d→30d)
  - Ranked-only
  - Increase min rounds
- Provide an explicit “Refresh” button that invalidates cache for this scope.

---

# Verification Checklist
1) Workspace is top-level.
2) Team panel no longer triggers `_load_workspace_rows`.
3) Team endpoint returns quickly on cache hit (<2s).
4) Cache miss finishes (<10s) or returns partial fallback (never hard timeout).
5) Operators/Matchups still work and share scope behavior.

---

# Completion Output
Provide:
- Summary of changes
- Files modified/added
- How caching works (key + TTL + storage)
- How to verify locally
