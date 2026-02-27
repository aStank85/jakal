# UI Redesign Brief — Jakal Web UI

## Goal
Improve scanability and consistency of the UI so users can quickly identify:
- best/worst maps and operators
- ATK vs DEF problems
- confidence/sample size issues
- changes over time (streaks, sessions, RP swings)

## Must keep
- All existing tabs and functionality.
- Existing `/api/*` endpoints and websocket behavior.
- No new frontend build toolchain.

## Operators page redesign target
Current problem: too many marks per map, hard to scan.

New structure:
- Overview grid of map cards:
  - header: map name, n, ATK baseline, DEF baseline
  - body: ATK Top/Bottom K and DEF Top/Bottom K (default K=5)
  - clear axis labels: Δ win% vs baseline
  - confidence encoding for each operator point
- Click map → detail view:
  - expanded Top/Bottom 10, filters, evidence list

## Dashboard redesign target
- Filters grouped in a collapsible drawer.
- Active filters shown as chips.
- Graphs organized by section (Trajectory / Maps / Operators / Sessions / Rounds / Context).
- Avoid control overload.

## Success criteria
- A new user can answer “what should I fix?” within 10 seconds on Operators/Dashboard.
- No UI freezing on large datasets.
- Everything still works with the same backend.