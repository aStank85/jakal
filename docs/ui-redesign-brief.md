# UI Redesign Brief

## Goal
Redesign the Jakal web UI for consistency, readability, and performance while preserving full feature parity and existing backend contracts.

## What "good" looks like

### App-shell layout sketch (text)
- Global shell
  - Top app bar: app title, active tab/page title, status chips (rate-limit, scraper state), primary action.
  - Left/top navigation: persistent tab navigation (Scanner, Matches, Stored, Dashboard).
  - Main content area: predictable grid with consistent spacing tokens.
  - Secondary utility area: collapsible logs/evidence drawer.
- Per-page structure
  - Row 1: page title + key status + primary CTA.
  - Row 2: filters in collapsible drawer/panel (especially on smaller screens).
  - Row 3: core data surface (network graph, tables, cards, heatmap, etc.).
  - Row 4: contextual logs/evidence/inspector.
- Dashboard/Graphs
  - Cleaner control density grouped by domain:
    - Match Types
    - Normalization
    - Confidence / min sample
    - View options
  - Graphs split into clear tabs/panels.
  - Heatmap headers/legend/tooltips always readable and non-obstructive.

## What must not change
- No backend endpoint contract changes under `/api/*`.
- No websocket behavior regressions.
- No frontend build step introduction (keep static serving + native JS/CSS).
- No feature removals:
  - Network Scanner
  - Match Scraper
  - Stored Matches
  - Dashboard (insights + graphs + workspace behavior)

## Priorities
1. Dashboard usability and heatmap readability first.
2. Introduce consistent layout/tokens across all tabs.
3. Refactor frontend into native ES modules without behavior drift.
4. Performance safeguards:
   - Avoid expensive full re-renders.
   - Virtualize long lists where needed.
   - Keep UI responsive during heavy data views.
5. Accessibility baseline:
   - keyboard navigation,
   - focus visibility,
   - contrast and non-color-only cues.
