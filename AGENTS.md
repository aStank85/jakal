# AGENTS.md — Jakal (global)

## Project context
Jakal is a local-first Rainbow Six Siege analytics tool.
Key parts:
- FastAPI backend serves a static web UI from `web/static/`.
- Data and insights come from SQLite + Python plugins in `src/`.
- Frontend is currently vanilla HTML/CSS/JS (no build step).

## Non-negotiables
- Do NOT break existing HTTP endpoints under `/api/*` or websocket behavior.
- Do NOT introduce a frontend build pipeline (no React/Vite/Webpack/etc.) unless explicitly requested.
- Keep dependencies minimal. Prefer stdlib and existing deps.
- Keep UI functional first: redesign must not remove features (scanner, match scraper, stored matches, dashboard).

## Output expectations
- Prefer small, reviewable changes (multiple commits / PR-sized steps).
- Avoid giant rewrites in one shot.
- Refactors must preserve behavior; when changing behavior, document it clearly.

## Code quality
- No new global mutable state in JS unless unavoidable.
- Favor modularization (ES modules) and clear separation:
  - API client
  - state/store
  - UI components/renderers
  - page controllers (tabs)

## Performance
- UI should not freeze on large lists or large heatmaps.
- Prefer lazy rendering / virtualization for long lists.
- Avoid expensive DOM reflows in loops.

## Accessibility
- Keyboard navigation for tabs, dialogs.
- Visible focus states.
- Ensure readable contrast; do not rely on color alone for meaning.

## Local run
- Web: `uvicorn web.app:app --reload`
