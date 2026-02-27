# AGENTS.md — Jakal (global)

## Project reality (keep accurate)
- Backend: FastAPI app in `web/app.py` serving the static UI from `web/static/`.
- Data: SQLite via `src/database.py` (default path is `data/jakal_fresh.db` unless `JAKAL_DB_PATH` is set).
- Analytics: Python plugins under `src/plugins/`.
- Frontend: plain HTML/CSS/JS (no build step). Keep it that way unless explicitly asked.

## Non‑negotiables
- Do NOT break existing HTTP endpoints under `/api/*` or websocket behavior.
- Do NOT add a frontend build pipeline (React/Vite/Webpack/etc.) unless explicitly requested.
- Keep dependencies minimal (stdlib + existing deps).
- Redesign must NOT remove features/tabs (Network Scanner, Match Scraper, Stored Matches, Players, Team Builder, Operators, Dashboard).

## Repo hygiene
- Do NOT commit `.db`, `-wal`, `-shm`, scraped blobs, or large generated assets.
- If you need sample data, add small fixtures under `tests/fixtures/`.

## Change style
- Prefer small, reviewable PR-sized changes.
- Mechanical refactor first (no behavior change), then UX changes.
- When behavior changes, document it in the PR summary + inline comments.

## Performance (backend + DB)
- Avoid per-row commits in ingestion paths; batch with a single transaction.
- Favor indices aligned to *actual* query filters/sorts.
- Prefer pre-aggregated/summary tables for dashboards over repeated full scans.

## UI performance
- Avoid full re-render of large DOM trees on minor state changes.
- Throttle expensive renders (heatmaps / large lists / graphs).
- Virtualize long lists (Stored Matches, match cards) when needed.

## Accessibility
- Keyboard-navigable tabs and dialogs.
- Visible focus styles.
- Don’t rely on color alone for meaning.

## Local run
- Web: `uvicorn web.app:app --reload`
