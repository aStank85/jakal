# AGENTS.md instructions for web/static/

## Refactor plan for JS
The current `network.js` is monolithic. Break into modules:

- `app.js` (entry): boot, tab routing, global event wiring
- `api/client.js`: all fetch() calls + websocket helpers
- `state/store.js`: single source of truth; event emitter/subscribers
- `ui/components/*`: modal, chips, toast, table, tooltip, loading skeleton
- `pages/scanner.js`, `pages/matches.js`, `pages/stored.js`, `pages/dashboard.js`
- `viz/*`: network graph wrapper, heatmap renderer, sparklines

Rules:
- No "God state" object mutated everywhere.
- Each page module owns:
  - DOM queries within its panel
  - event handlers
  - render() methods driven by store state
- API module returns plain objects; pages decide rendering.

## CSS rules
- Establish design tokens in :root (spacing, radii, typography).
- Components must reuse tokens; avoid one-off magic numbers.
- Ensure focus styles exist for buttons/inputs.

## UX improvements (priority)
- Filter controls: move into a drawer/panel, show active filters as chips.
- Lists: virtualize Stored Matches list if large.
- Heatmap: sticky row/column headers + readable legend + better tooltip positioning.
