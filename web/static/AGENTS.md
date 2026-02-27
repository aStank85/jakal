# AGENTS.md — web/static/

## JS structure
Keep behavior stable while improving maintainability.

### Refactor guideline
- Prefer extracting small modules around existing code rather than rewriting everything.
- Entry point stays `web/static/network.js` unless explicitly changed.
- If converting to ES modules:
  - Use `<script type="module">` and keep paths compatible with FastAPI static serving.
  - Ensure no endpoint changes.

### Suggested module boundaries
- `api/*` : fetch/websocket, no DOM
- `ui/*`  : components (chips, modal, tooltip, toast, skeleton)
- `pages/*`: tab controllers (operators/dashboard/etc.)
- `viz/*` : heatmap + operator impact visualizations

## Operators UI implementation requirements
- Default view should prioritize scanability:
  - Top/Bottom K lists (ranked) + recommendation strip.
  - Dot-plot is optional/Advanced.
- Always label axis meaning: “Δ win% vs baseline” and which baseline is used.
- Tooltips must show: delta, n, raw win%, baseline, and CI if available.

## Rendering/perf rules
- Build DOM via `DocumentFragment` or template strings → single append.
- Cache DOM references used in render loops.
- Debounce filter changes; throttle expensive rerenders.
- Virtualize long lists where needed.

## CSS rules
- Use `:root` tokens: `--space-*`, `--radius-*`, `--font-*`, `--surface-*`, `--border-*`, `--text-*`, `--accent-*`.
- No magic numbers; reuse tokens.
- Ensure focus-visible styles.
