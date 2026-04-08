# CODEX_UI_REDESIGN_TASK.md — Jakal Web UI Redesign (Run-to-Completion)

## Objective
Redesign the Jakal web UI so it becomes cohesive, stable, and scalable (competitive + SaaS-friendly).
The current UI feels inconsistent and breaks easily after small changes. Fix that permanently.

You MUST implement a design system + app shell + reusable components, then refactor pages to use them.

## Hard Constraints (Non-Negotiable)
- Do NOT change or break existing backend endpoints under `/api/*` or websocket behavior.
- Do NOT add a frontend build pipeline (no React/Vite/Webpack). Keep it static vanilla HTML/CSS/JS.
- Do NOT remove features or pages. Feature parity must be preserved.
- Do NOT introduce large new dependencies casually. Chart library is allowed only if it is vendored and used through an adapter.
- Keep changes incremental and reviewable: multiple commits are preferred.
- Always ensure the app still runs on the local server after each milestone.

## Primary Deliverable
A consistent web UI that:
- uses a single app-shell layout across all tabs/pages
- uses CSS tokens for spacing/typography/colors
- uses a small set of reusable UI components
- keeps analytics visuals readable and stable
- reduces “minor break” regressions

## Definition of Done (Must Satisfy All)
1) **Design tokens exist and are used everywhere**
   - CSS variables define spacing, typography, surfaces, borders, radius, semantic colors, accent.
   - No random one-off colors/sizes/spacing outside tokenized rules.

2) **App shell layout is implemented**
   - Persistent header/topbar, navigation, content area, collapsible filter drawer, optional evidence/log drawer.
   - All pages (Network Scanner, Match Scraper, Stored Matches, Dashboard, Operators, etc.) fit into the same layout skeleton.

3) **Reusable components exist (plain JS + CSS)**
   Implement and use these components:
   - Button (primary/secondary/ghost)
   - Chip (status + active filters)
   - Card (panel container)
   - Drawer (filters/logs)
   - Tooltip (single consistent tooltip system)
   - DataTable (sortable header, sticky header, row hover)
   These must be used instead of copy/pasted HTML patterns.

4) **Operators & Dashboard are “decision-first”**
   - Default views must be scanable (Top/Bottom K, recommendation strip, baseline clarity, confidence encoding).
   - Advanced mode can show deeper visuals (e.g. dot/CI view), but default should not be cluttered.

5) **Stability**
   - UI changes should not break visuals due to CSS tweaks.
   - Centralize rendering logic; avoid brittle absolute positioning hacks unless isolated behind a component.

6) **Performance sanity**
   - No huge DOM explosions.
   - Use DocumentFragment or batched rendering.
   - Consider virtualization for long lists (stored matches).

## Iteration Loop (How You Work)
Repeat this loop until Definition of Done is met:

1) Inventory current UI structure and identify inconsistencies.
2) Implement foundation (tokens + shell + components).
3) Migrate one page at a time to the new shell/components.
4) Verify local server works and UI is functional.
5) Proceed to next page.

## Required Implementation Order (Do Not Skip)
### Milestone 1 — Tokens + Base Styles
- Create `web/static/styles/tokens.css` with variables:
  - spacing scale: --space-1..--space-8
  - radius: --radius-1..--radius-4
  - typography: --text-xs..--text-2xl
  - surfaces: --surface-0..--surface-3
  - borders/shadows
  - semantic colors: --good, --bad, --warn, --info, --accent
- Create `web/static/styles/base.css`:
  - body defaults, typography, links, focus states, scrollbar style (optional)
- Replace magic numbers gradually. New/modified UI must only use tokens.

### Milestone 2 — App Shell Layout
- Update `web/static/index.html` (or relevant entry page) to use a consistent layout:
  - header/topbar (title + status chips + primary action area)
  - left nav / tabs area
  - main content area
  - filter drawer (collapsible)
  - evidence/log drawer (collapsible)
- Ensure all existing pages/tabs still render and function within the shell.

### Milestone 3 — Component Library (Vanilla)
Create in `web/static/ui/`:
- `button.js` + CSS
- `chip.js` + CSS
- `card.js` + CSS
- `drawer.js` + CSS
- `tooltip.js` + CSS
- `datatable.js` + CSS

Rules:
- Components must be small, composable, and not framework-y.
- No global mutable state outside a small `store` module if needed.
- All components must use tokens.

### Milestone 4 — Filters + Active Filter Chips
- Move filters into the filter drawer.
- Show active filters as chips under the page title.
- Add “Reset filters” control.
- Ensure filters no longer cause layout thrash.

### Milestone 5 — Operators Page Redesign
- Keep feature parity and your recent operator image fixes.
- Default view:
  - Top/Bottom K per map per side
  - clear axis meaning (Δ vs baseline)
  - confidence encoding visible
  - recommendation strip (Top 3 / Avoid Bottom 2) per side
- Advanced toggle can show deeper visuals.
- Make it compact: reduce scroll height and redundant repeated blocks.

### Milestone 6 — Dashboard Redesign
- Organize into sections:
  - Trajectory, Maps, Operators, Sessions, Rounds, Context
- Add 2–3 “flagship” visuals first (do not overbuild):
  - Opening Impact
  - End Reasons
  - Rolling Win%
- Keep the controls clean: drawer + chips.

### Milestone 7 — Remaining Pages + Performance
- Apply shell/components to Match Scraper, Stored Matches, Network Scanner.
- Use DataTable where appropriate.
- Virtualize long lists if performance becomes an issue.
- Confirm no regressions.

## Optional Chart Library (Allowed, but Must Follow Rules)
If you migrate away from custom DOM charts:
- Use Apache ECharts (preferred).
- Vendor it into `web/static/vendor/echarts.min.js` (no CDN required).
- Create `web/static/viz/echarts_adapter.js` so pages do not depend on ECharts directly.
- Migrate one chart at a time.
- Do NOT refactor everything at once.

## Code Organization Rules
- No monolithic `network.js`-style God files. Split into:
  - `api/` (fetch/websocket only)
  - `state/` (store)
  - `ui/` (components)
  - `pages/` (page controllers)
  - `viz/` (charts)
- Each page module owns only its panel DOM and render cycle.
- Avoid repeated querySelector calls inside render loops (cache elements).

## Safety Checks Before Declaring “Done”
- Run the local server and click through every page/tab.
- Verify no console errors on load and during common interactions.
- Verify filters work and don’t break layout.
- Verify Operators and Dashboard read clearly on a normal laptop screen.
- Verify keyboard focus styles exist (tab navigation).

## Completion Output
When done, provide:
- A concise summary of what changed
- What files were added/modified
- Any follow-ups you recommend (but do not require)