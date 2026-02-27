# AGENTS.md instructions for web/

## UI redesign goals (high-level)
Make the web UI feel like a cohesive "app shell":
- Persistent navigation (tabs can remain, but should feel like a real app)
- A consistent layout grid with predictable spacing
- Filters should be organized (prefer a collapsible filter drawer/panel)
- Data views should feel "premium": strong hierarchy, clean typography, clear affordances

## Visual style direction
- Dark, technical/hacker vibe (clean, not cheesy).
- Keep the current dark theme, but unify tokens and spacing.
- Add optional accent color support via CSS variables.

## Information architecture improvements (must keep all features)
Current tabs: Network Scanner, Match Scraper, Stored Matches, Dashboard.
Rework each tab to follow the same structure:
- Top bar: page title, status chips (rate limit, scraper status), primary action button.
- Secondary row: filters (collapsible on small screens).
- Main content: visualization/table/cards
- Bottom/side: logs (collapsible; default collapsed on small screens)

## Technical constraints
- No bundler. Use native ES modules:
  - Convert `network.js` into `app.js` (entry) + `modules/*`.
  - Use `<script type="module">` from `index.html`.
- Keep CSS in plain files; splitting into multiple CSS files is OK.

## Deliverables
1) A new app shell layout
2) A reorganized Dashboard UI:
   - Filters grouped and collapsible
   - "Graphs" view with cleaner control density
3) Refactored JS structure (no 4,000-line single file)
