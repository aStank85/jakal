# AGENTS.md — web/

## UI redesign goals (A/C: competitive + SaaS)
Build a decision-first analytics UI that stays clean for public users:
- Default view: fast signal, minimal noise, obvious “what to do.”
- Advanced mode: deeper plots + diagnostics + evidence drilldown.
- Statistical honesty: confidence/samples must be visible everywhere.

## Visual direction
- Dark technical vibe, clean and modern.
- Use CSS variables (design tokens) for spacing, type, surfaces, borders, and an optional accent.
- Keep control density low; group filters and use a collapsible filter drawer.

## Page layout rule (all tabs)
- Top bar: title + status chips (rate limit, DB, scraper state) + primary actions.
- Filter row/drawer: grouped controls + active filter chips.
- Main content: visualization/table/cards.
- Optional right panel: evidence/logs/details (collapsible).

## Operators page (current focus)
Goal: convert “data” into “decisions” without dumbing it down.

### Must-haves
- Per-map **Recommendation Strip** (derived from your existing Top/Bottom K):
  - “Recommended ATK core (Top 3)” and “Avoid (Bottom 2)”
  - Same for DEF.
- Show **Map Bias** clearly (ATK baseline vs DEF baseline) with a small bias bar/badge.
- Reduce redundancy:
  - Default view should NOT show both the Top/Bottom tables and the dot-plots at full prominence.
  - Provide an **Advanced toggle** to show the dot-plots (or expanded distribution) on demand.

### Confidence rules
- Encode sample size per operator row/mark (opacity or badge).
- Low-sample marks should look unstable (hollow/dashed, faded, or hidden under threshold).

## Dashboard page
- Organize graphs by sections (Trajectory / Maps / Operators / Sessions / Rounds / Context).
- Default to the 2–4 highest-value panels; keep the rest behind expanders.

## Technical constraints
- No bundler/build system.
- Plain HTML/CSS/JS (native ES modules allowed).
- Keep static serving compatible with `web/app.py`.
