# UI Redesign Brief — Jakal Web UI (A/C)

## Audience
- A) Competitive stacks (decision speed + depth)
- C) Public/SaaS users (clarity + trust)

## Operators page — success criteria
A user can answer in <10 seconds:
- “What should I run on this map (ATK/DEF)?”
- “What should I avoid?”
- “Do I trust this (sample size/confidence)?”

### Default view (scan)
Per map card:
- Header: Map name + sample size.
- Map bias strip: ATK baseline vs DEF baseline.
- Recommendation strip:
  - ATK Top 3 (core)
  - ATK Avoid Bottom 2
  - DEF Top 3
  - DEF Avoid Bottom 2
- Ranked Top/Bottom K lists with delta + n badges.

### Advanced view (diagnose)
- Toggle reveals dot-plot / distribution and richer tooltips.
- Optional “show all operators” toggle.

## Statistical guardrails
- Minimum n threshold (user adjustable).
- Low-n encoding (faded/hollow + warning badge).
- Avoid misleading sorts without n visibility.

## Non-goals
- No endpoint redesign.
- No frontend build pipeline.
- No heavy new dependencies.
