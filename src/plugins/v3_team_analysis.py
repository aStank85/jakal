"""
src/plugins/v3_team_analysis.py
================================
Data-driven team analysis using match_detail_players.
"""

from __future__ import annotations

from itertools import combinations
from typing import Any

MIN_RELIABLE_MATCHES = 5
MIN_MENTION_MATCHES = 3


class TeamAnalysisPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = str(username or "").strip()
        self._result: dict | None = None

    def analyze(self) -> dict:
        player_matches = self._fetch_player_matches()
        if not player_matches:
            return self._empty("No match data found for this player.")

        total_matches = len(player_matches)
        wins = sum(1 for m in player_matches if m["result"] == "win")
        baseline_wr = wins / total_matches if total_matches > 0 else 0.0

        match_ids = [m["match_id"] for m in player_matches]
        match_lookup = {m["match_id"]: m for m in player_matches}

        teammates_by_match = self._fetch_teammates(match_ids)
        partner_stats = self._calc_partner_stats(match_lookup, teammates_by_match, baseline_wr)

        reliable = [p for p in partner_stats if p["matches"] >= MIN_RELIABLE_MATCHES]
        mentioned = [p for p in partner_stats if p["matches"] >= MIN_MENTION_MATCHES]

        best_partner = max(reliable, key=lambda x: x["chemistry_delta"], default=None)
        worst_partner = min(reliable, key=lambda x: x["chemistry_delta"], default=None)
        most_played = max(partner_stats, key=lambda x: x["matches"], default=None)

        stack_size_stats = self._calc_stack_size_stats(match_lookup, teammates_by_match, partner_stats)
        best_stack_size = self._best_stack_size(stack_size_stats)

        synergy_pairs = self._calc_synergy_pairs(match_lookup, teammates_by_match, min_matches=MIN_MENTION_MATCHES)
        best_pair = synergy_pairs[0] if synergy_pairs else None

        findings = self._generate_findings(
            baseline_wr=baseline_wr,
            partner_stats=mentioned,
            best_partner=best_partner,
            worst_partner=worst_partner,
            most_played=most_played,
            stack_size_stats=stack_size_stats,
            best_stack_size=best_stack_size,
            synergy_pairs=synergy_pairs,
        )

        result = {
            "username": self.username,
            "baseline_win_rate": round(baseline_wr * 100, 1),
            "total_matches": total_matches,
            "partner_stats": partner_stats,
            "best_partner": best_partner,
            "worst_partner": worst_partner,
            "most_played_with": most_played,
            "stack_size_stats": stack_size_stats,
            "best_stack_size": best_stack_size,
            "synergy_pairs": synergy_pairs,
            "best_pair": best_pair,
            "findings": findings,
        }
        self._result = result
        return result

    def summary(self) -> None:
        result = self._result or self.analyze()
        if result.get("error"):
            print(f"[TeamAnalysis] {result['error']}")
            return

        print(f"\n=== TEAM ANALYSIS: {result['username']} ===")
        print(f"Baseline WR: {result['baseline_win_rate']}% over {result['total_matches']} matches")

        print("\n--- QUEUE PARTNERS (3+ shared matches) ---")
        for p in result["partner_stats"]:
            if p["matches"] < MIN_MENTION_MATCHES:
                continue
            sign = "+" if p["chemistry_delta"] >= 0 else ""
            print(
                f"  {p['username']:<20} {p['matches']:>3} matches  "
                f"{p['win_rate']:>5.1f}% WR  {sign}{p['chemistry_delta']:.1f}% delta"
            )

        print("\n--- STACK SIZE ---")
        for s in result["stack_size_stats"]:
            print(f"  {s['label']:<10}  {s['matches']:>3} matches  {s['win_rate']:>5.1f}% WR")

        print("\n--- TOP TRIO COMBOS ---")
        for pair in result["synergy_pairs"][:3]:
            print(
                f"  {pair['partner_a']} + {pair['partner_b']}: "
                f"{pair['win_rate']}% WR ({pair['matches']} matches)"
            )

        print("\n--- FINDINGS ---")
        for f in result["findings"]:
            tag = str(f.get("severity") or "info").upper()
            print(f"  [{tag}] {f.get('message', '')}")

    def _fetch_player_matches(self) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT mdp.match_id, mdp.team_id, mdp.result, mdp.rank_points,
                   mdp.rank_points_delta, mdp.kills, mdp.deaths, mdp.kd_ratio,
                   mdp.scraped_at
            FROM match_detail_players mdp
            WHERE mdp.username = ?
              AND EXISTS (
                    SELECT 1
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = mdp.match_id
                      AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
                      AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
              )
            ORDER BY mdp.scraped_at DESC
            """,
            (self.username,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _fetch_teammates(self, match_ids: list[str]) -> dict[str, list[dict]]:
        if not match_ids:
            return {}

        placeholders = ",".join("?" * len(match_ids))
        cur = self._conn.cursor()
        cur.execute(
            f"""
            SELECT
                mdp.match_id,
                mdp.username,
                mdp.team_id,
                mdp.result,
                mdp.kills,
                mdp.deaths,
                mdp.kd_ratio,
                mdp.rank_points,
                mdp.rank_points_delta
            FROM match_detail_players mdp
            JOIN match_detail_players me
                ON me.match_id = mdp.match_id
                AND me.username = ?
            WHERE mdp.match_id IN ({placeholders})
              AND mdp.username != ?
              AND mdp.team_id = me.team_id
            """,
            [self.username] + match_ids + [self.username],
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        result: dict[str, list[dict]] = {}
        for row in rows:
            result.setdefault(row["match_id"], []).append(row)
        return result

    @staticmethod
    def _calc_partner_stats(match_lookup: dict, teammates_by_match: dict[str, list[dict]], baseline_wr: float) -> list[dict]:
        agg: dict[str, dict] = {}

        for match_id, teammates in teammates_by_match.items():
            m = match_lookup.get(match_id) or {}
            won = m.get("result") == "win"
            for teammate in teammates:
                name = teammate.get("username") or ""
                if not name:
                    continue
                if name not in agg:
                    agg[name] = {
                        "username": name,
                        "matches": 0,
                        "wins": 0,
                        "total_kd": 0.0,
                        "total_rp": 0.0,
                    }
                agg[name]["matches"] += 1
                if won:
                    agg[name]["wins"] += 1
                agg[name]["total_kd"] += teammate.get("kd_ratio") or 0.0
                agg[name]["total_rp"] += teammate.get("rank_points") or 0.0

        stats = []
        for name, data in agg.items():
            n = data["matches"]
            wr = data["wins"] / n if n > 0 else 0.0
            avg_kd = data["total_kd"] / n if n > 0 else 0.0
            avg_rp = data["total_rp"] / n if n > 0 else 0.0
            delta = wr - baseline_wr
            stats.append(
                {
                    "username": name,
                    "matches": n,
                    "wins": data["wins"],
                    "win_rate": round(wr * 100, 1),
                    "chemistry_delta": round(delta * 100, 1),
                    "avg_kd": round(avg_kd, 2),
                    "avg_rp": int(round(avg_rp, 0)),
                }
            )

        stats.sort(key=lambda x: (-x["matches"], -x["chemistry_delta"]))
        return stats

    @staticmethod
    def _calc_stack_size_stats(match_lookup: dict, teammates_by_match: dict[str, list[dict]], partner_stats: list[dict]) -> list[dict]:
        recurring = {p["username"] for p in partner_stats if p["matches"] >= MIN_MENTION_MATCHES}
        size_agg: dict[int, dict] = {}

        for match_id, teammates in teammates_by_match.items():
            match_row = match_lookup.get(match_id) or {}
            won = match_row.get("result") == "win"
            recurring_count = sum(1 for t in teammates if (t.get("username") or "") in recurring)
            size = min(recurring_count, 4)
            if size not in size_agg:
                size_agg[size] = {"stack_size": size, "matches": 0, "wins": 0}
            size_agg[size]["matches"] += 1
            if won:
                size_agg[size]["wins"] += 1

        result = []
        for size in sorted(size_agg.keys()):
            data = size_agg[size]
            n = data["matches"]
            wr = data["wins"] / n * 100 if n > 0 else 0.0
            label = "Solo" if size == 0 else "Duo" if size == 1 else "Trio" if size == 2 else f"{size + 1}-Stack"
            result.append(
                {
                    "stack_size": size,
                    "label": label,
                    "matches": n,
                    "wins": data["wins"],
                    "win_rate": round(wr, 1),
                }
            )
        return result

    @staticmethod
    def _best_stack_size(stack_size_stats: list[dict]) -> dict | None:
        reliable = [s for s in stack_size_stats if s["matches"] >= MIN_MENTION_MATCHES]
        if not reliable:
            return None
        return max(reliable, key=lambda x: x["win_rate"])

    @staticmethod
    def _calc_synergy_pairs(
        match_lookup: dict,
        teammates_by_match: dict[str, list[dict]],
        min_matches: int = MIN_MENTION_MATCHES,
    ) -> list[dict]:
        pair_agg: dict[tuple[str, str], dict] = {}

        for match_id, teammates in teammates_by_match.items():
            match_row = match_lookup.get(match_id) or {}
            won = match_row.get("result") == "win"
            names = sorted(t["username"] for t in teammates if (t.get("username") or ""))
            for a, b in combinations(names, 2):
                key = (a, b)
                if key not in pair_agg:
                    pair_agg[key] = {"partner_a": a, "partner_b": b, "matches": 0, "wins": 0}
                pair_agg[key]["matches"] += 1
                if won:
                    pair_agg[key]["wins"] += 1

        results = []
        for data in pair_agg.values():
            n = data["matches"]
            if n < min_matches:
                continue
            wr = data["wins"] / n * 100
            results.append(
                {
                    "partner_a": data["partner_a"],
                    "partner_b": data["partner_b"],
                    "matches": n,
                    "wins": data["wins"],
                    "win_rate": round(wr, 1),
                }
            )

        results.sort(key=lambda x: (-x["matches"], -x["win_rate"]))
        return results

    @staticmethod
    def _generate_findings(
        baseline_wr: float,
        partner_stats: list[dict],
        best_partner: dict | None,
        worst_partner: dict | None,
        most_played: dict | None,
        stack_size_stats: list[dict],
        best_stack_size: dict | None,
        synergy_pairs: list[dict],
    ) -> list[dict]:
        findings = []

        if best_partner and best_partner["chemistry_delta"] >= 8:
            findings.append(
                {
                    "severity": "info",
                    "category": "queue_partner",
                    "message": (
                        f"Queue with {best_partner['username']} whenever possible. "
                        f"{best_partner['win_rate']}% WR together "
                        f"(+{best_partner['chemistry_delta']}% vs your {baseline_wr * 100:.0f}% baseline, "
                        f"{best_partner['matches']} matches)."
                    ),
                }
            )

        if worst_partner and worst_partner["chemistry_delta"] <= -8:
            findings.append(
                {
                    "severity": "warning",
                    "category": "queue_partner",
                    "message": (
                        f"Avoid queuing with {worst_partner['username']}. "
                        f"{worst_partner['win_rate']}% WR together "
                        f"({worst_partner['chemistry_delta']}% vs baseline, "
                        f"{worst_partner['matches']} matches). "
                        "Consider whether this stack is worth it."
                    ),
                }
            )

        if (
            most_played
            and most_played["matches"] >= MIN_RELIABLE_MATCHES
            and abs(most_played.get("chemistry_delta", 0)) < 5
            and most_played["username"]
            not in ((best_partner or {}).get("username", ""), (worst_partner or {}).get("username", ""))
        ):
            findings.append(
                {
                    "severity": "info",
                    "category": "queue_partner",
                    "message": (
                        f"Most played with: {most_played['username']} "
                        f"({most_played['matches']} matches, {most_played['win_rate']}% WR, "
                        "roughly neutral - comfortable but not boosting your rate)."
                    ),
                }
            )

        reliable = [p for p in partner_stats if p["matches"] >= MIN_RELIABLE_MATCHES]
        if len(reliable) >= 2:
            spread = max(p["chemistry_delta"] for p in reliable) - min(p["chemistry_delta"] for p in reliable)
            if spread >= 15:
                findings.append(
                    {
                        "severity": "warning",
                        "category": "queue_partner",
                        "message": (
                            f"Your win rate swings {spread:.0f}% depending on who you queue with. "
                            "Teammate selection is one of your biggest controllable levers."
                        ),
                    }
                )

        if best_stack_size:
            solo = next((s for s in stack_size_stats if s["stack_size"] == 0), None)
            if solo and best_stack_size["stack_size"] > 0:
                delta = best_stack_size["win_rate"] - solo["win_rate"]
                if delta >= 5:
                    findings.append(
                        {
                            "severity": "info",
                            "category": "stack_size",
                            "message": (
                                f"You perform best in {best_stack_size['label']} "
                                f"({best_stack_size['win_rate']}% WR, {best_stack_size['matches']} matches) "
                                f"vs solo queue ({solo['win_rate']}% WR). "
                                f"Queue with {best_stack_size['stack_size']} recurring teammate(s)."
                            ),
                        }
                    )
                elif delta <= -5:
                    findings.append(
                        {
                            "severity": "info",
                            "category": "stack_size",
                            "message": (
                                f"You actually perform better solo ({solo['win_rate']}% WR) "
                                f"than in {best_stack_size['label']} ({best_stack_size['win_rate']}% WR). "
                                "Solo queue may be your optimal path."
                            ),
                        }
                    )

        if synergy_pairs:
            top = synergy_pairs[0]
            findings.append(
                {
                    "severity": "info",
                    "category": "synergy",
                    "message": (
                        f"Best trio: you + {top['partner_a']} + {top['partner_b']} - "
                        f"{top['win_rate']}% WR across {top['matches']} matches together."
                    ),
                }
            )

        return findings

    def _empty(self, reason: str) -> dict:
        return {
            "username": self.username,
            "baseline_win_rate": 0.0,
            "total_matches": 0,
            "partner_stats": [],
            "best_partner": None,
            "worst_partner": None,
            "most_played_with": None,
            "stack_size_stats": [],
            "best_stack_size": None,
            "synergy_pairs": [],
            "best_pair": None,
            "findings": [{"severity": "warning", "category": "data", "message": reason}],
            "error": reason,
        }
