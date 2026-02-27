"""
src/plugins/v2_map_stats.py
===========================
Per-map win rate and side breakdown from ranked data.
"""

from __future__ import annotations

from typing import Any

MIN_MATCHES = 3
MIN_MENTION = 2

RP_WIN = 25.0
RP_LOSS = -20.0
NEUTRAL_EXPECTED_RP = (0.5 * RP_WIN) + (0.5 * RP_LOSS)

SIDE_GAP_THRESHOLD = 20.0


class MapStatsPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = str(username or "").strip()
        self._result: dict | None = None

    def analyze(self) -> dict:
        match_rows = self._fetch_match_results()
        round_rows = self._fetch_round_sides()

        if not match_rows:
            return self._empty_result("No ranked match data found")

        maps = self._aggregate(match_rows, round_rows)
        reliable = [m for m in maps if m["matches"] >= MIN_MATCHES]
        mentioned = [m for m in maps if m["matches"] >= MIN_MENTION]

        best_map = max(reliable, key=lambda x: x["win_pct"], default=None)
        worst_map = min(reliable, key=lambda x: x["win_pct"], default=None)
        ban_recommendation = min(reliable, key=lambda x: x["win_pct"], default=None)

        side_weak_maps = [
            m for m in mentioned
            if m["side_gap"] is not None and m["side_gap"] >= SIDE_GAP_THRESHOLD
        ]
        rp_drain_maps = [
            m for m in mentioned
            if m["expected_rp"] is not None and m["expected_rp"] < 0
        ]

        result = {
            "username": self.username,
            "maps": maps,
            "best_map": best_map,
            "worst_map": worst_map,
            "ban_recommendation": ban_recommendation,
            "side_weak_maps": side_weak_maps,
            "rp_drain_maps": rp_drain_maps,
            "total_matches_analyzed": len(match_rows),
            "findings": self._generate_findings(
                maps=mentioned,
                best_map=best_map,
                ban_recommendation=ban_recommendation,
                side_weak_maps=side_weak_maps,
                rp_drain_maps=rp_drain_maps,
            ),
        }
        self._result = result
        return result

    def summary(self) -> None:
        result = self._result or self.analyze()
        if result.get("error"):
            print(f"[MapStats] {result['error']}")
            return

        print(f"\n=== MAP STATS: {result['username']} ===")
        print(f"Ranked matches analyzed: {result['total_matches_analyzed']}")
        print(f"\n  {'Map':<22} {'M':>3} {'Win%':>6} {'ATK%':>6} {'DEF%':>6} {'Gap':>5} {'AvgRP':>7}")
        for m in result["maps"]:
            atk = f"{m['atk_win_pct']:.0f}%" if m["atk_win_pct"] is not None else "  -  "
            dfn = f"{m['def_win_pct']:.0f}%" if m["def_win_pct"] is not None else "  -  "
            gap = f"{m['side_gap']:.0f}%" if m["side_gap"] is not None else "  -"
            print(
                f"  {m['map_name']:<22} {m['matches']:>3} "
                f"{m['win_pct']:>5.1f}% {atk:>6} {dfn:>6} {gap:>5} "
                f"{m['avg_rp_delta']:>+7.1f}"
            )

        print("\n--- FINDINGS ---")
        for finding in result["findings"]:
            print(f"  [{finding['severity'].upper()}] {finding['message']}")

    def _fetch_match_results(self) -> list[tuple]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT smc.map_name, mdp.result, mdp.rank_points_delta
            FROM match_detail_players mdp
            JOIN scraped_match_cards smc ON smc.match_id = mdp.match_id
            WHERE mdp.username = ?
              AND mdp.match_type = 'Ranked'
              AND smc.map_name IS NOT NULL
            """,
            (self.username,),
        )
        return cur.fetchall()

    def _fetch_round_sides(self) -> list[tuple]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT smc.map_name, pr.side, pr.result
            FROM player_rounds pr
            JOIN scraped_match_cards smc ON smc.match_id = pr.match_id
            WHERE pr.username = ?
              AND pr.match_type = 'Ranked'
              AND smc.map_name IS NOT NULL
              AND pr.side IS NOT NULL
            """,
            (self.username,),
        )
        return cur.fetchall()

    @staticmethod
    def _aggregate(match_rows: list[tuple], round_rows: list[tuple]) -> list[dict]:
        match_agg: dict[str, dict] = {}
        for map_name, result, rp_delta in match_rows:
            if map_name not in match_agg:
                match_agg[map_name] = {"matches": 0, "wins": 0, "total_rp": 0.0}
            match_agg[map_name]["matches"] += 1
            if result == "win":
                match_agg[map_name]["wins"] += 1
            match_agg[map_name]["total_rp"] += rp_delta or 0.0

        round_agg: dict[str, dict] = {}
        for map_name, side, result in round_rows:
            if map_name not in round_agg:
                round_agg[map_name] = {"atk_rounds": 0, "atk_wins": 0, "def_rounds": 0, "def_wins": 0}
            row = round_agg[map_name]
            if side == "attacker":
                row["atk_rounds"] += 1
                if result in ("victory", "win"):
                    row["atk_wins"] += 1
            elif side == "defender":
                row["def_rounds"] += 1
                if result in ("victory", "win"):
                    row["def_wins"] += 1

        results = []
        for map_name, md in match_agg.items():
            n = md["matches"]
            win_pct = md["wins"] / n * 100 if n > 0 else 0.0
            avg_rp = md["total_rp"] / n if n > 0 else 0.0
            expected_rp = (win_pct / 100 * RP_WIN) + ((1 - win_pct / 100) * RP_LOSS)
            rp_above_neutral = expected_rp - NEUTRAL_EXPECTED_RP

            rd = round_agg.get(map_name, {})
            atk_rounds = rd.get("atk_rounds", 0)
            def_rounds = rd.get("def_rounds", 0)
            atk_win_pct = (rd.get("atk_wins", 0) / atk_rounds * 100) if atk_rounds > 0 else None
            def_win_pct = (rd.get("def_wins", 0) / def_rounds * 100) if def_rounds > 0 else None
            if atk_win_pct is not None and def_win_pct is not None:
                side_gap = abs(atk_win_pct - def_win_pct)
                weak_side = "attack" if atk_win_pct < def_win_pct else "defense"
            else:
                side_gap = None
                weak_side = None

            results.append(
                {
                    "map_name": map_name,
                    "matches": n,
                    "wins": md["wins"],
                    "win_pct": round(win_pct, 1),
                    "avg_rp_delta": round(avg_rp, 1),
                    "expected_rp": round(expected_rp, 1),
                    "rp_above_neutral": round(rp_above_neutral, 1),
                    "atk_rounds": atk_rounds,
                    "def_rounds": def_rounds,
                    "atk_win_pct": round(atk_win_pct, 1) if atk_win_pct is not None else None,
                    "def_win_pct": round(def_win_pct, 1) if def_win_pct is not None else None,
                    "side_gap": round(side_gap, 1) if side_gap is not None else None,
                    "weak_side": weak_side,
                }
            )

        results.sort(key=lambda x: (-x["matches"], -x["win_pct"]))
        return results

    @staticmethod
    def _generate_findings(
        maps: list[dict],
        best_map: dict | None,
        ban_recommendation: dict | None,
        side_weak_maps: list[dict],
        rp_drain_maps: list[dict],
    ) -> list[dict]:
        findings = []

        if best_map and best_map["win_pct"] >= 55:
            findings.append(
                {
                    "severity": "info",
                    "category": "map_pool",
                    "message": (
                        f"Best map: {best_map['map_name']} at {best_map['win_pct']}% WR "
                        f"({best_map['matches']} matches, +{best_map['rp_above_neutral']:.1f} RP vs neutral). "
                        "Try to keep this in the pool."
                    ),
                }
            )

        if ban_recommendation and ban_recommendation["win_pct"] <= 40:
            findings.append(
                {
                    "severity": "warning",
                    "category": "ban",
                    "message": (
                        f"Ban {ban_recommendation['map_name']}. "
                        f"{ban_recommendation['win_pct']}% WR over {ban_recommendation['matches']} matches "
                        f"({ban_recommendation['rp_above_neutral']:.1f} RP vs neutral per match)."
                    ),
                }
            )

        for item in rp_drain_maps:
            if ban_recommendation and item["map_name"] == ban_recommendation["map_name"]:
                continue
            findings.append(
                {
                    "severity": "warning",
                    "category": "rp_drain",
                    "message": (
                        f"{item['map_name']} is costing you RP - "
                        f"{item['win_pct']}% WR, expected {item['expected_rp']:.1f} RP/match "
                        f"({item['rp_above_neutral']:.1f} vs neutral). {item['matches']} matches."
                    ),
                }
            )

        for item in sorted(side_weak_maps, key=lambda x: -x["side_gap"])[:3]:
            findings.append(
                {
                    "severity": "warning",
                    "category": "side_weakness",
                    "message": (
                        f"{item['map_name']}: {item['side_gap']:.0f}% gap between sides - "
                        f"ATK {item['atk_win_pct']}% / DEF {item['def_win_pct']}%. "
                        f"Weak {item['weak_side']} side. "
                        f"({item['atk_rounds']}atk / {item['def_rounds']}def rounds)"
                    ),
                }
            )

        strong = [
            m for m in maps
            if m["win_pct"] >= 60 and m["matches"] >= MIN_MENTION
            and (best_map is None or m["map_name"] != best_map["map_name"])
        ]
        for item in strong[:2]:
            findings.append(
                {
                    "severity": "info",
                    "category": "map_pool",
                    "message": f"{item['map_name']} is a strong map for you - {item['win_pct']}% WR over {item['matches']} matches.",
                }
            )

        return findings

    def _empty_result(self, reason: str) -> dict:
        return {
            "username": self.username,
            "maps": [],
            "best_map": None,
            "worst_map": None,
            "ban_recommendation": None,
            "side_weak_maps": [],
            "rp_drain_maps": [],
            "total_matches_analyzed": 0,
            "findings": [{"severity": "warning", "category": "data", "message": reason}],
            "error": reason,
        }
