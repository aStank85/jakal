"""
src/plugins/v2_operator_stats.py
================================
Per-operator performance from player_rounds data (ranked matches only).
"""

from __future__ import annotations

from typing import Any

MIN_ROUNDS = 5
MIN_MENTION_ROUNDS = 3
CORE_WIN_PCT_FLOOR = 40.0
CUT_LIST_LIMIT = 3

ATK_BASELINE = 48.0
DEF_BASELINE = 52.0


class OperatorStatsPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = str(username or "").strip()
        self._result: dict | None = None

    def analyze(self) -> dict:
        rows = self._fetch_operator_rounds()
        if not rows:
            return self._empty_result("No ranked round data found")

        filtered_rows = [row for row in rows if self._is_valid_operator_name(row[0])]
        ops = self._aggregate(filtered_rows)
        reliable = [o for o in ops if o["rounds"] >= MIN_ROUNDS]
        mentioned = [o for o in ops if o["rounds"] >= MIN_MENTION_ROUNDS]

        atk_ops = sorted(
            [o for o in reliable if o["side"] == "attacker" and o["win_pct"] >= CORE_WIN_PCT_FLOOR],
            key=lambda x: (-x["win_pct"], -x["rounds"]),
        )
        def_ops = sorted(
            [o for o in reliable if o["side"] == "defender" and o["win_pct"] >= CORE_WIN_PCT_FLOOR],
            key=lambda x: (-x["win_pct"], -x["rounds"]),
        )

        core_attack = atk_ops[:3]
        core_defense = def_ops[:3]

        cut_list = sorted(
            [o for o in reliable if o["edge"] <= -10.0],
            key=lambda x: (-x["rounds"], x["edge"]),
        )[:CUT_LIST_LIMIT]
        most_played = max(ops, key=lambda x: x["rounds"]) if ops else None
        diversity_score = len(mentioned)
        best_op = max(reliable, key=lambda x: x["win_pct"], default=None)
        worst_op = min(reliable, key=lambda x: x["win_pct"], default=None)

        result = {
            "username": self.username,
            "operators": ops,
            "core_attack": core_attack,
            "core_defense": core_defense,
            "cut_list": cut_list,
            "best_operator": best_op,
            "worst_operator": worst_op,
            "most_played_operator": most_played,
            "diversity_score": diversity_score,
            "total_rounds_analyzed": sum(o["rounds"] for o in ops),
            "findings": self._generate_findings(
                core_attack=core_attack,
                core_defense=core_defense,
                cut_list=cut_list,
                best_op=best_op,
                diversity_score=diversity_score,
                all_ops=mentioned,
            ),
        }
        self._result = result
        return result

    def summary(self) -> None:
        result = self._result or self.analyze()
        if result.get("error"):
            print(f"[OperatorStats] {result['error']}")
            return

        print(f"\n=== OPERATOR STATS: {result['username']} ===")
        print(f"Total ranked rounds analyzed: {result['total_rounds_analyzed']}")
        print(f"Operator diversity: {result['diversity_score']} ops with 3+ rounds")
        print("\n--- ALL OPERATORS (3+ rounds) ---")
        print(f"  {'Operator':<14} {'Side':<10} {'Rnd':>4} {'Win%':>6} {'Edge':>6} {'Avg K':>6} {'FB%':>5}")
        for op in result["operators"]:
            if op["rounds"] < MIN_MENTION_ROUNDS:
                continue
            sign = "+" if op["edge"] >= 0 else ""
            print(
                f"  {op['operator']:<14} {op['side']:<10} {op['rounds']:>4} "
                f"{op['win_pct']:>5.1f}% {sign}{op['edge']:>5.1f}% "
                f"{op['avg_kills']:>6.2f} {op['first_blood_rate']:>4.0f}%"
            )
        print("\n--- FINDINGS ---")
        for finding in result["findings"]:
            print(f"  [{finding['severity'].upper()}] {finding['message']}")

    def _fetch_operator_rounds(self) -> list[tuple]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
                pr.operator,
                pr.side,
                pr.result,
                pr.kills,
                pr.deaths,
                pr.first_blood,
                pr.clutch_won,
                pr.clutch_lost
            FROM player_rounds pr
            WHERE pr.username = ?
              AND pr.operator IS NOT NULL
              AND pr.operator != ''
              AND EXISTS (
                    SELECT 1
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = pr.match_id
                      AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
                      AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
              )
            """,
            (self.username,),
        )
        return cur.fetchall()

    @staticmethod
    def _aggregate(rows: list[tuple]) -> list[dict]:
        agg: dict[tuple, dict] = {}

        for operator, side, result, kills, deaths, first_blood, clutch_won, clutch_lost in rows:
            key = (operator, side)
            if key not in agg:
                agg[key] = {
                    "operator": operator,
                    "side": side,
                    "rounds": 0,
                    "wins": 0,
                    "kills": 0,
                    "deaths": 0,
                    "first_bloods": 0,
                    "clutches_won": 0,
                    "clutches_lost": 0,
                }
            item = agg[key]
            item["rounds"] += 1
            if result in ("victory", "win"):
                item["wins"] += 1
            item["kills"] += kills or 0
            item["deaths"] += deaths or 0
            item["first_bloods"] += first_blood or 0
            item["clutches_won"] += clutch_won or 0
            item["clutches_lost"] += clutch_lost or 0

        results = []
        for item in agg.values():
            n = item["rounds"]
            win_pct = item["wins"] / n * 100 if n > 0 else 0.0
            baseline = ATK_BASELINE if item["side"] == "attacker" else DEF_BASELINE
            edge = win_pct - baseline
            avg_kills = item["kills"] / n if n > 0 else 0.0
            first_blood_rate = item["first_bloods"] / n * 100 if n > 0 else 0.0
            total_clutches = item["clutches_won"] + item["clutches_lost"]
            clutch_rate = item["clutches_won"] / total_clutches * 100 if total_clutches > 0 else 0.0
            kd = item["kills"] / item["deaths"] if item["deaths"] > 0 else float(item["kills"])
            results.append(
                {
                    "operator": item["operator"],
                    "side": item["side"],
                    "rounds": n,
                    "wins": item["wins"],
                    "win_pct": round(win_pct, 1),
                    "edge": round(edge, 1),
                    "avg_kills": round(avg_kills, 2),
                    "kd": round(kd, 2),
                    "first_blood_rate": round(first_blood_rate, 1),
                    "clutch_rate": round(clutch_rate, 1),
                    "clutches_won": item["clutches_won"],
                    "clutches_lost": item["clutches_lost"],
                }
            )

        results.sort(key=lambda x: (-x["rounds"], -x["win_pct"]))
        return results

    @staticmethod
    def _generate_findings(
        core_attack: list[dict],
        core_defense: list[dict],
        cut_list: list[dict],
        best_op: dict | None,
        diversity_score: int,
        all_ops: list[dict],
    ) -> list[dict]:
        findings = []

        if core_attack:
            text = ", ".join(f"{o['operator']} ({o['win_pct']}%, {o['rounds']}rnd)" for o in core_attack)
            findings.append(
                {
                    "severity": "info",
                    "category": "operator_pool",
                    "message": f"Core attack pool: {text}. Stick to these.",
                }
            )

        if core_defense:
            text = ", ".join(f"{o['operator']} ({o['win_pct']}%, {o['rounds']}rnd)" for o in core_defense)
            findings.append(
                {
                    "severity": "info",
                    "category": "operator_pool",
                    "message": f"Core defense pool: {text}. Stick to these.",
                }
            )

        for op in cut_list:
            findings.append(
                {
                    "severity": "warning",
                    "category": "cut_list",
                    "message": (
                        f"Drop {op['operator']} ({op['side']}). "
                        f"{op['win_pct']}% win rate over {op['rounds']} rounds "
                        f"({op['edge']:+.0f}% vs baseline). You are losing rounds on this pick."
                    ),
                }
            )

        if best_op and best_op["edge"] >= 10:
            findings.append(
                {
                    "severity": "info",
                    "category": "strength",
                    "message": (
                        f"{best_op['operator']} is your strongest operator - "
                        f"{best_op['win_pct']}% win rate ({best_op['edge']:+.0f}% above baseline) "
                        f"over {best_op['rounds']} rounds."
                    ),
                }
            )

        first_blood_ops = sorted(
            [o for o in all_ops if o["first_blood_rate"] >= 20 and OperatorStatsPlugin._is_valid_operator_name(o.get("operator"))],
            key=lambda x: -x["first_blood_rate"],
        )
        if first_blood_ops:
            top = first_blood_ops[0]
            findings.append(
                {
                    "severity": "info",
                    "category": "first_blood",
                    "message": (
                        f"{top['operator']} is your best entry pick - "
                        f"{top['first_blood_rate']:.0f}% first blood rate over {top['rounds']} rounds."
                    ),
                }
            )

        if diversity_score > 15:
            findings.append(
                {
                    "severity": "info",
                    "category": "diversity",
                    "message": (
                        f"High operator diversity ({diversity_score} ops with 3+ rounds). "
                        "Consider narrowing to a tighter pool for consistency."
                    ),
                }
            )
        elif diversity_score <= 3:
            findings.append(
                {
                    "severity": "warning",
                    "category": "diversity",
                    "message": (
                        f"Low operator diversity ({diversity_score} ops with 3+ rounds). "
                        "Enemies can predict your picks. Expand your pool."
                    ),
                }
            )

        return findings

    @staticmethod
    def _is_valid_operator_name(value: Any) -> bool:
        name = str(value or "").strip()
        if not name:
            return False
        lowered = name.lower()
        return lowered not in {"unknown", "n/a", "none", "null", "?"}

    def _empty_result(self, reason: str) -> dict:
        return {
            "username": self.username,
            "operators": [],
            "core_attack": [],
            "core_defense": [],
            "cut_list": [],
            "best_operator": None,
            "worst_operator": None,
            "most_played_operator": None,
            "diversity_score": 0,
            "total_rounds_analyzed": 0,
            "findings": [{"severity": "warning", "category": "data", "message": reason}],
            "error": reason,
        }
