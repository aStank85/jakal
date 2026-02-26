"""
src/plugins/v3_enemy_operator_threat.py
=======================================
Analyzes which enemy operators most often kill a player and their impact
on round outcomes.
"""

from __future__ import annotations

from typing import Any

MIN_ENCOUNTERS = 2


class EnemyOperatorThreatPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = str(username or "").strip()
        self._result: dict | None = None

    def analyze(self) -> dict:
        baseline = self._fetch_baseline()
        if not baseline["total_rounds"]:
            return self._empty("No ranked round data found for this player.")

        rows = self._fetch_threat_rows()
        if not rows:
            return self._empty("No deaths with resolved enemy operators found yet.")

        points = []
        threats = []
        for row in rows:
            encounters = int(row["times_killed_by"] or 0)
            if encounters <= 0:
                continue
            round_losses = int(row["round_losses"] or 0)
            wins_when_present = encounters - round_losses
            win_rate_when_present = (wins_when_present / encounters) * 100.0
            presence_pct = (encounters / baseline["total_rounds"]) * 100.0
            win_delta = win_rate_when_present - baseline["baseline_win_rate"]
            threat = {
                "operator": row["killed_by_operator"],
                "times_killed_by": encounters,
                "round_losses": round_losses,
                "round_wins": wins_when_present,
                "presence_pct": round(presence_pct, 1),
                "win_rate_when_present": round(win_rate_when_present, 1),
                "win_delta": round(win_delta, 1),
                "is_reliable": encounters >= MIN_ENCOUNTERS,
            }
            threats.append(threat)
            points.append(
                {
                    "operator": threat["operator"],
                    "presence_pct": threat["presence_pct"],
                    "win_delta": threat["win_delta"],
                    "times_killed_by": threat["times_killed_by"],
                    "round_losses": threat["round_losses"],
                }
            )

        threats.sort(key=lambda x: (-x["times_killed_by"], x["win_delta"]))
        points.sort(key=lambda x: -x["times_killed_by"])
        result = {
            "username": self.username,
            "total_rounds": baseline["total_rounds"],
            "total_death_rounds": baseline["total_death_rounds"],
            "baseline_win_rate": baseline["baseline_win_rate"],
            "threats": threats,
            "scatter": {
                "x_label": "Presence (% of rounds)",
                "y_label": "Win delta vs baseline (%)",
                "points": points,
            },
            "findings": self._findings(threats),
        }
        self._result = result
        return result

    def summary(self) -> None:
        result = self._result or self.analyze()
        if result.get("error"):
            print(f"[EnemyOperatorThreat] {result['error']}")
            return

        print(f"\n{'=' * 62}")
        print(f"  ENEMY OPERATOR THREAT — {self.username}")
        print(f"{'=' * 62}")
        print(
            f"  Baseline round WR: {result['baseline_win_rate']:.1f}% "
            f"({result['total_rounds']} rounds)"
        )
        print(
            f"  {'Operator':<14} {'Kills':>5} {'Losses':>6} {'Presence':>9} {'Delta':>8}"
        )
        for row in result["threats"][:10]:
            sign = "+" if row["win_delta"] >= 0 else ""
            print(
                f"  {row['operator']:<14} {row['times_killed_by']:>5} {row['round_losses']:>6} "
                f"{row['presence_pct']:>8.1f}% {sign}{row['win_delta']:>7.1f}%"
            )

    def _fetch_baseline(self) -> dict:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_rounds,
                SUM(CASE WHEN pr.deaths = 1 THEN 1 ELSE 0 END) AS total_death_rounds,
                SUM(CASE WHEN pr.result = 'victory' THEN 1 ELSE 0 END) AS total_wins
            FROM player_rounds pr
            WHERE pr.username = ?
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
        row = cur.fetchone()
        total_rounds = int(row["total_rounds"] or 0)
        total_wins = int(row["total_wins"] or 0)
        return {
            "total_rounds": total_rounds,
            "total_death_rounds": int(row["total_death_rounds"] or 0),
            "baseline_win_rate": round((total_wins / total_rounds * 100.0), 1) if total_rounds else 0.0,
        }

    def _fetch_threat_rows(self) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
                killed_by_operator,
                COUNT(*) as times_killed_by,
                SUM(CASE WHEN result='defeat' THEN 1 ELSE 0 END) as round_losses
            FROM player_rounds
            WHERE player_id = (
                    SELECT player_id
                    FROM players
                    WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))
                    ORDER BY player_id DESC
                    LIMIT 1
                )
              AND deaths = 1
              AND killed_by_operator IS NOT NULL
              AND TRIM(killed_by_operator) != ''
              AND EXISTS (
                    SELECT 1
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = player_rounds.match_id
                      AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
                      AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
              )
            GROUP BY killed_by_operator
            """,
            (self.username,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def _findings(threats: list[dict]) -> list[dict]:
        findings = []
        if not threats:
            return findings

        top = threats[0]
        findings.append(
            {
                "severity": "warning",
                "message": (
                    f"Top killer: {top['operator']} ({top['times_killed_by']} death rounds, "
                    f"{top['presence_pct']}% presence proxy, {top['win_delta']}% WR delta)."
                ),
            }
        )
        high_impact = [t for t in threats if t["is_reliable"] and t["win_delta"] <= -10]
        if high_impact:
            picks = ", ".join(t["operator"] for t in high_impact[:4])
            findings.append(
                {
                    "severity": "warning",
                    "message": f"Most punishing enemy operators for your rounds: {picks}.",
                }
            )
        return findings

    def _empty(self, reason: str) -> dict:
        return {
            "username": self.username,
            "total_rounds": 0,
            "total_death_rounds": 0,
            "baseline_win_rate": 0.0,
            "threats": [],
            "scatter": {"x_label": "Presence (% of rounds)", "y_label": "Win delta vs baseline (%)", "points": []},
            "findings": [{"severity": "warning", "message": reason}],
            "error": reason,
        }


if __name__ == "__main__":
    import argparse
    import sqlite3

    parser = argparse.ArgumentParser(description="V3 Enemy Operator Threat Plugin")
    parser.add_argument("--db", required=True)
    parser.add_argument("--username", required=True)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    plugin = EnemyOperatorThreatPlugin(conn, args.username)
    print(plugin.analyze())
