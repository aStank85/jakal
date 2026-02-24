"""
src/plugins/v3_teammate_chemistry.py
=====================================
Analyzes teammate chemistry with citations of specific matches
where the difference was most apparent.
"""

from __future__ import annotations
from typing import Any

MIN_SHARED_MATCHES_RELIABLE = 5
MIN_SHARED_MATCHES_MENTION  = 3
MIN_TOTAL_MATCHES           = 10
MAX_CITATIONS               = 3


def _fmt_match(map_name: str, result: str, kills: int, deaths: int) -> str:
    outcome = "W" if result == "win" else "L"
    m = (map_name or "?").split()[0]
    return f"{m} ({outcome} {kills}-{deaths})"


class TeammateChemistryPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = username
        self._result: dict | None = None

    def analyze(self) -> dict:
        baseline = self._fetch_baseline()
        if baseline["total_matches"] < MIN_TOTAL_MATCHES:
            return self._empty(
                f"Only {baseline['total_matches']} matches (need {MIN_TOTAL_MATCHES})."
            )

        teammates = self._fetch_teammates()
        if not teammates:
            return self._empty("No teammate data found.")

        for t in teammates:
            t["chemistry_delta"] = round(t["win_rate"] - baseline["win_rate"], 1)
            t["reliable"]        = t["shared_matches"] >= MIN_SHARED_MATCHES_RELIABLE

        reliable    = [t for t in teammates if t["reliable"]]
        reliable.sort(key=lambda x: x["chemistry_delta"], reverse=True)
        all_sorted  = sorted(teammates, key=lambda x: x["chemistry_delta"], reverse=True)

        result = {
            "username":                self.username,
            "baseline_win_rate":       baseline["win_rate"],
            "total_matches_analyzed":  baseline["total_matches"],
            "unique_teammates_seen":   len(teammates),
            "reliable_teammate_count": len(reliable),
            "top_5":     reliable[:5],
            "bottom_5":  list(reversed(reliable[-5:])) if len(reliable) >= 5 else list(reversed(reliable)),
            "all_teammates":  all_sorted,
            "best_teammate":  reliable[0]  if reliable else None,
            "worst_teammate": reliable[-1] if reliable else None,
            "most_played_with": max(teammates, key=lambda x: x["shared_matches"]) if teammates else None,
        }
        result["findings"] = self._generate_findings(result)
        self._result = result
        return result

    def summary(self) -> None:
        r = self._result or self.analyze()
        if r.get("error"):
            print(f"[TeammateChemistry] {r['error']}")
            return

        print(f"\n{'=' * 62}")
        print(f"  TEAMMATE CHEMISTRY — {self.username}")
        print(f"  Baseline win rate: {r['baseline_win_rate']:.1f}%  "
              f"({r['total_matches_analyzed']} matches, "
              f"{r['unique_teammates_seen']} unique teammates)")
        print(f"{'=' * 62}")

        if r["top_5"]:
            print(f"\n  BEST TEAMMATES  (min {MIN_SHARED_MATCHES_RELIABLE} shared matches)")
            print(f"  {'Player':<20} {'Matches':>7} {'Win%':>6} {'Delta':>7} {'Avg KD':>7}")
            print(f"  {'-'*20} {'-'*7} {'-'*6} {'-'*7} {'-'*7}")
            for t in r["top_5"]:
                sign = "+" if t["chemistry_delta"] >= 0 else ""
                print(f"  {t['teammate']:<20} {t['shared_matches']:>7} "
                      f"{t['win_rate']:>5.1f}% {sign}{t['chemistry_delta']:>5.1f}%"
                      f"  {t['avg_teammate_kd']:>6.2f}")

        if r["bottom_5"]:
            print(f"\n  TOUGHEST QUEUES")
            print(f"  {'Player':<20} {'Matches':>7} {'Win%':>6} {'Delta':>7} {'Avg KD':>7}")
            print(f"  {'-'*20} {'-'*7} {'-'*6} {'-'*7} {'-'*7}")
            for t in r["bottom_5"]:
                sign = "+" if t["chemistry_delta"] >= 0 else ""
                print(f"  {t['teammate']:<20} {t['shared_matches']:>7} "
                      f"{t['win_rate']:>5.1f}% {sign}{t['chemistry_delta']:>5.1f}%"
                      f"  {t['avg_teammate_kd']:>6.2f}")

        print(f"\n  FINDINGS")
        for f in r["findings"]:
            sev    = f["severity"]
            marker = "!!" if sev == "critical" else "! " if sev == "warning" else "  "
            print(f"  {marker} {f['message']}")
            if f.get("citations"):
                print(f"       e.g. {f['citations']}")
        print()

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------

    def _fetch_baseline(self) -> dict:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins
            FROM match_detail_players
            WHERE username = ?
              AND EXISTS (
                    SELECT 1
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = match_detail_players.match_id
                      AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
                      AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
              )
            """, (self.username,)
        )
        row   = cur.fetchone()
        total = row[0] if row else 0
        wins  = row[1] if row else 0
        return {
            "total_matches": total,
            "wins":          wins,
            "win_rate":      round(wins / total * 100, 1) if total else 0.0,
        }

    def _fetch_teammates(self) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
                mdp2.username                                              AS teammate,
                COUNT(DISTINCT mdp1.match_id)                             AS shared_matches,
                SUM(CASE WHEN mdp1.result = 'win' THEN 1 ELSE 0 END)     AS wins,
                AVG(mdp2.kd_ratio)                                        AS avg_teammate_kd,
                AVG(mdp2.kills)                                           AS avg_teammate_kills,
                AVG(mdp2.rank_points)                                     AS avg_teammate_rp
            FROM match_detail_players mdp1
            JOIN match_detail_players mdp2
                ON  mdp1.match_id = mdp2.match_id
                AND mdp1.team_id  = mdp2.team_id
                AND mdp1.username != mdp2.username
            WHERE mdp1.username = ?
              AND mdp2.username != ''
              AND EXISTS (
                    SELECT 1
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = mdp1.match_id
                      AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
                      AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
              )
            GROUP BY mdp2.username
            HAVING COUNT(DISTINCT mdp1.match_id) >= ?
            ORDER BY shared_matches DESC
            """,
            (self.username, MIN_SHARED_MATCHES_MENTION),
        )
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        for r in rows:
            total = r["shared_matches"]
            r["win_rate"]           = round((r["wins"] or 0) / total * 100, 1) if total else 0.0
            r["avg_teammate_kd"]    = round(r["avg_teammate_kd"]    or 0.0, 2)
            r["avg_teammate_kills"] = round(r["avg_teammate_kills"]  or 0.0, 1)
            r["avg_teammate_rp"]    = int(r["avg_teammate_rp"] or 0)
        return rows

    def _fetch_shared_matches(self, teammate: str, result_filter: str | None = None) -> list[dict]:
        """
        Fetch individual match rows for player+teammate pair.
        result_filter: 'win' | 'loss' | None for all.
        """
        cur = self._conn.cursor()
        if result_filter:
            cur.execute(
                f"""
                SELECT mdp1.match_id, mdp1.result, mdp1.kills, mdp1.deaths,
                       COALESCE(smc.map_name, '?') AS map_name
                FROM match_detail_players mdp1
                JOIN match_detail_players mdp2
                    ON  mdp1.match_id = mdp2.match_id
                    AND mdp1.team_id  = mdp2.team_id
                    AND mdp2.username = ?
                LEFT JOIN scraped_match_cards smc ON mdp1.match_id = smc.match_id
                WHERE mdp1.username = ? AND mdp1.result = ?
                  AND EXISTS (
                        SELECT 1
                        FROM scraped_match_cards smc2
                        WHERE smc2.match_id = mdp1.match_id
                          AND LOWER(COALESCE(smc2.mode, '')) LIKE '%ranked%'
                          AND LOWER(COALESCE(smc2.mode, '')) NOT LIKE '%unranked%'
                  )
                ORDER BY smc.match_date DESC
                LIMIT {MAX_CITATIONS}
                """,
                (teammate, self.username, result_filter),
            )
        else:
            cur.execute(
                f"""
                SELECT mdp1.match_id, mdp1.result, mdp1.kills, mdp1.deaths,
                       COALESCE(smc.map_name, '?') AS map_name
                FROM match_detail_players mdp1
                JOIN match_detail_players mdp2
                    ON  mdp1.match_id = mdp2.match_id
                    AND mdp1.team_id  = mdp2.team_id
                    AND mdp2.username = ?
                LEFT JOIN scraped_match_cards smc ON mdp1.match_id = smc.match_id
                WHERE mdp1.username = ?
                  AND EXISTS (
                        SELECT 1
                        FROM scraped_match_cards smc2
                        WHERE smc2.match_id = mdp1.match_id
                          AND LOWER(COALESCE(smc2.mode, '')) LIKE '%ranked%'
                          AND LOWER(COALESCE(smc2.mode, '')) NOT LIKE '%unranked%'
                  )
                ORDER BY smc.match_date DESC
                LIMIT {MAX_CITATIONS}
                """,
                (teammate, self.username),
            )
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    @staticmethod
    def _cite_matches(matches: list[dict]) -> str:
        return " · ".join(
            _fmt_match(m["map_name"], m["result"], m["kills"], m["deaths"])
            for m in matches
        )

    # ------------------------------------------------------------------
    # Findings
    # ------------------------------------------------------------------

    def _generate_findings(self, r: dict) -> list[dict]:
        findings = []
        baseline = r["baseline_win_rate"]
        reliable = [t for t in r["all_teammates"] if t["reliable"]]

        def add(severity: str, message: str, teammate: str | None = None,
                result_filter: str | None = None) -> None:
            f = {"severity": severity, "message": message}
            if teammate:
                matches = self._fetch_shared_matches(teammate, result_filter)
                if matches:
                    f["citations"] = self._cite_matches(matches)
            findings.append(f)

        if not reliable:
            add("info",
                f"Not enough shared matches with any teammate to compute reliable chemistry yet. "
                f"Need {MIN_SHARED_MATCHES_RELIABLE}+ games together.")
            return findings

        best  = r["best_teammate"]
        worst = r["worst_teammate"]
        most_played = r["most_played_with"]

        if best and best["chemistry_delta"] >= 8:
            add("info",
                f"With {best['teammate']}: {best['win_rate']:.0f}% win rate ({best['shared_matches']} matches). "
                f"Baseline: {baseline:.0f}%. That's +{best['chemistry_delta']:.0f}% — queue together whenever possible.",
                best["teammate"], "win")
        elif best and best["chemistry_delta"] > 0:
            add("info",
                f"Best chemistry: {best['teammate']} at {best['win_rate']:.0f}% WR "
                f"(+{best['chemistry_delta']:.0f}% vs baseline, {best['shared_matches']} matches).",
                best["teammate"], "win")

        if worst and worst["chemistry_delta"] <= -8:
            add("warning",
                f"With {worst['teammate']}: {worst['win_rate']:.0f}% win rate ({worst['shared_matches']} matches). "
                f"That's {abs(worst['chemistry_delta']):.0f}% below baseline. "
                f"Consider whether this queue is worth it.",
                worst["teammate"], "loss")
        elif worst and worst["chemistry_delta"] < 0:
            add("info",
                f"Toughest queue: {worst['teammate']} at {worst['win_rate']:.0f}% WR "
                f"({worst['chemistry_delta']:.0f}% vs baseline).",
                worst["teammate"], "loss")

        if (most_played and
                most_played["teammate"] != (best["teammate"]  if best  else "") and
                most_played["teammate"] != (worst["teammate"] if worst else "")):
            delta = most_played["chemistry_delta"]
            sign  = "+" if delta >= 0 else ""
            add("info",
                f"Most played with: {most_played['teammate']} "
                f"({most_played['shared_matches']} matches, {most_played['win_rate']:.0f}% WR, "
                f"{sign}{delta:.0f}% vs baseline). "
                + ("Roughly neutral — comfortable but not boosting." if abs(delta) < 5
                   else "Chemistry is meaningful at this sample size."),
                most_played["teammate"])

        if len(reliable) >= 3:
            spread = reliable[0]["win_rate"] - reliable[-1]["win_rate"]
            if spread >= 25:
                add("warning",
                    f"Your win rate swings {spread:.0f}% depending on who you queue with "
                    f"({reliable[0]['teammate']} {reliable[0]['win_rate']:.0f}% "
                    f"vs {reliable[-1]['teammate']} {reliable[-1]['win_rate']:.0f}%). "
                    f"Teammate selection is one of your biggest controllable levers.")
            elif spread >= 15:
                add("info",
                    f"Notable chemistry spread: {spread:.0f}% gap between best and worst queue "
                    f"across {len(reliable)} teammates.")

        return findings

    @staticmethod
    def _empty(reason: str) -> dict:
        return {"error": reason, "findings": []}


if __name__ == "__main__":
    import argparse, sqlite3
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",       required=True)
    parser.add_argument("--username", required=True)
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    TeammateChemistryPlugin(conn, args.username).summary()
