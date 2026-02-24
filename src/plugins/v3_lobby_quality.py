"""
src/plugins/v3_lobby_quality.py
================================
Analyzes lobby quality (enemy RP bracket distribution) with citations
of specific matches where conditions were most apparent.
"""

from __future__ import annotations
from collections import defaultdict
from typing import Any

BRACKETS = [
    (0,    1499,  "Copper/Bronze"),
    (1500, 1999,  "Bronze/Silver"),
    (2000, 2499,  "Silver"),
    (2500, 2999,  "Gold"),
    (3000, 3499,  "Platinum"),
    (3500, 3999,  "Emerald"),
    (4000, 4499,  "Diamond"),
    (4500, 9999,  "Champions"),
]

MIN_VALID_ENEMY_RP  = 100
MIN_BRACKET_MATCHES = 2
MIN_TOTAL_MATCHES   = 8
MISMATCH_THRESHOLD  = 200
MAX_CITATIONS       = 3


def _bracket_label(rp: float) -> str:
    for lo, hi, label in BRACKETS:
        if lo <= rp <= hi:
            return label
    return "Unknown"


def _fmt_match(map_name: str, result: str, my_rp: int, enemy_avg_rp: float) -> str:
    outcome = "W" if result == "win" else "L"
    m = (map_name or "?").split()[0]
    return f"{m} ({outcome}, you {my_rp:,} vs avg {enemy_avg_rp:,.0f})"


class LobbyQualityPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = username
        self._result: dict | None = None

    def analyze(self) -> dict:
        matches = self._fetch_match_lobby_data()
        matches = [m for m in matches if m["my_rp"] >= MIN_VALID_ENEMY_RP]

        if len(matches) < MIN_TOTAL_MATCHES:
            return self._empty(
                f"Only {len(matches)} usable matches (need {MIN_TOTAL_MATCHES})."
            )

        for m in matches:
            m["rp_diff"]       = m["my_rp"] - m["enemy_avg_rp"]
            m["enemy_bracket"] = _bracket_label(m["enemy_avg_rp"])
            m["win"]           = 1 if m["result"] == "win" else 0

        result = {"username": self.username, "matches_analyzed": len(matches)}
        result.update(self._overall_lobby_stats(matches))
        result.update(self._bracket_breakdown(matches))
        result.update(self._mismatch_analysis(matches))
        result["findings"] = self._generate_findings(result, matches)

        self._result = result
        return result

    def summary(self) -> None:
        r = self._result or self.analyze()
        if r.get("error"):
            print(f"[LobbyQuality] {r['error']}")
            return

        print(f"\n{'=' * 62}")
        print(f"  LOBBY QUALITY — {self.username}")
        print(f"  {r['matches_analyzed']} matches analyzed")
        print(f"{'=' * 62}")

        diff      = r["avg_rp_diff"]
        sign      = "+" if diff >= 0 else ""
        direction = ("punching down" if diff > MISMATCH_THRESHOLD else
                     "punching up"   if diff < -MISMATCH_THRESHOLD else "well matched")
        print(f"\n  OVERALL LOBBY STATS")
        print(f"    Your avg RP            : {r['avg_my_rp']:,.0f}  ({r['my_bracket']})")
        print(f"    Avg enemy lobby RP     : {r['avg_enemy_rp']:,.0f}")
        print(f"    RP mismatch (you-enemy): {sign}{diff:,.0f}  ({direction})")
        print(f"    Overall win rate       : {r['overall_win_rate']:.1f}%")

        if r.get("bracket_data"):
            print(f"\n  WIN RATE BY ENEMY BRACKET")
            print(f"  {'Bracket':<18} {'Matches':>7} {'Win%':>6}")
            print(f"  {'-'*18} {'-'*7} {'-'*6}")
            for b in r["bracket_data"]:
                note = " ← you" if b["label"] == r.get("my_bracket") else ""
                print(f"  {b['label']:<18} {b['matches']:>7} {b['win_rate']:>5.1f}%{note}")

        print(f"\n  PERFORMANCE BY MATCHUP TYPE")
        print(f"    vs higher RP  ({r['higher_count']:>2} matches): {r['win_rate_vs_higher']:.1f}%")
        print(f"    vs even RP    ({r['even_count']:>2} matches): {r['win_rate_vs_even']:.1f}%")
        print(f"    vs lower RP   ({r['lower_count']:>2} matches): {r['win_rate_vs_lower']:.1f}%")

        print(f"\n  FINDINGS")
        for f in r["findings"]:
            sev    = f["severity"]
            marker = "!!" if sev == "critical" else "! " if sev == "warning" else "  "
            print(f"  {marker} {f['message']}")
            if f.get("citations"):
                print(f"       e.g. {f['citations']}")
        print()

    # ------------------------------------------------------------------
    # Data fetch
    # ------------------------------------------------------------------

    def _fetch_match_lobby_data(self) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
                mdp_me.match_id,
                mdp_me.result,
                mdp_me.rank_points AS my_rp,
                COALESCE(
                    (
                        SELECT smc2.map_name
                        FROM scraped_match_cards smc2
                        WHERE smc2.match_id = mdp_me.match_id
                        ORDER BY smc2.id DESC
                        LIMIT 1
                    ),
                    '?'
                ) AS map_name,
                AVG(CASE WHEN mdp_enemy.rank_points > :min_rp
                         THEN mdp_enemy.rank_points ELSE NULL END)   AS enemy_avg_rp,
                COUNT(CASE WHEN mdp_enemy.rank_points > :min_rp
                           THEN 1 ELSE NULL END)                     AS valid_enemy_count,
                AVG(CASE WHEN mdp_enemy.rank_points > :min_rp
                         THEN mdp_enemy.kd_ratio ELSE NULL END)      AS enemy_avg_kd
            FROM match_detail_players mdp_me
            JOIN match_detail_players mdp_enemy
                ON  mdp_me.match_id  = mdp_enemy.match_id
                AND mdp_me.team_id  != mdp_enemy.team_id
                AND mdp_enemy.username != ''
            WHERE mdp_me.username = :username
              AND EXISTS (
                    SELECT 1
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = mdp_me.match_id
                      AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
                      AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
              )
            GROUP BY mdp_me.match_id
            HAVING valid_enemy_count >= 2
            ORDER BY (
                SELECT smc3.match_date
                FROM scraped_match_cards smc3
                WHERE smc3.match_id = mdp_me.match_id
                ORDER BY smc3.id DESC
                LIMIT 1
            ) DESC
            """,
            {"min_rp": MIN_VALID_ENEMY_RP, "username": self.username},
        )
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        for r in rows:
            r["my_rp"]        = int(r["my_rp"] or 0)
            r["enemy_avg_rp"] = round(float(r["enemy_avg_rp"] or 0), 0)
            r["enemy_avg_kd"] = round(float(r["enemy_avg_kd"] or 0), 2)
        return rows

    @staticmethod
    def _cite_matches(matches: list[dict], n: int = MAX_CITATIONS) -> str:
        return " · ".join(
            _fmt_match(m["map_name"], m["result"], m["my_rp"], m["enemy_avg_rp"])
            for m in matches[:n]
        )

    # ------------------------------------------------------------------
    # Analysis modules
    # ------------------------------------------------------------------

    def _overall_lobby_stats(self, matches: list[dict]) -> dict:
        total     = len(matches)
        wins      = sum(m["win"] for m in matches)
        avg_mine  = sum(m["my_rp"] for m in matches) / total
        avg_enemy = sum(m["enemy_avg_rp"] for m in matches) / total
        return {
            "avg_my_rp":        round(avg_mine, 0),
            "avg_enemy_rp":     round(avg_enemy, 0),
            "avg_rp_diff":      round(avg_mine - avg_enemy, 0),
            "my_bracket":       _bracket_label(avg_mine),
            "overall_win_rate": round(wins / total * 100, 1),
        }

    def _bracket_breakdown(self, matches: list[dict]) -> dict:
        by_bracket: dict[str, list] = defaultdict(list)
        for m in matches:
            by_bracket[m["enemy_bracket"]].append(m)

        bracket_data = []
        for _, _, label in BRACKETS:
            group = by_bracket.get(label, [])
            if len(group) < MIN_BRACKET_MATCHES:
                continue
            wins = sum(m["win"] for m in group)
            bracket_data.append({
                "label":    label,
                "matches":  len(group),
                "wins":     wins,
                "win_rate": round(wins / len(group) * 100, 1),
                "_matches": group,    # raw for citations
            })

        best    = max(bracket_data, key=lambda x: x["win_rate"]) if bracket_data else None
        hardest = min(bracket_data, key=lambda x: x["win_rate"]) if bracket_data else None
        return {
            "bracket_data":             bracket_data,
            "best_bracket":             best["label"]    if best    else None,
            "best_bracket_win_rate":    best["win_rate"] if best    else None,
            "hardest_bracket":          hardest["label"]    if hardest else None,
            "hardest_bracket_win_rate": hardest["win_rate"] if hardest else None,
        }

    def _mismatch_analysis(self, matches: list[dict]) -> dict:
        higher = [m for m in matches if m["rp_diff"] < -MISMATCH_THRESHOLD]
        lower  = [m for m in matches if m["rp_diff"] >  MISMATCH_THRESHOLD]
        even   = [m for m in matches if abs(m["rp_diff"]) <= MISMATCH_THRESHOLD]

        def wr(group):
            if not group:
                return 0.0
            return round(sum(m["win"] for m in group) / len(group) * 100, 1)

        return {
            "higher_count":       len(higher),
            "even_count":         len(even),
            "lower_count":        len(lower),
            "win_rate_vs_higher": wr(higher),
            "win_rate_vs_even":   wr(even),
            "win_rate_vs_lower":  wr(lower),
            "_higher_matches":    higher,
            "_lower_matches":     lower,
            "_even_matches":      even,
        }

    # ------------------------------------------------------------------
    # Findings with citations
    # ------------------------------------------------------------------

    def _generate_findings(self, r: dict, matches: list[dict]) -> list[dict]:
        findings = []

        def add(severity: str, message: str, cite_matches: list[dict] | None = None) -> None:
            f = {"severity": severity, "message": message}
            if cite_matches:
                f["citations"] = self._cite_matches(cite_matches)
            findings.append(f)

        avg_diff   = r["avg_rp_diff"]
        my_bracket = r.get("my_bracket", "")
        avg_enemy  = r["avg_enemy_rp"]
        avg_mine   = r["avg_my_rp"]

        # Lobby calibration — cite the most lopsided matches
        down_matches = sorted(matches, key=lambda m: m["rp_diff"], reverse=True)  # biggest gap down
        up_matches   = sorted(matches, key=lambda m: m["rp_diff"])                # biggest gap up

        if avg_diff > 400:
            add("warning",
                f"Average enemy lobby is {avg_enemy:,.0f} RP — well below your {avg_mine:,.0f}. "
                f"You're frequently matched down. Beating these lobbies should be the floor.",
                down_matches[:MAX_CITATIONS])
        elif avg_diff > 150:
            add("info",
                f"Lobbies average {avg_enemy:,.0f} RP vs your {avg_mine:,.0f}. "
                f"Slightly favorable matchmaking.",
                down_matches[:MAX_CITATIONS])
        elif avg_diff < -200:
            add("info",
                f"Lobbies average {avg_enemy:,.0f} RP vs your {avg_mine:,.0f}. "
                f"You're regularly punching up — wins here are worth more than they look.",
                up_matches[:MAX_CITATIONS])
        else:
            add("info",
                f"Lobbies well calibrated: avg enemy {avg_enemy:,.0f} vs your {avg_mine:,.0f}.")

        # Own bracket performance
        bracket_data = r.get("bracket_data", [])
        my_bdata = next((b for b in bracket_data if b["label"] == my_bracket), None)
        if my_bdata:
            wr       = my_bdata["win_rate"]
            b_losses = [m for m in my_bdata["_matches"] if m["win"] == 0]
            b_wins   = [m for m in my_bdata["_matches"] if m["win"] == 1]
            if wr >= 55:
                add("info",
                    f"In your own bracket ({my_bracket}): {wr:.0f}% WR ({my_bdata['matches']} matches). "
                    f"Solid — you're above water here.",
                    b_wins[:MAX_CITATIONS])
            elif wr < 44:
                add("warning",
                    f"In your own bracket ({my_bracket}): only {wr:.0f}% WR ({my_bdata['matches']} matches). "
                    f"Struggling against same-RP opponents is the core issue to fix.",
                    b_losses[:MAX_CITATIONS])
            else:
                add("info",
                    f"In your own bracket ({my_bracket}): {wr:.0f}% WR — roughly break-even.",
                    b_losses[:MAX_CITATIONS])

        # Hardest bracket ceiling
        hardest    = r.get("hardest_bracket")
        hardest_wr = r.get("hardest_bracket_win_rate")
        if hardest and hardest != my_bracket and hardest_wr is not None and hardest_wr < 35:
            hdata = next((b for b in bracket_data if b["label"] == hardest), None)
            if hdata and hdata["matches"] >= MIN_BRACKET_MATCHES:
                h_losses = [m for m in hdata["_matches"] if m["win"] == 0]
                add("warning",
                    f"{hardest} lobbies are your current ceiling: "
                    f"{hardest_wr:.0f}% WR over {hdata['matches']} matches.",
                    h_losses[:MAX_CITATIONS])

        # Punching down but not converting
        lower_matches = r.get("_lower_matches", [])
        if r["lower_count"] >= MIN_BRACKET_MATCHES and r["win_rate_vs_lower"] < 55:
            lower_losses = [m for m in lower_matches if m["win"] == 0]
            add("warning",
                f"Only {r['win_rate_vs_lower']:.0f}% WR vs lower-RP enemies ({r['lower_count']} matches). "
                f"Not converting favorable matchups consistently.",
                lower_losses[:MAX_CITATIONS])

        # Good performance punching up
        higher_matches = r.get("_higher_matches", [])
        if r["higher_count"] >= MIN_BRACKET_MATCHES and r["win_rate_vs_higher"] >= 45:
            higher_wins = [m for m in higher_matches if m["win"] == 1]
            add("info",
                f"{r['win_rate_vs_higher']:.0f}% WR vs higher-RP enemies ({r['higher_count']} matches). "
                f"Strong performance punching up.",
                higher_wins[:MAX_CITATIONS])

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
    LobbyQualityPlugin(conn, args.username).summary()
