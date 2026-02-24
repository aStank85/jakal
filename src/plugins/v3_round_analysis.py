"""
src/plugins/v3_round_analysis.py
=================================
Analyzes per-round performance with inline citations of specific rounds
where conditions were most apparent.

Requires: player_rounds + round_outcomes + scraped_match_cards tables.
"""

from __future__ import annotations
import sqlite3
from typing import Any

MIN_ROUNDS_FOR_ANALYSIS = 20
MIN_FB_ROUNDS           = 5
MIN_CLUTCH_ROUNDS       = 3
MAX_CITATIONS           = 3   # max examples to inline per finding


def _fmt_round(map_name: str, round_id: int, operator: str, result: str) -> str:
    """Short citation: 'Kafe R4 (Dokkaebi, L)'"""
    outcome = "W" if result in ("victory", "win") else "L"
    op = operator or "?"
    m = (map_name or "?").split()[0]   # first word of map name keeps it compact
    return f"{m} R{round_id} ({op}, {outcome})"


def _fmt_match(map_name: str, result: str, kills: int, deaths: int) -> str:
    """Short citation: 'Oregon (L 7-4)'"""
    outcome = "W" if result == "win" else "L"
    m = (map_name or "?").split()[0]
    return f"{m} ({outcome} {kills}-{deaths})"


class RoundAnalysisPlugin:
    def __init__(self, db_or_conn: Any, username: str):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = username
        self._result: dict | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(self) -> dict:
        rows = self._fetch_rounds()
        if not rows:
            return self._empty("No round data found for this player.")
        total = len(rows)
        if total < MIN_ROUNDS_FOR_ANALYSIS:
            return self._empty(
                f"Only {total} rounds available (need {MIN_ROUNDS_FOR_ANALYSIS} for reliable analysis)."
            )

        result = {
            "username":     self.username,
            "total_rounds": total,
            "data_quality": "sufficient" if total >= 50 else "limited",
        }
        result.update(self._first_blood_impact(rows))
        result.update(self._first_death_impact(rows))
        result.update(self._clutch_analysis(rows))
        result.update(self._side_analysis(rows))
        result.update(self._win_condition_analysis(rows))
        result["findings"] = self._generate_findings(result, rows)

        self._result = result
        return result

    def summary(self) -> None:
        r = self._result or self.analyze()
        if r.get("error"):
            print(f"[RoundAnalysis] {r['error']}")
            return

        print(f"\n{'=' * 62}")
        print(f"  ROUND ANALYSIS — {self.username}")
        print(f"  {r['total_rounds']} rounds analyzed ({r['data_quality']} data)")
        print(f"{'=' * 62}")

        print(f"\n  FIRST BLOOD IMPACT")
        print(f"    FB rate           : {r['fb_rate']:.1f}% ({r['fb_rounds']} rounds)")
        print(f"    Win rate WITH FB  : {r['team_win_rate_with_fb']:.1f}%")
        print(f"    Win rate NO FB    : {r['team_win_rate_without_fb']:.1f}%")
        sign = "+" if r["fb_impact_delta"] >= 0 else ""
        print(f"    FB impact delta   : {sign}{r['fb_impact_delta']:.1f}%")

        print(f"\n  FIRST DEATH IMPACT")
        print(f"    FD rate           : {r['fd_rate']:.1f}% ({r['fd_rounds']} rounds)")
        print(f"    Win rate when FD  : {r['team_win_rate_with_fd']:.1f}%")

        print(f"\n  SIDE PERFORMANCE")
        print(f"    Attack WR : {r['atk_win_rate']:.1f}% ({r['atk_rounds']} rounds)")
        print(f"    Defense WR: {r['def_win_rate']:.1f}% ({r['def_rounds']} rounds)")
        gap = abs(r["side_gap"])
        stronger = "atk" if r["side_gap"] > 0 else "def"
        print(f"    Side gap  : {gap:.1f}% ({stronger} stronger)")

        print(f"\n  CLUTCH PERFORMANCE")
        print(f"    Clutch entry rate : {r['clutch_entry_rate']:.1f}%")
        if r["clutch_attempts"] >= MIN_CLUTCH_ROUNDS:
            print(f"    Clutch win rate   : {r['clutch_win_rate']:.1f}% ({r['clutch_attempts']} attempts)")
        else:
            print(f"    Clutch win rate   : N/A ({r['clutch_attempts']} attempts)")

        print(f"\n  WIN CONDITIONS")
        print(f"    Primary : {r['primary_win_condition']}")
        print(f"    Elim wins: {r['elim_wins']}   Obj wins: {r['obj_wins']}")

        print(f"\n  FINDINGS")
        for f in r["findings"]:
            sev = f["severity"]
            marker = "!!" if sev == "critical" else "! " if sev == "warning" else "  "
            print(f"  {marker} {f['message']}")
            if f.get("citations"):
                print(f"       e.g. {f['citations']}")
        print()

    # ------------------------------------------------------------------
    # Data fetch
    # ------------------------------------------------------------------

    def _fetch_rounds(self) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
                pr.match_id,
                pr.round_id,
                pr.side,
                pr.operator,
                pr.result,
                pr.kills,
                pr.deaths,
                pr.first_blood,
                pr.first_death,
                pr.clutch_won,
                pr.clutch_lost,
                ro.end_reason,
                ro.winner_side,
                COALESCE(smc.map_name, '?') AS map_name,
                CASE WHEN pr.result = 'victory' THEN 1 ELSE 0 END AS team_won
            FROM player_rounds pr
            JOIN round_outcomes ro
                ON pr.match_id = ro.match_id
                AND pr.round_id = ro.round_id
            LEFT JOIN scraped_match_cards smc
                ON pr.match_id = smc.match_id
            WHERE pr.username = ?
              AND LOWER(COALESCE(smc.mode, '')) LIKE '%ranked%'
              AND LOWER(COALESCE(smc.mode, '')) NOT LIKE '%unranked%'
            ORDER BY pr.match_id, pr.round_id
            """,
            (self.username,),
        )
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Citation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _cite_rounds(rounds: list[dict], n: int = MAX_CITATIONS) -> str:
        """Format up to n rounds as inline citations."""
        samples = rounds[:n]
        return " · ".join(
            _fmt_round(r["map_name"], r["round_id"], r["operator"], r["result"])
            for r in samples
        )

    # ------------------------------------------------------------------
    # Analysis modules (unchanged logic, return same keys)
    # ------------------------------------------------------------------

    def _first_blood_impact(self, rows: list[dict]) -> dict:
        fb_rounds    = [r for r in rows if r["first_blood"] == 1]
        no_fb_rounds = [r for r in rows if r["first_blood"] == 0]
        fb_count     = len(fb_rounds)
        total        = len(rows)

        win_with_fb = (
            sum(r["team_won"] for r in fb_rounds) / fb_count * 100
            if fb_count >= MIN_FB_ROUNDS else None
        )
        win_without_fb = (
            sum(r["team_won"] for r in no_fb_rounds) / len(no_fb_rounds) * 100
            if no_fb_rounds else 0.0
        )
        impact_delta = (win_with_fb - win_without_fb) if win_with_fb is not None else 0.0

        return {
            "fb_rounds":               fb_count,
            "fb_rate":                 fb_count / total * 100,
            "team_win_rate_with_fb":   win_with_fb or 0.0,
            "team_win_rate_without_fb": win_without_fb,
            "fb_impact_delta":         impact_delta,
            "fb_reliable":             fb_count >= MIN_FB_ROUNDS,
            "_fb_rounds_raw":          fb_rounds,
        }

    def _first_death_impact(self, rows: list[dict]) -> dict:
        fd_rounds = [r for r in rows if r["first_death"] == 1]
        fd_count  = len(fd_rounds)
        total     = len(rows)
        win_with_fd = (
            sum(r["team_won"] for r in fd_rounds) / fd_count * 100
            if fd_count >= MIN_FB_ROUNDS else None
        )
        return {
            "fd_rounds":              fd_count,
            "fd_rate":                fd_count / total * 100,
            "team_win_rate_with_fd":  win_with_fd or 0.0,
            "fd_reliable":            fd_count >= MIN_FB_ROUNDS,
            "_fd_rounds_raw":         fd_rounds,
        }

    def _clutch_analysis(self, rows: list[dict]) -> dict:
        total         = len(rows)
        clutch_won    = sum(r["clutch_won"]  for r in rows)
        clutch_lost   = sum(r["clutch_lost"] for r in rows)
        clutch_attempts = clutch_won + clutch_lost
        clutch_win_rate = (
            clutch_won / clutch_attempts * 100
            if clutch_attempts >= MIN_CLUTCH_ROUNDS else 0.0
        )
        clutch_rounds = [r for r in rows if (r["clutch_won"] or r["clutch_lost"])]
        return {
            "clutch_won_total":  clutch_won,
            "clutch_lost_total": clutch_lost,
            "clutch_attempts":   clutch_attempts,
            "clutch_entry_rate": clutch_attempts / total * 100,
            "clutch_win_rate":   clutch_win_rate,
            "clutch_reliable":   clutch_attempts >= MIN_CLUTCH_ROUNDS,
            "_clutch_rounds_raw": clutch_rounds,
        }

    def _side_analysis(self, rows: list[dict]) -> dict:
        atk  = [r for r in rows if r["side"] == "attacker"]
        def_ = [r for r in rows if r["side"] == "defender"]
        atk_wr = sum(r["team_won"] for r in atk)  / len(atk)  * 100 if atk  else 0.0
        def_wr = sum(r["team_won"] for r in def_) / len(def_) * 100 if def_ else 0.0
        side_gap = atk_wr - def_wr
        # Worst-performing side rounds for citation
        weak_side_rounds = [r for r in (atk if side_gap < 0 else def_) if r["team_won"] == 0]
        return {
            "atk_rounds":      len(atk),
            "def_rounds":      len(def_),
            "atk_win_rate":    atk_wr,
            "def_win_rate":    def_wr,
            "side_gap":        side_gap,
            "weak_side":       "defense" if side_gap > 0 else "attack" if side_gap < 0 else "even",
            "_weak_side_losses": weak_side_rounds,
        }

    def _win_condition_analysis(self, rows: list[dict]) -> dict:
        won_rounds  = [r for r in rows if r["team_won"] == 1]
        elim_reasons = {"attackers_eliminated", "defenders_eliminated",
                        "defenders_eliminated_after_defuser_planted"}
        obj_reasons  = {"bomb_exploded", "bomb_defused", "defuser_deactivated"}
        elim_wins = sum(1 for r in won_rounds if r["end_reason"] in elim_reasons)
        obj_wins  = sum(1 for r in won_rounds if r["end_reason"] in obj_reasons)
        total_wins = len(won_rounds)
        overall_wr = total_wins / len(rows) * 100 if rows else 0.0
        primary = (
            "elimination" if elim_wins >= obj_wins else
            "objective"   if obj_wins > elim_wins  else "mixed"
        )
        return {
            "total_wins":              total_wins,
            "overall_round_win_rate":  overall_wr,
            "elim_wins":               elim_wins,
            "obj_wins":                obj_wins,
            "primary_win_condition":   primary,
        }

    # ------------------------------------------------------------------
    # Findings engine — with citations
    # ------------------------------------------------------------------

    def _generate_findings(self, r: dict, rows: list[dict]) -> list[dict]:
        findings = []

        def add(severity: str, message: str, cite_rounds: list[dict] | None = None) -> None:
            f = {"severity": severity, "message": message}
            if cite_rounds:
                f["citations"] = self._cite_rounds(cite_rounds)
            findings.append(f)

        # --- First Blood ---
        if r["fb_reliable"]:
            delta = r["fb_impact_delta"]
            # Cite rounds where FB was gotten but team still lost (shows when it matters most)
            fb_losses = [x for x in r["_fb_rounds_raw"] if x["team_won"] == 0]
            fb_wins   = [x for x in r["_fb_rounds_raw"] if x["team_won"] == 1]
            if delta >= 20:
                add("info",
                    f"When you get first blood your team wins {r['team_win_rate_with_fb']:.0f}% of rounds "
                    f"(vs {r['team_win_rate_without_fb']:.0f}% without). "
                    f"+{delta:.0f}% impact — play for first blood every round.",
                    fb_wins[:MAX_CITATIONS])
            elif delta >= 10:
                add("info",
                    f"First bloods boost team win rate by +{delta:.0f}% "
                    f"({r['team_win_rate_with_fb']:.0f}% vs {r['team_win_rate_without_fb']:.0f}%). "
                    f"Keep prioritizing entry duels.",
                    fb_wins[:MAX_CITATIONS])
            elif delta < 0:
                add("warning",
                    f"First bloods are hurting your team: {r['team_win_rate_with_fb']:.0f}% win rate WITH your FB "
                    f"vs {r['team_win_rate_without_fb']:.0f}% without. You may be forcing bad duels.",
                    fb_losses[:MAX_CITATIONS])
        else:
            add("info", f"Not enough FB rounds ({r['fb_rounds']}) for reliable impact analysis yet.")

        # --- First Death ---
        if r["fd_reliable"]:
            fd_rate = r["fd_rate"]
            fd_win  = r["team_win_rate_with_fd"]
            # Cite FD rounds that were losses — the costly ones
            fd_losses = [x for x in r["_fd_rounds_raw"] if x["team_won"] == 0]
            if fd_rate > 15:
                add("warning",
                    f"You die first in {fd_rate:.0f}% of rounds. "
                    f"Your team wins only {fd_win:.0f}% of those. "
                    f"Stop taking early off-angle duels you're not winning.",
                    fd_losses[:MAX_CITATIONS])
            elif fd_rate > 8:
                add("info",
                    f"You die first in {fd_rate:.0f}% of rounds. "
                    f"Team win rate when you FD: {fd_win:.0f}%.",
                    fd_losses[:MAX_CITATIONS])

        # --- Side imbalance ---
        gap = abs(r["side_gap"])
        if gap >= 20:
            stronger = "attack" if r["side_gap"] > 0 else "defense"
            weaker   = "defense" if r["side_gap"] > 0 else "attack"
            add("warning",
                f"Major side imbalance: {stronger} {max(r['atk_win_rate'], r['def_win_rate']):.0f}% "
                f"vs {weaker} {min(r['atk_win_rate'], r['def_win_rate']):.0f}% "
                f"(gap: {gap:.0f}%). Your {weaker} is your biggest correctable loss source.",
                r["_weak_side_losses"][:MAX_CITATIONS])
        elif gap >= 10:
            add("info",
                f"Slight {r['weak_side']} weakness: atk {r['atk_win_rate']:.0f}% / def {r['def_win_rate']:.0f}%.",
                r["_weak_side_losses"][:MAX_CITATIONS])

        # --- Overall round win rate ---
        rwr = r["overall_round_win_rate"]
        total_losses = [x for x in rows if x["team_won"] == 0]
        if rwr < 44:
            add("critical",
                f"Round win rate of {rwr:.0f}% is below break-even (~44%). "
                f"You are losing RP every session regardless of other factors.",
                total_losses[:MAX_CITATIONS])
        elif rwr < 50:
            add("warning",
                f"Round win rate {rwr:.0f}% — slightly below 50%. You need ~55% to climb meaningfully.")
        elif rwr >= 55:
            add("info", f"Round win rate {rwr:.0f}% — solid. You should be climbing.")

        # --- Clutch ---
        if r["clutch_reliable"]:
            cwr = r["clutch_win_rate"]
            clutch_losses = [x for x in r["_clutch_rounds_raw"] if x["clutch_lost"]]
            clutch_wins   = [x for x in r["_clutch_rounds_raw"] if x["clutch_won"]]
            if cwr < 25:
                add("warning",
                    f"Clutch win rate: {cwr:.0f}% over {r['clutch_attempts']} attempts. "
                    f"Clutches are costing you rounds. Trade and reset instead of forcing 1v3+.",
                    clutch_losses[:MAX_CITATIONS])
            elif cwr >= 50:
                add("info",
                    f"Strong clutch performance: {cwr:.0f}% over {r['clutch_attempts']} attempts.",
                    clutch_wins[:MAX_CITATIONS])

        # --- Win condition ---
        add("info",
            f"Primary win condition: {r['primary_win_condition']} "
            f"({r['elim_wins']} elim wins vs {r['obj_wins']} obj wins).")

        return findings

    @staticmethod
    def _empty(reason: str) -> dict:
        return {"error": reason, "findings": []}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="V3 Round Analysis Plugin")
    parser.add_argument("--db",       required=True)
    parser.add_argument("--username", required=True)
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    RoundAnalysisPlugin(conn, args.username).summary()
