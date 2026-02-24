"""
src/plugins/v3_trade_analysis.py
================================
Analyzes "trades" from round kill events.

Trade definition (5s default):
If player A dies, and teammate C gets a kill within the next 5 seconds,
that death is considered traded. If C kills A's killer, it's a direct refrag.
"""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any

TRADE_WINDOW_SECONDS = 5.0
MIN_DEATHS_FOR_RELIABLE = 8
MAX_CITATIONS = 3


def _norm_name(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_mode_ranked(mode: Any) -> bool:
    text = str(mode or "").strip().lower()
    if text in {"ranked", "pvp_ranked"}:
        return True
    return ("ranked" in text) and ("unranked" not in text)


def _to_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        if n > 1e12:
            return n / 1000.0
        return n
    text = str(value).strip()
    if not text:
        return None
    try:
        n = float(text)
        if n > 1e12:
            return n / 1000.0
        return n
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.timestamp()
    except ValueError:
        return None


def _team_key(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return ""
    if text in {"0", "a", "team_a", "teama", "blue"}:
        return "A"
    if text in {"1", "b", "team_b", "teamb", "orange"}:
        return "B"
    if "attacker" in text:
        return "ATT"
    if "defender" in text:
        return "DEF"
    if "blue" in text:
        return "A"
    if "orange" in text:
        return "B"
    return text


def _extract_player_name(player: dict) -> str:
    for key in ("nickname", "pseudonym", "name", "username", "playerName"):
        value = player.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_player_team(player: dict) -> str:
    for key in ("team", "team_id", "teamId", "side", "sideId"):
        value = player.get(key)
        if value is None:
            continue
        tk = _team_key(value)
        if tk:
            return tk
    return ""


def _round_citation(match_map: str, round_no: int, killer: str, victim: str, dt: float, direct: bool) -> str:
    tag = "direct" if direct else "trade"
    m = (match_map or "?").split()[0]
    return f"{m} R{round_no}: {killer} -> {victim} ({dt:.1f}s, {tag})"


class TradeAnalysisPlugin:
    def __init__(self, db_or_conn: Any, username: str, window_seconds: float = TRADE_WINDOW_SECONDS):
        if hasattr(db_or_conn, "conn"):
            self._conn = db_or_conn.conn
        else:
            self._conn = db_or_conn
        self.username = str(username or "").strip()
        self.window_seconds = float(window_seconds)
        self._result: dict | None = None

    def analyze(self) -> dict:
        matches = self._fetch_matches()
        if not matches:
            return self._empty("No ranked matches with round events found.")

        user = _norm_name(self.username)
        total_deaths = 0
        traded_deaths = 0
        direct_refrags = 0
        trade_times: list[float] = []
        citations: list[str] = []

        for match in matches:
            map_name = str(match.get("map_name") or "Unknown")
            rounds = match.get("rounds") or []
            for r_idx, rnd in enumerate(rounds, 1):
                events_raw = rnd.get("kill_events")
                if not isinstance(events_raw, list) or not events_raw:
                    continue

                name_to_team = {}
                round_players = rnd.get("players")
                if isinstance(round_players, list):
                    for p in round_players:
                        if not isinstance(p, dict):
                            continue
                        pname = _extract_player_name(p)
                        pteam = _extract_player_team(p)
                        if pname and pteam:
                            name_to_team[_norm_name(pname)] = pteam

                parsed_events: list[dict] = []
                for e_idx, ev in enumerate(events_raw):
                    if not isinstance(ev, dict):
                        continue
                    killer = _norm_name(
                        ev.get("killerName")
                        or ev.get("killer")
                        or ev.get("killerUsername")
                        or ev.get("attacker")
                        or ev.get("from")
                    )
                    victim = _norm_name(
                        ev.get("victimName")
                        or ev.get("victim")
                        or ev.get("victimUsername")
                        or ev.get("target")
                        or ev.get("to")
                    )
                    if not killer or not victim:
                        continue
                    t = (
                        _to_seconds(ev.get("timestamp"))
                        or _to_seconds(ev.get("time"))
                        or _to_seconds(ev.get("eventTime"))
                    )
                    if t is None:
                        t = float(e_idx)
                    parsed_events.append(
                        {
                            "killer": killer,
                            "victim": victim,
                            "time": t,
                            "idx": e_idx,
                            "killer_team": name_to_team.get(killer, ""),
                            "victim_team": name_to_team.get(victim, ""),
                        }
                    )

                if not parsed_events:
                    continue
                parsed_events.sort(key=lambda x: (x["time"], x["idx"]))

                for i, death_ev in enumerate(parsed_events):
                    if death_ev["victim"] != user:
                        continue
                    total_deaths += 1
                    victim_team = death_ev.get("victim_team") or ""
                    if not victim_team:
                        continue
                    killer_name = death_ev.get("killer") or ""

                    for later in parsed_events[i + 1:]:
                        dt = later["time"] - death_ev["time"]
                        if dt <= 0:
                            continue
                        if dt > self.window_seconds:
                            break
                        if later.get("killer_team") != victim_team:
                            continue
                        if later.get("killer") == user:
                            continue
                        traded_deaths += 1
                        direct = later.get("victim") == killer_name and bool(killer_name)
                        if direct:
                            direct_refrags += 1
                        trade_times.append(dt)
                        if len(citations) < MAX_CITATIONS:
                            citations.append(
                                _round_citation(
                                    map_name=map_name,
                                    round_no=int(rnd.get("round_number") or r_idx),
                                    killer=later.get("killer", "?"),
                                    victim=later.get("victim", "?"),
                                    dt=dt,
                                    direct=direct,
                                )
                            )
                        break

        if total_deaths == 0:
            return self._empty("No player death events found to evaluate trades.")

        trade_rate = traded_deaths / total_deaths * 100.0
        direct_rate = direct_refrags / traded_deaths * 100.0 if traded_deaths else 0.0
        avg_trade_time = sum(trade_times) / len(trade_times) if trade_times else 0.0

        result = {
            "username": self.username,
            "window_seconds": self.window_seconds,
            "matches_analyzed": len(matches),
            "total_deaths": total_deaths,
            "traded_deaths": traded_deaths,
            "untraded_deaths": total_deaths - traded_deaths,
            "trade_rate": trade_rate,
            "direct_refrags": direct_refrags,
            "direct_refrag_rate": direct_rate,
            "avg_trade_time_seconds": avg_trade_time,
            "data_quality": "sufficient" if total_deaths >= MIN_DEATHS_FOR_RELIABLE else "limited",
            "citations": citations,
            "findings": self._findings(
                total_deaths=total_deaths,
                trade_rate=trade_rate,
                direct_rate=direct_rate,
                avg_trade_time=avg_trade_time,
                citations=citations,
            ),
        }
        self._result = result
        return result

    def _fetch_matches(self) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT match_id, map_name, mode, rounds_json
            FROM scraped_match_cards
            WHERE username = ?
              AND rounds_json IS NOT NULL
              AND TRIM(rounds_json) != ''
              AND TRIM(rounds_json) != '[]'
            ORDER BY id DESC
            """,
            (self.username,),
        )
        out = []
        for row in cur.fetchall():
            item = dict(row)
            if not _safe_mode_ranked(item.get("mode")):
                continue
            try:
                rounds = json.loads(item.get("rounds_json") or "[]")
            except json.JSONDecodeError:
                rounds = []
            if not isinstance(rounds, list) or not rounds:
                continue
            out.append(
                {
                    "match_id": item.get("match_id") or "",
                    "map_name": item.get("map_name") or "",
                    "rounds": rounds,
                }
            )
        return out

    def _findings(
        self,
        total_deaths: int,
        trade_rate: float,
        direct_rate: float,
        avg_trade_time: float,
        citations: list[str],
    ) -> list[dict]:
        findings: list[dict] = []

        def add(sev: str, msg: str) -> None:
            findings.append({"severity": sev, "message": msg})

        cite = f" Examples: {'; '.join(citations)}." if citations else ""
        if trade_rate < 35:
            add("warning", f"Low trade support: only {trade_rate:.0f}% of your deaths were traded within 5s.{cite}")
        elif trade_rate < 55:
            add("info", f"Average trade support: {trade_rate:.0f}% of your deaths were traded within 5s.{cite}")
        else:
            add("info", f"Strong trade support: {trade_rate:.0f}% of your deaths were traded within 5s.{cite}")

        if total_deaths >= MIN_DEATHS_FOR_RELIABLE:
            add("info", f"Direct refrag share: {direct_rate:.0f}% of trades killed your killer.")
            add("info", f"Average trade timing: {avg_trade_time:.2f}s after your death.")
        else:
            add("info", f"Limited sample ({total_deaths} deaths). More matches needed for stable trade patterns.")

        return findings

    @staticmethod
    def _empty(reason: str) -> dict:
        return {"error": reason, "findings": []}


if __name__ == "__main__":
    import argparse
    import sqlite3

    parser = argparse.ArgumentParser(description="V3 Trade Analysis Plugin")
    parser.add_argument("--db", required=True, help="Path to jakal.db")
    parser.add_argument("--username", required=True, help="Player username to analyze")
    parser.add_argument("--window", type=float, default=5.0, help="Trade window in seconds (default 5)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    plugin = TradeAnalysisPlugin(conn, args.username, window_seconds=args.window)
    print(plugin.analyze())
