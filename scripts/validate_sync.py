from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: str


def scalar(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> int:
    row = cur.execute(sql, params).fetchone()
    if row is None:
        return 0
    value = row[0]
    return int(value or 0)


def run_checks(db_path: Path, player: str) -> tuple[list[CheckResult], dict[str, int]]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        player_rows = scalar(
            cur,
            "SELECT COUNT(*) FROM players WHERE handle = ?",
            (player,),
        )
        match_index_rows = scalar(
            cur,
            "SELECT COUNT(*) FROM player_match_index WHERE handle = ?",
            (player,),
        )
        matches_rows = scalar(cur, "SELECT COUNT(*) FROM matches")
        match_with_10_players = scalar(
            cur,
            """
            SELECT COUNT(*)
            FROM (
                SELECT match_id
                FROM match_players
                GROUP BY match_id
                HAVING COUNT(*) = 10
            )
            """,
        )
        player_round_rows = scalar(cur, "SELECT COUNT(*) FROM player_rounds")
        rounds_with_win_condition = scalar(
            cur,
            "SELECT COUNT(*) FROM rounds WHERE win_condition IS NOT NULL AND TRIM(win_condition) <> ''",
        )
        kill_event_rows = scalar(cur, "SELECT COUNT(*) FROM kill_events")
        promoted_metric_rows = scalar(
            cur,
            """
            SELECT COUNT(*)
            FROM match_players
            WHERE kd_ratio IS NOT NULL
              AND hs_pct IS NOT NULL
              AND esr IS NOT NULL
            """,
        )
        v2_done_rows = scalar(
            cur,
            "SELECT COUNT(*) FROM scrape_match_status WHERE v2_done = 1",
        )
        v1_done_matches = scalar(
            cur,
            "SELECT COUNT(*) FROM scrape_match_status WHERE v2_done = 1 AND v1_done = 1",
        )
        v1_pending_matches = scalar(
            cur,
            "SELECT COUNT(*) FROM scrape_match_status WHERE v2_done = 1 AND v1_done = 0",
        )

        results = [
            CheckResult(
                name="players row exists for player",
                passed=player_rows >= 1,
                details=f"found {player_rows} row(s) for player '{player}'",
            ),
            CheckResult(
                name="player_match_index has rows for player",
                passed=match_index_rows >= 1,
                details=f"found {match_index_rows} indexed match row(s) for '{player}'",
            ),
            CheckResult(
                name="matches table has rows",
                passed=matches_rows >= 1,
                details=f"found {matches_rows} match row(s)",
            ),
            CheckResult(
                name="at least one match has exactly 10 match_players rows",
                passed=match_with_10_players >= 1,
                details=f"found {match_with_10_players} match(es) with exactly 10 players",
            ),
            CheckResult(
                name="player_rounds has rows",
                passed=player_round_rows >= 1,
                details=f"found {player_round_rows} player_round row(s)",
            ),
            CheckResult(
                name="rounds has win_condition populated",
                passed=rounds_with_win_condition >= 1,
                details=f"found {rounds_with_win_condition} round row(s) with win_condition",
            ),
            CheckResult(
                name="kill_events has rows",
                passed=kill_event_rows >= 1,
                details=f"found {kill_event_rows} kill event row(s)",
            ),
            CheckResult(
                name="match_players has promoted metrics",
                passed=promoted_metric_rows >= 1,
                details=f"found {promoted_metric_rows} row(s) with kd_ratio, hs_pct, and esr",
            ),
            CheckResult(
                name="scrape_match_status has successful v2 rows",
                passed=v2_done_rows >= 1,
                details=f"found {v2_done_rows} row(s) with v2_done = 1",
            ),
        ]

        counts = {
            "v1_done": v1_done_matches,
            "v1_not_done": v1_pending_matches,
        }
        return results, counts
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate scraper sync results.")
    parser.add_argument("--db", required=True, help="Path to scraper SQLite DB")
    parser.add_argument("--player", required=True, help="Player handle used for sync")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)

    if not db_path.exists():
        print(f"FAIL  database exists :: missing file '{db_path}'")
        return 2

    try:
        results, counts = run_checks(db_path, args.player)
    except sqlite3.Error as exc:
        print(f"FAIL  validation query error :: {exc}")
        return 2

    print(f"Scraper Sync Validation Report")
    print(f"DB: {db_path}")
    print(f"Player: {args.player}")
    print("")

    failures = 0
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{status:<5} {result.name} :: {result.details}")
        if not result.passed:
            failures += 1

    print("")
    print("V1 Detail Coverage")
    print(f"v1_done=1 :: {counts['v1_done']}")
    print(f"v1_done=0 :: {counts['v1_not_done']}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
