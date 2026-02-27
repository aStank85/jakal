"""SQLite diagnostics for operator/round data integrity.

Run:
    python -m src.tools.data_integrity_check
or:
    python -m src.tools.data_integrity_check --db data/jakal_fresh.db --show-schema
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Iterable


DEFAULT_DB = "data/jakal_fresh.db"


def resolve_db_path(arg_db: str | None) -> Path:
    env_db = os.getenv("JAKAL_DB_PATH", "").strip()
    chosen = arg_db or env_db or DEFAULT_DB
    path = Path(chosen)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / path
    return path


def fetch_one_int(cur: sqlite3.Cursor, sql: str, params: Iterable[object] = ()) -> int:
    cur.execute(sql, tuple(params))
    row = cur.fetchone()
    if not row:
        return 0
    value = row[0]
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def print_table_schema(cur: sqlite3.Cursor, table_name: str) -> None:
    print(f"\n[{table_name}] schema")
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    if not rows:
        print("  (table missing)")
        return
    for cid, name, col_type, notnull, dflt_value, pk in rows:
        parts = [f"{name} {col_type or 'TEXT'}"]
        if pk:
            parts.append("PRIMARY KEY")
        if notnull:
            parts.append("NOT NULL")
        if dflt_value is not None:
            parts.append(f"DEFAULT {dflt_value}")
        print("  - " + " ".join(parts))


def print_counts(cur: sqlite3.Cursor) -> None:
    print("\nA) Match totals")
    total_matches = fetch_one_int(cur, "SELECT COUNT(*) FROM scraped_match_cards")
    ranked_matches = fetch_one_int(
        cur,
        """
        SELECT COUNT(*)
        FROM scraped_match_cards
        WHERE LOWER(TRIM(COALESCE(mode, ''))) IN
            ('ranked', 'pvp_ranked', 'rank')
        """,
    )
    print(f"  total matches: {total_matches}")
    print(f"  ranked matches: {ranked_matches}")

    print("\nB) Round-level rows")
    total_player_rounds = fetch_one_int(cur, "SELECT COUNT(*) FROM player_rounds")
    ranked_player_rounds = fetch_one_int(
        cur,
        """
        SELECT COUNT(*)
        FROM player_rounds
        WHERE LOWER(TRIM(COALESCE(match_type, ''))) IN
            ('ranked', 'pvp_ranked', 'rank')
           OR LOWER(TRIM(COALESCE(match_id, ''))) IN (
                SELECT LOWER(TRIM(match_id))
                FROM scraped_match_cards
                WHERE LOWER(TRIM(COALESCE(mode, ''))) IN ('ranked', 'pvp_ranked', 'rank')
           )
        """,
    )
    distinct_match_round_pairs = fetch_one_int(
        cur,
        "SELECT COUNT(*) FROM (SELECT DISTINCT match_id, round_id FROM player_rounds)",
    )
    distinct_operators = fetch_one_int(
        cur,
        "SELECT COUNT(DISTINCT LOWER(TRIM(COALESCE(operator, '')))) FROM player_rounds",
    )
    print(f"  total player_rounds rows: {total_player_rounds}")
    print(f"  ranked player_rounds rows: {ranked_player_rounds}")
    print(f"  distinct (match_id, round_id) in player_rounds: {distinct_match_round_pairs}")
    print(f"  distinct operators in player_rounds: {distinct_operators}")

    print("\nC) Coverage")
    matches_with_rounds = fetch_one_int(
        cur,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT smc.match_id
            FROM scraped_match_cards smc
            WHERE smc.match_id IS NOT NULL AND TRIM(smc.match_id) != ''
              AND EXISTS (
                  SELECT 1
                  FROM player_rounds pr
                  WHERE pr.match_id = smc.match_id
              )
        )
        """,
    )
    matches_with_outcomes = fetch_one_int(
        cur,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT smc.match_id
            FROM scraped_match_cards smc
            WHERE smc.match_id IS NOT NULL AND TRIM(smc.match_id) != ''
              AND EXISTS (
                  SELECT 1
                  FROM round_outcomes ro
                  WHERE ro.match_id = smc.match_id
              )
        )
        """,
    )

    denominator = total_matches if total_matches > 0 else 1
    rounds_pct = (matches_with_rounds / denominator) * 100.0
    outcomes_pct = (matches_with_outcomes / denominator) * 100.0
    print(f"  % matches with any player_rounds: {rounds_pct:.1f}% ({matches_with_rounds}/{total_matches})")
    print(f"  % matches with any round_outcomes: {outcomes_pct:.1f}% ({matches_with_outcomes}/{total_matches})")

    print("\n  newest 10 matches missing player_rounds:")
    cur.execute(
        """
        SELECT smc.match_id, COALESCE(smc.match_date, smc.scraped_at) AS sort_ts, smc.mode
        FROM scraped_match_cards smc
        WHERE smc.match_id IS NOT NULL AND TRIM(smc.match_id) != ''
          AND NOT EXISTS (
              SELECT 1
              FROM player_rounds pr
              WHERE pr.match_id = smc.match_id
          )
        ORDER BY COALESCE(smc.match_date, smc.scraped_at) DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("    (none)")
    else:
        for match_id, sort_ts, mode in rows:
            print(f"    - {match_id} | {sort_ts} | mode={mode}")

    print("\n  newest 10 matches missing round_outcomes:")
    cur.execute(
        """
        SELECT smc.match_id, COALESCE(smc.match_date, smc.scraped_at) AS sort_ts, smc.mode
        FROM scraped_match_cards smc
        WHERE smc.match_id IS NOT NULL AND TRIM(smc.match_id) != ''
          AND NOT EXISTS (
              SELECT 1
              FROM round_outcomes ro
              WHERE ro.match_id = smc.match_id
          )
        ORDER BY COALESCE(smc.match_date, smc.scraped_at) DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("    (none)")
    else:
        for match_id, sort_ts, mode in rows:
            print(f"    - {match_id} | {sort_ts} | mode={mode}")


def print_queue_values(cur: sqlite3.Cursor) -> None:
    print("\nQueue/mode labels (scraped_match_cards.mode)")
    cur.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(mode), ''), '<EMPTY>') AS mode_value, COUNT(*) AS c
        FROM scraped_match_cards
        GROUP BY mode_value
        ORDER BY c DESC, mode_value ASC
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)")
        return
    for mode_value, count in rows:
        print(f"  - {mode_value}: {count}")


def print_operator_values(cur: sqlite3.Cursor, limit: int = 100) -> None:
    print("\nOperator strings (player_rounds.operator)")
    cur.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(operator), ''), '<EMPTY>') AS op, COUNT(*) AS c
        FROM player_rounds
        GROUP BY op
        ORDER BY c DESC, op ASC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)")
        return
    for op, count in rows:
        print(f"  - {op}: {count}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Jakal data integrity diagnostics")
    parser.add_argument("--db", default="", help="SQLite DB path (defaults to JAKAL_DB_PATH or data/jakal_fresh.db)")
    parser.add_argument("--show-schema", action="store_true", help="Print relevant table schemas")
    parser.add_argument("--show-queues", action="store_true", help="Print distinct queue/mode labels")
    parser.add_argument("--show-operators", action="store_true", help="Print distinct operator strings")
    parser.add_argument("--operator-limit", type=int, default=100, help="Max operator rows for --show-operators")
    args = parser.parse_args()

    db_path = resolve_db_path(args.db.strip() or None)
    print(f"DB path: {db_path}")
    if not db_path.exists():
        print("ERROR: DB file does not exist.")
        return 2

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        if args.show_schema:
            for table in ("scraped_match_cards", "match_detail_players", "player_rounds", "round_outcomes", "agg_player_operator"):
                print_table_schema(cur, table)
        print_counts(cur)
        if args.show_queues:
            print_queue_values(cur)
        if args.show_operators:
            print_operator_values(cur, limit=args.operator_limit)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

