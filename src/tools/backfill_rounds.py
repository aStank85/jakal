"""Backfill normalized round tables from stored scraped match cards.

Run examples:
    python -m src.tools.backfill_rounds --queue ranked --since 2025-01-01
    python -m src.tools.backfill_rounds --username Shermanz12312 --batch-size 100
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from src.database import Database


DEFAULT_DB = "data/jakal_fresh.db"


def resolve_db_path(arg_db: str | None) -> str:
    env_db = os.getenv("JAKAL_DB_PATH", "").strip()
    chosen = arg_db or env_db or DEFAULT_DB
    path = Path(chosen)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / path
    return str(path)


def pending_count(
    db: Database,
    *,
    username: str = "",
    queue_key: str = "",
    since_date: str = "",
) -> int:
    cursor = db.conn.cursor()
    params: list[Any] = []
    query = """
        SELECT COUNT(*)
        FROM scraped_match_cards
        WHERE summary_json IS NOT NULL
          AND TRIM(summary_json) != ''
          AND TRIM(summary_json) != '{}'
          AND LOWER(TRIM(summary_json)) != 'null'
          AND match_id IS NOT NULL
          AND TRIM(match_id) != ''
          AND (COALESCE(has_rounds, 0) = 0 OR COALESCE(has_outcomes, 0) = 0)
    """
    if username:
        query += " AND username = ?"
        params.append(username)
    if since_date:
        query += " AND COALESCE(match_date, scraped_at) >= ?"
        params.append(since_date)
    if queue_key:
        qk = db._canonicalize_queue_key(queue_key)
        query += """
            AND COALESCE(NULLIF(TRIM(mode_key), ''), CASE
                WHEN LOWER(TRIM(COALESCE(mode, ''))) LIKE '%ranked%' THEN 'ranked'
                WHEN LOWER(TRIM(COALESCE(mode, ''))) LIKE '%unranked%' THEN 'standard'
                WHEN LOWER(TRIM(COALESCE(mode, ''))) LIKE '%standard%' THEN 'standard'
                WHEN LOWER(TRIM(COALESCE(mode, ''))) LIKE '%quick%' OR LOWER(TRIM(COALESCE(mode, ''))) LIKE '%casual%' THEN 'quickmatch'
                WHEN LOWER(TRIM(COALESCE(mode, ''))) LIKE '%event%' OR LOWER(TRIM(COALESCE(mode, ''))) LIKE '%arcade%' THEN 'event'
                ELSE 'other'
            END) = ?
        """
        params.append(qk)
    cursor.execute(query, tuple(params))
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing round-level normalized rows from stored cards")
    parser.add_argument("--db", default="", help="SQLite DB path (defaults to JAKAL_DB_PATH or data/jakal_fresh.db)")
    parser.add_argument("--username", default="", help="Optional username scope")
    parser.add_argument("--queue", default="", help="Optional queue filter (ranked/standard/quickmatch/event/other)")
    parser.add_argument("--since", default="", help="Optional date filter (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows to scan per batch")
    parser.add_argument("--max-batches", type=int, default=200, help="Safety cap for batch loop")
    parser.add_argument("--dry-run", action="store_true", help="Print pending count only and exit")
    args = parser.parse_args()

    db_path = resolve_db_path(args.db.strip() or None)
    print(f"DB path: {db_path}")
    if not Path(db_path).exists():
        print("ERROR: DB file does not exist.")
        return 2

    db = Database(db_path)
    try:
        before = pending_count(
            db,
            username=args.username.strip(),
            queue_key=args.queue.strip(),
            since_date=args.since.strip(),
        )
        print(f"Pending cards before backfill: {before}")
        if args.dry_run:
            return 0

        total = {
            "batches": 0,
            "scanned": 0,
            "unpacked_matches": 0,
            "inserted_detail_rows": 0,
            "inserted_round_rows": 0,
            "inserted_player_round_rows": 0,
            "aggregates_refreshed_trackers": 0,
            "skipped": 0,
            "errors": 0,
        }
        batch_size = max(1, int(args.batch_size))
        max_batches = max(1, int(args.max_batches))

        for i in range(max_batches):
            stats = db.unpack_pending_scraped_match_cards(
                username=args.username.strip() or None,
                limit=batch_size,
                queue_key=args.queue.strip() or None,
                since_date=args.since.strip() or None,
            )
            total["batches"] += 1
            for key in total.keys():
                if key == "batches":
                    continue
                total[key] += int(stats.get(key, 0))

            print(
                f"Batch {i + 1}: scanned={stats.get('scanned', 0)} unpacked={stats.get('unpacked_matches', 0)} "
                f"round_rows={stats.get('inserted_player_round_rows', 0)} errors={stats.get('errors', 0)}"
            )

            if int(stats.get("scanned", 0)) == 0:
                break
            if int(stats.get("unpacked_matches", 0)) == 0 and int(stats.get("errors", 0)) == 0:
                # No progress in this batch; stop instead of looping forever.
                break

        after = pending_count(
            db,
            username=args.username.strip(),
            queue_key=args.queue.strip(),
            since_date=args.since.strip(),
        )
        print("Backfill summary:")
        print(f"  batches: {total['batches']}")
        print(f"  scanned: {total['scanned']}")
        print(f"  unpacked_matches: {total['unpacked_matches']}")
        print(f"  inserted_detail_rows: {total['inserted_detail_rows']}")
        print(f"  inserted_round_rows: {total['inserted_round_rows']}")
        print(f"  inserted_player_round_rows: {total['inserted_player_round_rows']}")
        print(f"  aggregates_refreshed_trackers: {total['aggregates_refreshed_trackers']}")
        print(f"  skipped: {total['skipped']}")
        print(f"  errors: {total['errors']}")
        print(f"Pending cards after backfill: {after}")
        return 0 if total["errors"] == 0 else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
