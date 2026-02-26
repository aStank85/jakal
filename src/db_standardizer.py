"""
db_standardizer.py
==================
Audits and repairs the JAKAL database, ensuring all match data is
parsed to the same schema regardless of which scraper version wrote it.

Three source formats exist in scraped_match_cards:
  1. 'summary'   - Playwright scraper, match-summary API
                   Has: operator, side, result per player per round
                   Missing: per-round kills/deaths/assists/hs (derived from killfeed)
  2. 'ow-ingest' - Playwright scraper, round-detail API
                   Has: full round stats per player from players[].rounds[]
                   operatorId is null for non-tracked players
  3. (future)    - TrackerAPI client (new syncs going forward, canonical format)

Repairs applied:
  R1. Reconstruct per-round kills/deaths from killfeed where missing (summary format)
  R2. Backfill round stats from players[].rounds[] for ow-ingest format
  R3. Resolve null usernames via player_id_tracker lookup across match tables
  R4. Normalize match_type values to canonical set (Ranked/Unranked/Quick Match)
  R5. Flag matches with data quality issues for future re-sync

Run as a script:
  python -m src.db_standardizer [--db data/jakal.db] [--dry-run] [--verbose]
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

MATCH_TYPE_MAP = {
    "ranked": "Ranked",
    "pvp_ranked": "Ranked",
    "Ranked": "Ranked",
    "unranked": "Unranked",
    "pvp_unranked": "Unranked",
    "Unranked": "Unranked",
    "casual": "Quick Match",
    "quick match": "Quick Match",
    "Quick Match": "Quick Match",
    "quickmatch": "Quick Match",
}

CANONICAL_MATCH_TYPES = {"Ranked", "Unranked", "Quick Match"}


@dataclass
class AuditReport:
    db_path: str
    run_at: str = field(default_factory=lambda: datetime.now().isoformat())
    total_matches: int = 0
    total_player_rounds: int = 0
    total_round_outcomes: int = 0

    null_usernames_found: int = 0
    null_usernames_fixed: int = 0
    bad_match_types_found: int = 0
    bad_match_types_fixed: int = 0
    summary_kills_missing: int = 0
    summary_kills_reconstructed: int = 0
    owingest_stats_missing: int = 0
    owingest_stats_fixed: int = 0
    killed_by_op_missing: int = 0
    killed_by_op_fixed: int = 0
    data_quality_flags: list[str] = field(default_factory=list)

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("DB STANDARDIZER - AUDIT REPORT")
        print("=" * 60)
        print(f"Database:             {self.db_path}")
        print(f"Run at:               {self.run_at}")
        print(f"Total matches:        {self.total_matches}")
        print(f"Total player_rounds:  {self.total_player_rounds}")
        print(f"Total round_outcomes: {self.total_round_outcomes}")
        print()
        print("--- Null Usernames ---")
        print(f"  Found:  {self.null_usernames_found}")
        print(f"  Fixed:  {self.null_usernames_fixed}")
        unfixed = self.null_usernames_found - self.null_usernames_fixed
        if unfixed:
            print(f"  Remaining (anonymous players, unfixable): {unfixed}")
        print()
        print("--- match_type normalization ---")
        print(f"  Non-canonical found:  {self.bad_match_types_found}")
        print(f"  Fixed:                {self.bad_match_types_fixed}")
        print()
        print("--- Summary format kill reconstruction ---")
        print(f"  Rounds missing kills: {self.summary_kills_missing}")
        print(f"  Reconstructed:        {self.summary_kills_reconstructed}")
        print()
        print("--- ow-ingest all-player stats backfill ---")
        print(f"  Player-rounds missing stats: {self.owingest_stats_missing}")
        print(f"  Fixed:                       {self.owingest_stats_fixed}")
        print("--- killed_by_operator backfill ---")
        print(f"  Deaths without resolved killer op: {self.killed_by_op_missing}")
        print(f"  Fixed:                              {self.killed_by_op_fixed}")
        if self.data_quality_flags:
            print()
            print("--- Data quality flags ---")
            for flag in self.data_quality_flags:
                print(f"  {flag}")
        print("=" * 60)


class DatabaseStandardizer:
    def __init__(self, db_path: str, dry_run: bool = False, verbose: bool = False):
        self.db_path = db_path
        self.dry_run = dry_run
        self.verbose = verbose
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.report = AuditReport(db_path=db_path)

        logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    def run(self) -> AuditReport:
        logger.info("Starting database standardization...")

        self._gather_baseline_counts()
        self._repair_match_types()
        self._repair_null_usernames()
        self._repair_summary_kills()
        self._repair_owingest_all_player_stats()
        self._repair_killed_by_operator()
        self._flag_data_quality_issues()

        if not self.dry_run:
            self.conn.commit()
            logger.info("Changes committed.")
        else:
            logger.info("DRY RUN - no changes written.")

        self.conn.close()
        return self.report

    def _gather_baseline_counts(self) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM scraped_match_cards")
        self.report.total_matches = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM player_rounds")
        self.report.total_player_rounds = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM round_outcomes")
        self.report.total_round_outcomes = cur.fetchone()[0]

    def _repair_match_types(self) -> None:
        logger.info("R4: Normalizing match_type values...")
        cur = self.conn.cursor()

        for table in ("player_rounds", "match_detail_players", "round_outcomes", "scraped_match_cards"):
            col = "mode" if table == "scraped_match_cards" else "match_type"
            try:
                cur.execute(f"SELECT DISTINCT {col} FROM {table}")
            except sqlite3.OperationalError:
                continue
            existing = [r[0] for r in cur.fetchall() if r[0]]
            for val in existing:
                canonical = MATCH_TYPE_MAP.get(val)
                if canonical and canonical != val:
                    self.report.bad_match_types_found += 1
                    logger.debug("  %s.%s: '%s' -> '%s'", table, col, val, canonical)
                    if not self.dry_run:
                        cur.execute(
                            f"UPDATE {table} SET {col} = ? WHERE {col} = ?",
                            (canonical, val),
                        )
                        self.report.bad_match_types_fixed += 1

    def _repair_null_usernames(self) -> None:
        logger.info("R3: Resolving null usernames...")
        cur = self.conn.cursor()

        cur.execute(
            "SELECT DISTINCT player_id_tracker, username FROM match_detail_players "
            "WHERE player_id_tracker IS NOT NULL AND username IS NOT NULL AND username != ''"
        )
        tracker_to_username = {r["player_id_tracker"]: r["username"] for r in cur.fetchall()}

        cur.execute(
            "SELECT DISTINCT player_id_tracker, username FROM player_rounds "
            "WHERE player_id_tracker IS NOT NULL AND username IS NOT NULL AND username != ''"
        )
        for row in cur.fetchall():
            tracker_to_username.setdefault(row["player_id_tracker"], row["username"])

        cur.execute(
            "SELECT id, player_id_tracker FROM player_rounds "
            "WHERE (username IS NULL OR username = '') AND player_id_tracker IS NOT NULL"
        )
        null_rows = cur.fetchall()
        self.report.null_usernames_found = len(null_rows)

        for row in null_rows:
            username = tracker_to_username.get(row["player_id_tracker"])
            if username:
                if not self.dry_run:
                    cur.execute(
                        "UPDATE player_rounds SET username = ? WHERE id = ?",
                        (username, row["id"]),
                    )
                self.report.null_usernames_fixed += 1

        cur.execute(
            "SELECT id, player_id_tracker FROM match_detail_players "
            "WHERE (username IS NULL OR username = '') AND player_id_tracker IS NOT NULL"
        )
        for row in cur.fetchall():
            username = tracker_to_username.get(row["player_id_tracker"])
            if username and not self.dry_run:
                cur.execute(
                    "UPDATE match_detail_players SET username = ? WHERE id = ?",
                    (username, row["id"]),
                )

    def _repair_summary_kills(self) -> None:
        logger.info("R1: Reconstructing kills/deaths from summary killfeed...")
        cur = self.conn.cursor()
        cur.execute(
            "SELECT match_id, round_data_json FROM scraped_match_cards "
            "WHERE round_data_source = 'summary' AND round_data_json IS NOT NULL "
            "AND TRIM(round_data_json) NOT IN ('', '{}', 'null')"
        )
        rows = cur.fetchall()
        logger.info("  Processing %s summary matches...", len(rows))

        for row in rows:
            match_id = row["match_id"]
            try:
                payload = json.loads(row["round_data_json"])
            except (json.JSONDecodeError, TypeError):
                logger.warning("  Bad JSON for match %s, skipping", match_id)
                continue

            killfeed = payload.get("killfeed", [])
            rounds = payload.get("rounds", [])

            kills_by_round: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            deaths_by_round: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))

            for event in killfeed:
                rid = event.get("roundId")
                attacker = event.get("attackerId")
                victim = event.get("victimId")
                if rid is not None and attacker:
                    kills_by_round[rid][attacker] += 1
                if rid is not None and victim:
                    deaths_by_round[rid][victim] += 1

            for rnd in rounds:
                rid = rnd.get("id")
                for player in rnd.get("players", []):
                    pid = player.get("id")
                    if not pid or rid is None:
                        continue

                    new_kills = kills_by_round[rid].get(pid, 0)
                    new_deaths = deaths_by_round[rid].get(pid, 0)

                    cur.execute(
                        "SELECT id, kills, deaths FROM player_rounds "
                        "WHERE match_id = ? AND round_id = ? AND player_id_tracker = ?",
                        (match_id, rid, pid),
                    )
                    pr = cur.fetchone()
                    if pr is None:
                        continue

                    stored_kills = pr["kills"] or 0
                    stored_deaths = pr["deaths"] or 0
                    if stored_kills != new_kills or stored_deaths != new_deaths:
                        self.report.summary_kills_missing += 1
                        if not self.dry_run:
                            cur.execute(
                                "UPDATE player_rounds SET kills = ?, deaths = ? WHERE id = ?",
                                (new_kills, new_deaths, pr["id"]),
                            )
                            self.report.summary_kills_reconstructed += 1

    def _repair_owingest_all_player_stats(self) -> None:
        logger.info("R2: Backfilling ow-ingest all-player round stats...")
        cur = self.conn.cursor()
        cur.execute(
            "SELECT match_id, round_data_json FROM scraped_match_cards "
            "WHERE round_data_source = 'ow-ingest' AND round_data_json IS NOT NULL "
            "AND TRIM(round_data_json) NOT IN ('', '{}', 'null')"
        )
        rows = cur.fetchall()
        logger.info("  Processing %s ow-ingest matches...", len(rows))

        for row in rows:
            match_id = row["match_id"]
            try:
                payload = json.loads(row["round_data_json"])
            except (json.JSONDecodeError, TypeError):
                continue

            players = payload.get("players", [])
            for player in players:
                pid = player.get("id")
                nickname = player.get("nickname") or player.get("pseudonym")
                if not pid:
                    continue

                for rnd in player.get("rounds", []):
                    rid = rnd.get("id")
                    stats = rnd.get("stats", {}) or {}
                    kills = stats.get("kills", 0) or 0
                    deaths = stats.get("deaths", 0) or 0
                    assists = stats.get("assists", 0) or 0
                    headshots = stats.get("headshots", 0) or 0

                    cur.execute(
                        "SELECT id, kills, deaths, assists, headshots, username FROM player_rounds "
                        "WHERE match_id = ? AND round_id = ? AND player_id_tracker = ?",
                        (match_id, rid, pid),
                    )
                    pr = cur.fetchone()
                    if pr is None:
                        continue

                    needs_update = (
                        (pr["kills"] or 0) != kills
                        or (pr["deaths"] or 0) != deaths
                        or (pr["assists"] or 0) != assists
                        or (pr["headshots"] or 0) != headshots
                    )
                    if needs_update:
                        self.report.owingest_stats_missing += 1
                        if not self.dry_run:
                            update_username = nickname if nickname and not pr["username"] else pr["username"]
                            cur.execute(
                                "UPDATE player_rounds SET kills=?, deaths=?, assists=?, headshots=?, username=? "
                                "WHERE id = ?",
                                (kills, deaths, assists, headshots, update_username, pr["id"]),
                            )
                            self.report.owingest_stats_fixed += 1

    def _repair_killed_by_operator(self) -> None:
        logger.info("R6: Backfilling killed_by_operator from killfeed...")
        cur = self.conn.cursor()

        try:
            cur.execute(
                """
                SELECT id, match_id, round_id, player_id_tracker
                FROM player_rounds
                WHERE deaths = 1 AND (killed_by_operator IS NULL OR killed_by_operator = '')
                """
            )
        except sqlite3.OperationalError:
            if self.dry_run:
                self.report.data_quality_flags.append(
                    "killed_by_operator column missing on player_rounds (run without --dry-run once to auto-add)."
                )
                return
            cur.execute("ALTER TABLE player_rounds ADD COLUMN killed_by_operator TEXT")
            self.conn.commit()
            cur.execute(
                """
                SELECT id, match_id, round_id, player_id_tracker
                FROM player_rounds
                WHERE deaths = 1 AND (killed_by_operator IS NULL OR killed_by_operator = '')
                """
            )

        death_rows = cur.fetchall()
        self.report.killed_by_op_missing = len(death_rows)
        if not death_rows:
            logger.info("  No death rows missing killed_by_operator.")
            return

        logger.info("  %s death rows need killed_by_operator...", len(death_rows))
        by_match: dict[str, list] = defaultdict(list)
        for row in death_rows:
            by_match[row["match_id"]].append(row)

        for match_id, rows in by_match.items():
            cur.execute(
                "SELECT round_data_json, round_data_source FROM scraped_match_cards WHERE match_id = ?",
                (match_id,),
            )
            card = cur.fetchone()
            if not card or not card["round_data_json"]:
                continue
            try:
                payload = json.loads(card["round_data_json"])
            except (json.JSONDecodeError, TypeError):
                continue

            source = card["round_data_source"]

            if source == "summary":
                killfeed = payload.get("killfeed", [])
                killer_lookup: dict[tuple[int, str], str] = {}
                for event in killfeed:
                    rid = event.get("roundId")
                    victim = event.get("victimId")
                    attacker_op = event.get("attackerOperatorName")
                    if rid is not None and victim and attacker_op:
                        killer_lookup[(rid, victim)] = attacker_op

                for row in rows:
                    op = killer_lookup.get((row["round_id"], row["player_id_tracker"]))
                    if not op:
                        continue
                    if not self.dry_run:
                        cur.execute(
                            "UPDATE player_rounds SET killed_by_operator = ? WHERE id = ?",
                            (op, row["id"]),
                        )
                    self.report.killed_by_op_fixed += 1

            elif source == "ow-ingest":
                killfeed = payload.get("killfeed", [])
                cur.execute(
                    "SELECT round_id, player_id_tracker, operator FROM player_rounds "
                    "WHERE match_id = ? AND operator IS NOT NULL AND operator != ''",
                    (match_id,),
                )
                op_lookup = {
                    (r["round_id"], r["player_id_tracker"]): r["operator"]
                    for r in cur.fetchall()
                }

                killer_lookup: dict[tuple[int, str], str] = {}
                for event in killfeed:
                    rid = event.get("roundId")
                    attacker_id = event.get("attackerId")
                    victim_id = event.get("victimId")
                    if rid is None or not attacker_id or not victim_id:
                        continue
                    attacker_op = op_lookup.get((rid, attacker_id))
                    if attacker_op:
                        killer_lookup[(rid, victim_id)] = attacker_op

                for row in rows:
                    op = killer_lookup.get((row["round_id"], row["player_id_tracker"]))
                    if not op:
                        continue
                    if not self.dry_run:
                        cur.execute(
                            "UPDATE player_rounds SET killed_by_operator = ? WHERE id = ?",
                            (op, row["id"]),
                        )
                    self.report.killed_by_op_fixed += 1

    def _flag_data_quality_issues(self) -> None:
        logger.info("R5: Flagging data quality issues...")
        cur = self.conn.cursor()

        cur.execute(
            """
            SELECT smc.match_id, smc.round_data_source
            FROM scraped_match_cards smc
            LEFT JOIN round_outcomes ro ON smc.match_id = ro.match_id
            WHERE ro.match_id IS NULL
            """
        )
        no_outcomes = cur.fetchall()
        if no_outcomes:
            self.report.data_quality_flags.append(
                f"{len(no_outcomes)} matches have no round_outcomes (consider re-sync): "
                + ", ".join(r["match_id"] for r in no_outcomes[:3])
                + ("..." if len(no_outcomes) > 3 else "")
            )

        cur.execute(
            """
            SELECT smc.match_id, smc.round_data_source
            FROM scraped_match_cards smc
            LEFT JOIN player_rounds pr ON smc.match_id = pr.match_id
            WHERE pr.match_id IS NULL
            """
        )
        no_rounds = cur.fetchall()
        if no_rounds:
            self.report.data_quality_flags.append(
                f"{len(no_rounds)} matches have no player_rounds at all"
            )

        cur.execute("SELECT COUNT(*) FROM player_rounds WHERE operator IS NULL OR operator = ''")
        null_ops = cur.fetchone()[0]
        if null_ops:
            self.report.data_quality_flags.append(
                f"{null_ops} player_rounds still have null operator (ow-ingest operatorId gap)"
            )

        cur.execute("SELECT COUNT(*) FROM player_rounds WHERE username IS NULL OR username = ''")
        still_null = cur.fetchone()[0]
        if still_null:
            self.report.data_quality_flags.append(
                f"{still_null} player_rounds have unresolvable null username (anonymous/private players)"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="JAKAL DB Standardizer")
    parser.add_argument("--db", default="data/jakal.db", help="Path to SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Audit only, no writes")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"Database not found: {args.db}")
        return 1

    standardizer = DatabaseStandardizer(args.db, dry_run=args.dry_run, verbose=args.verbose)
    report = standardizer.run()
    report.print_summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
