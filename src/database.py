# src/database.py

import sqlite3
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import json
import time

class Database:
    """Handle all database operations."""
    
    def __init__(self, db_path: str = 'data/jakal.db'):
        self.db_path = self._resolve_db_path(db_path)
        self.conn = None
        self.init_database()

    @staticmethod
    def _resolve_db_path(db_path: str) -> str:
        """Return an absolute database path anchored to project root when relative."""
        path = Path(db_path)
        if path.is_absolute():
            return str(path)

        project_root = Path(__file__).resolve().parents[2]
        return str(project_root / path)
    
    def init_database(self):
        """Create tables if they don't exist."""
        try:
            # Ensure parent directory exists for the database file
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                try:
                    os.makedirs(db_dir, exist_ok=True)
                except OSError as e:
                    raise RuntimeError(f"Failed to create database directory '{db_dir}': {e}")

            self.conn = sqlite3.connect(self.db_path, timeout=30.0)
            self.conn.row_factory = sqlite3.Row  # Access columns by name
            self.conn.execute("PRAGMA busy_timeout = 30000")
            self.conn.execute("PRAGMA foreign_keys = ON")
            # WAL improves concurrency, but enabling it requires a write lock.
            # If another process holds the DB briefly, keep startup non-fatal.
            self._set_wal_mode_best_effort()

            cursor = self.conn.cursor()

            # Players table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    tracker_uuid TEXT,
                    device_tag TEXT DEFAULT 'pc',
                    tag TEXT DEFAULT 'untagged',
                    last_match_synced_at TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT
                )
            """)

            # Stats snapshots table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stats_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                snapshot_date DATE NOT NULL,
                snapshot_time TIME,
                season TEXT,
                
                -- Game stats
                abandons INTEGER,
                matches INTEGER,
                wins INTEGER,
                losses INTEGER,
                match_win_pct REAL,
                time_played_hours REAL,
                score INTEGER,
                
                -- Round stats
                rounds_played INTEGER,
                rounds_wins INTEGER,
                rounds_losses INTEGER,
                rounds_win_pct REAL,
                disconnected INTEGER,
                
                -- Combat stats
                kills INTEGER,
                deaths INTEGER,
                assists INTEGER,
                kd REAL,
                kills_per_round REAL,
                deaths_per_round REAL,
                assists_per_round REAL,
                kills_per_game REAL,
                headshots INTEGER,
                headshots_per_round REAL,
                hs_pct REAL,
                first_bloods INTEGER,
                first_deaths INTEGER,
                teamkills INTEGER,
                esr REAL,
                
                -- Clutch stats (stored as JSON for flexibility)
                clutches_data TEXT,
                
                -- Multikill stats
                aces INTEGER,
                kills_3k INTEGER,
                kills_4k INTEGER,
                kills_2k INTEGER,
                kills_1k INTEGER,
                
                -- Ranked stats
                current_rank INTEGER,
                max_rank INTEGER,
                top_rank_position INTEGER,

                -- Uncategorized stats
                rank_points INTEGER,
                max_rank_points INTEGER,
                trn_elo INTEGER,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            )
            """)

            # Computed metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS computed_metrics (
                    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    player_id INTEGER NOT NULL,

                    -- Entry metrics
                    entry_efficiency REAL,
                    aggression_score REAL,

                    -- Clutch metrics
                    clutch_attempt_rate REAL,
                    clutch_1v1_success REAL,
                    clutch_disadvantaged_success REAL,
                    overall_clutch_success REAL,
                    clutch_dropoff_rate REAL,
                    clutch_efficiency_score REAL,

                    -- Teamplay metrics
                    teamplay_index REAL,

                    -- Role scores
                    fragger_score REAL,
                    entry_score REAL,
                    support_score REAL,
                    anchor_score REAL,
                    clutch_specialist_score REAL,
                    carry_score REAL,

                    -- Role classification
                    primary_role TEXT,
                    primary_confidence REAL,
                    secondary_role TEXT,
                    secondary_confidence REAL,

                    -- Additional metrics
                    impact_rating REAL,
                    wins_per_hour REAL,
                    kd_win_gap REAL,

                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    FOREIGN KEY (snapshot_id) REFERENCES stats_snapshots(snapshot_id),
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # Stacks table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stacks (
                    stack_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    stack_name      TEXT NOT NULL,
                    stack_type      TEXT DEFAULT 'named',
                    description     TEXT,
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Stack members table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stack_members (
                    member_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    stack_id        INTEGER NOT NULL,
                    player_id       INTEGER NOT NULL,
                    role_override   TEXT,
                    joined_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (stack_id) REFERENCES stacks(stack_id),
                    FOREIGN KEY (player_id) REFERENCES players(player_id),
                    UNIQUE(stack_id, player_id)
                )
            """)

            # Stack analyses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stack_analyses (
                    analysis_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    stack_id            INTEGER NOT NULL,
                    analysis_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    role_distribution   TEXT,
                    roles_covered       TEXT,
                    roles_missing       TEXT,
                    composition_score   REAL,
                    team_avg_kd         REAL,
                    team_avg_win_pct    REAL,
                    team_avg_hs_pct     REAL,
                    team_avg_kpr        REAL,
                    team_avg_apr        REAL,
                    team_entry_efficiency   REAL,
                    team_first_blood_rate   REAL,
                    dedicated_entry_count   INTEGER,
                    team_clutch_success     REAL,
                    team_1v1_success        REAL,
                    primary_clutch_player   TEXT,
                    clutch_gap              REAL,
                    carry_player            TEXT,
                    carry_dependency        REAL,
                    team_strengths          TEXT,
                    team_weaknesses         TEXT,
                    team_insights           TEXT,
                    FOREIGN KEY (stack_id) REFERENCES stacks(stack_id)
                )
            """)

            # Matchup analyses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS matchup_analyses (
                    matchup_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    stack_a_id          INTEGER NOT NULL,
                    stack_b_id          INTEGER NOT NULL,
                    analysis_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    kd_advantage            TEXT,
                    entry_advantage         TEXT,
                    clutch_advantage        TEXT,
                    support_advantage       TEXT,
                    hs_advantage            TEXT,
                    win_rate_advantage      TEXT,
                    predicted_winner        TEXT,
                    confidence              REAL,
                    recommendations         TEXT,
                    key_battlegrounds       TEXT,
                    role_matchups           TEXT,
                    FOREIGN KEY (stack_a_id) REFERENCES stacks(stack_id),
                    FOREIGN KEY (stack_b_id) REFERENCES stacks(stack_id)
                )
            """)

            # Map stats table (scraped)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS map_stats (
                    map_stat_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id       INTEGER NOT NULL,
                    snapshot_id     INTEGER,
                    season          TEXT DEFAULT 'Y10S4',
                    map_name        TEXT NOT NULL,
                    matches         INTEGER,
                    win_pct         REAL,
                    wins            INTEGER,
                    losses          INTEGER,
                    kd              REAL,
                    atk_win_pct     REAL,
                    def_win_pct     REAL,
                    hs_pct          REAL,
                    esr             REAL,
                    scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # Operator stats table (scraped)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS operator_stats (
                    op_stat_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id       INTEGER NOT NULL,
                    snapshot_id     INTEGER,
                    season          TEXT DEFAULT 'Y10S4',
                    operator_name   TEXT NOT NULL,
                    rounds          INTEGER,
                    win_pct         REAL,
                    kd              REAL,
                    hs_pct          REAL,
                    kills           INTEGER,
                    deaths          INTEGER,
                    wins            INTEGER,
                    losses          INTEGER,
                    assists         INTEGER,
                    aces            INTEGER,
                    teamkills       INTEGER,
                    scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # Match history table (scraped)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS match_history (
                    match_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id       INTEGER NOT NULL,
                    time_ago        TEXT,
                    map_name        TEXT,
                    mode            TEXT,
                    score           TEXT,
                    result          TEXT,
                    rp              INTEGER,
                    rp_change       INTEGER,
                    kd              REAL,
                    kda             TEXT,
                    hs_pct          REAL,
                    had_ace         INTEGER DEFAULT 0,
                    had_4k          INTEGER DEFAULT 0,
                    had_3k          INTEGER DEFAULT 0,
                    had_2k          INTEGER DEFAULT 0,
                    scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # Match detail player leaderboard table (scraped)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS match_players (
                    match_player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id        INTEGER NOT NULL,
                    team            TEXT,
                    username        TEXT,
                    rp              INTEGER,
                    rp_change       INTEGER,
                    kd              REAL,
                    kills           INTEGER,
                    deaths          INTEGER,
                    assists         INTEGER,
                    hs_pct          REAL,
                    first_kills     INTEGER,
                    first_deaths    INTEGER,
                    clutches        INTEGER,
                    operators       TEXT,
                    FOREIGN KEY (match_id) REFERENCES match_history(match_id)
                )
            """)

            # API match detail players table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS match_detail_players (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id           INTEGER NOT NULL,
                    match_id            TEXT NOT NULL,
                    match_type          TEXT,
                    player_id_tracker   TEXT,
                    username            TEXT,
                    team_id             INTEGER,
                    result              TEXT,
                    kills               INTEGER,
                    deaths              INTEGER,
                    assists             INTEGER,
                    headshots           INTEGER,
                    first_bloods        INTEGER,
                    first_deaths        INTEGER,
                    clutches_won        INTEGER,
                    clutches_lost       INTEGER,
                    clutches_1v1        INTEGER,
                    clutches_1v2        INTEGER,
                    clutches_1v3        INTEGER,
                    clutches_1v4        INTEGER,
                    clutches_1v5        INTEGER,
                    kills_1k            INTEGER,
                    kills_2k            INTEGER,
                    kills_3k            INTEGER,
                    kills_4k            INTEGER,
                    kills_5k            INTEGER,
                    rounds_won          INTEGER,
                    rounds_lost         INTEGER,
                    rank_points         INTEGER,
                    rank_points_delta   INTEGER,
                    rank_points_previous INTEGER,
                    kd_ratio            REAL,
                    hs_pct              REAL,
                    esr                 REAL,
                    kills_per_round     REAL,
                    time_played_ms      INTEGER,
                    elo                 INTEGER,
                    elo_delta           INTEGER,
                    scraped_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # API round outcomes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS round_outcomes (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id       INTEGER NOT NULL,
                    match_id        TEXT NOT NULL,
                    match_type      TEXT,
                    round_id        INTEGER NOT NULL,
                    end_reason      TEXT,
                    winner_side     TEXT,
                    scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # API player-round table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS player_rounds (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id           INTEGER NOT NULL,
                    match_id            TEXT NOT NULL,
                    match_type          TEXT,
                    round_id            INTEGER NOT NULL,
                    player_id_tracker   TEXT,
                    username            TEXT,
                    team_id             INTEGER,
                    side                TEXT,
                    operator            TEXT,
                    result              TEXT,
                    is_disconnected     INTEGER DEFAULT 0,
                    kills               INTEGER,
                    deaths              INTEGER,
                    assists             INTEGER,
                    headshots           INTEGER,
                    first_blood         INTEGER,
                    first_death         INTEGER,
                    clutch_won          INTEGER,
                    clutch_lost         INTEGER,
                    hs_pct              REAL,
                    esr                 REAL,
                    scraped_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                )
            """)

            # Scraped match cards from live websocket scraper
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraped_match_cards (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    username        TEXT NOT NULL,
                    match_id        TEXT,
                    map_name        TEXT,
                    mode            TEXT,
                    score_team_a    INTEGER,
                    score_team_b    INTEGER,
                    duration        TEXT,
                    match_date      TEXT,
                    players_json    TEXT,
                    rounds_json     TEXT,
                    summary_json    TEXT,
                    round_data_json TEXT,
                    round_data_source TEXT,
                    scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_scraped_match_cards_username_scraped_at
                ON scraped_match_cards (username, scraped_at DESC)
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scrape_checkpoints (
                    username        TEXT NOT NULL,
                    mode_key        TEXT NOT NULL,
                    filter_key      TEXT NOT NULL,
                    skip_count      INTEGER NOT NULL DEFAULT 0,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (username, mode_key, filter_key)
                )
            """)

            self._commit_with_retry(context="init schema commit")
            self._migrate_schema()
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to initialize database at '{self.db_path}': {e}")

    def _migrate_schema(self) -> None:
        """
        Apply additive, idempotent schema migrations for older local databases.
        """
        try:
            self._migrate_players_table()
            self._migrate_stats_snapshots_table()
            self._migrate_computed_metrics_table()
            self._migrate_match_analysis_tables()
            self._commit_with_retry(context="migrate schema commit")
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to migrate database schema: {e}")

    def _commit_with_retry(self, retries: int = 8, delay_seconds: float = 0.25, context: str = "commit") -> None:
        """
        Retry commit on transient SQLITE_BUSY/locked errors.
        """
        last_error = None
        for attempt in range(retries):
            try:
                self.conn.commit()
                return
            except sqlite3.OperationalError as e:
                last_error = e
                if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                    raise
                if attempt == retries - 1:
                    break
                time.sleep(delay_seconds)
        raise RuntimeError(
            f"Failed to {context}: database remained locked after {retries} attempts ({last_error})"
        )

    def _set_wal_mode_best_effort(self, retries: int = 5, delay_seconds: float = 0.2) -> None:
        """Try to enable WAL without failing startup if the DB is temporarily locked."""
        for attempt in range(retries):
            try:
                self.conn.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "locked" not in msg and "busy" not in msg:
                    raise
                if attempt == retries - 1:
                    print(f"[DB] Warning: could not enable WAL mode (database locked); continuing. ({e})")
                    return
                time.sleep(delay_seconds)

    def _get_table_columns(self, table_name: str) -> set:
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {row["name"] for row in cursor.fetchall()}

    def _add_column_if_missing(self, table_name: str, column_sql: str, column_name: str) -> None:
        columns = self._get_table_columns(table_name)
        if column_name not in columns:
            cursor = self.conn.cursor()
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")

    def _migrate_players_table(self) -> None:
        self._add_column_if_missing("players", "tracker_uuid TEXT", "tracker_uuid")
        self._add_column_if_missing("players", "device_tag TEXT DEFAULT 'pc'", "device_tag")
        self._add_column_if_missing("players", "tag TEXT DEFAULT 'untagged'", "tag")
        self._add_column_if_missing("players", "last_match_synced_at TEXT", "last_match_synced_at")
        self._add_column_if_missing("players", "notes TEXT", "notes")

        cursor = self.conn.cursor()
        cursor.execute("UPDATE players SET device_tag = 'pc' WHERE device_tag IS NULL")

    def _migrate_stats_snapshots_table(self) -> None:
        self._add_column_if_missing("stats_snapshots", "score INTEGER", "score")
        self._add_column_if_missing("stats_snapshots", "kills_per_game REAL", "kills_per_game")
        self._add_column_if_missing("stats_snapshots", "headshots_per_round REAL", "headshots_per_round")
        self._add_column_if_missing("stats_snapshots", "top_rank_position INTEGER", "top_rank_position")

    def _migrate_computed_metrics_table(self) -> None:
        self._add_column_if_missing("computed_metrics", "player_id INTEGER", "player_id")
        self._add_column_if_missing("computed_metrics", "overall_clutch_success REAL", "overall_clutch_success")
        self._add_column_if_missing("computed_metrics", "clutch_dropoff_rate REAL", "clutch_dropoff_rate")
        self._add_column_if_missing("computed_metrics", "clutch_efficiency_score REAL", "clutch_efficiency_score")
        self._add_column_if_missing("computed_metrics", "impact_rating REAL", "impact_rating")
        self._add_column_if_missing("computed_metrics", "wins_per_hour REAL", "wins_per_hour")
        self._add_column_if_missing("computed_metrics", "kd_win_gap REAL", "kd_win_gap")
        self._add_column_if_missing(
            "computed_metrics", "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "created_at"
        )

        # Backfill player_id for legacy rows now that the column exists.
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE computed_metrics
            SET player_id = (
                SELECT s.player_id
                FROM stats_snapshots s
                WHERE s.snapshot_id = computed_metrics.snapshot_id
            )
            WHERE player_id IS NULL
        """)

    def _migrate_match_analysis_tables(self) -> None:
        self._add_column_if_missing("match_detail_players", "match_type TEXT", "match_type")
        self._add_column_if_missing("round_outcomes", "match_type TEXT", "match_type")
        self._add_column_if_missing("player_rounds", "match_type TEXT", "match_type")
        self._add_column_if_missing("scraped_match_cards", "round_data_source TEXT", "round_data_source")

        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE scraped_match_cards
            SET round_data_source = 'ow-ingest'
            WHERE (round_data_source IS NULL OR TRIM(round_data_source) = '')
              AND round_data_json IS NOT NULL
              AND TRIM(round_data_json) NOT IN ('', '{}', 'null')
            """
        )

        # Backfill match_type from scraped cards using username+match_id when available.
        cursor.execute(
            """
            UPDATE match_detail_players
            SET match_type = (
                SELECT smc.mode
                FROM scraped_match_cards smc
                WHERE smc.match_id = match_detail_players.match_id
                  AND LOWER(TRIM(smc.username)) = LOWER(TRIM(match_detail_players.username))
                ORDER BY smc.id DESC
                LIMIT 1
            )
            WHERE (match_type IS NULL OR TRIM(match_type) = '')
            """
        )
        cursor.execute(
            """
            UPDATE player_rounds
            SET match_type = (
                SELECT smc.mode
                FROM scraped_match_cards smc
                WHERE smc.match_id = player_rounds.match_id
                  AND LOWER(TRIM(smc.username)) = LOWER(TRIM(player_rounds.username))
                ORDER BY smc.id DESC
                LIMIT 1
            )
            WHERE (match_type IS NULL OR TRIM(match_type) = '')
            """
        )

        # round_outcomes has no username; map via player_rounds first.
        cursor.execute(
            """
            UPDATE round_outcomes
            SET match_type = (
                SELECT smc.mode
                FROM player_rounds pr
                JOIN scraped_match_cards smc
                  ON smc.match_id = pr.match_id
                 AND LOWER(TRIM(smc.username)) = LOWER(TRIM(pr.username))
                WHERE pr.player_id = round_outcomes.player_id
                  AND pr.match_id = round_outcomes.match_id
                ORDER BY smc.id DESC
                LIMIT 1
            )
            WHERE (match_type IS NULL OR TRIM(match_type) = '')
            """
        )

        # Fallback: any card with same match_id.
        for table_name in ("match_detail_players", "round_outcomes", "player_rounds"):
            cursor.execute(
                f"""
                UPDATE {table_name}
                SET match_type = (
                    SELECT smc.mode
                    FROM scraped_match_cards smc
                    WHERE smc.match_id = {table_name}.match_id
                    ORDER BY smc.id DESC
                    LIMIT 1
                )
                WHERE (match_type IS NULL OR TRIM(match_type) = '')
                """
            )
    
    def add_player(self, username: str, device_tag: str = "pc") -> int:
        """Add a player or return existing player_id."""
        try:
            cursor = self.conn.cursor()

            # Check if player exists
            cursor.execute("SELECT player_id, device_tag FROM players WHERE username = ?", (username,))
            row = cursor.fetchone()

            if row:
                if device_tag and row["device_tag"] != device_tag:
                    cursor.execute(
                        "UPDATE players SET device_tag = ? WHERE player_id = ?",
                        (device_tag, row["player_id"]),
                    )
                    self.conn.commit()
                return row['player_id']

            # Insert new player
            cursor.execute(
                "INSERT INTO players (username, device_tag) VALUES (?, ?)",
                (username, device_tag),
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to add player '{username}': {e}")
    
    def add_stats_snapshot(
        self,
        username: str,
        stats: Dict[str, Any],
        snapshot_date: str,
        snapshot_time: Optional[str] = None,
        season: str = "Y10S4",
        device_tag: str = "pc"
    ) -> int:
        """
        Add a stats snapshot for a player.

        Args:
            username: Player username
            stats: Parsed stats dictionary
            snapshot_date: Date in YYYY-MM-DD format
            snapshot_time: Time in HH:MM:SS format
            season: Season identifier

        Returns:
            snapshot_id
        """
        try:
            player_id = self.add_player(username, device_tag=device_tag)

            cursor = self.conn.cursor()

            # Prepare clutch data as JSON
            clutches_json = json.dumps(stats.get('clutches', {}))

            cursor.execute("""
                INSERT INTO stats_snapshots (
                player_id, snapshot_date, snapshot_time, season,
                abandons, matches, wins, losses, match_win_pct, time_played_hours, score,
                rounds_played, rounds_wins, rounds_losses, rounds_win_pct, disconnected,
                kills, deaths, assists, kd, kills_per_round, deaths_per_round,
                assists_per_round, kills_per_game, headshots, headshots_per_round, hs_pct, first_bloods, first_deaths,
                teamkills, esr,
                clutches_data,
                aces, kills_3k, kills_4k, kills_2k, kills_1k,
                current_rank, max_rank, top_rank_position,
                rank_points, max_rank_points, trn_elo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            player_id, snapshot_date, snapshot_time, season,
            stats['game'].get('abandons'),
            stats['game'].get('matches'),
            stats['game'].get('wins'),
            stats['game'].get('losses'),
            stats['game'].get('match_win_pct'),
            stats['game'].get('time_played_hours'),
            stats['game'].get('score'),
            stats['rounds'].get('rounds_played'),
            stats['rounds'].get('rounds_wins'),
            stats['rounds'].get('rounds_losses'),
            stats['rounds'].get('win_pct'),
            stats['rounds'].get('disconnected'),
            stats['combat'].get('kills'),
            stats['combat'].get('deaths'),
            stats['combat'].get('assists'),
            stats['combat'].get('kd'),
            stats['combat'].get('kills_per_round'),
            stats['combat'].get('deaths_per_round'),
            stats['combat'].get('assists_per_round'),
            stats['combat'].get('kills_per_game'),
            stats['combat'].get('headshots'),
            stats['combat'].get('headshots_per_round'),
            stats['combat'].get('hs_pct'),
            stats['combat'].get('first_bloods'),
            stats['combat'].get('first_deaths'),
            stats['combat'].get('teamkills'),
            stats['combat'].get('esr'),
            clutches_json,
            stats['multikills'].get('aces'),
            stats['multikills'].get('3k'),
            stats['multikills'].get('4k'),
            stats['multikills'].get('2k'),
            stats['multikills'].get('1k'),
            stats['ranked'].get('current_rank'),
            stats['ranked'].get('max_rank'),
            stats['ranked'].get('top_rank_position'),
            stats['uncategorized'].get('rank_points'),
            stats['uncategorized'].get('max_rank_points'),
            stats['uncategorized'].get('trn_elo')
        ))

            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to add stats snapshot for '{username}': {e}")
    
    def get_player_snapshots(self, username: str) -> List[Dict]:
        """Get all snapshots for a player."""
        try:
            cursor = self.conn.cursor()

            cursor.execute("""
                SELECT s.*, p.username, p.device_tag
                FROM stats_snapshots s
                JOIN players p ON s.player_id = p.player_id
                WHERE p.username = ?
                ORDER BY s.snapshot_date DESC, s.snapshot_time DESC
            """, (username,))

            rows = cursor.fetchall()

            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get snapshots for '{username}': {e}")
    
    def get_latest_snapshot(self, username: str) -> Optional[Dict]:
        """Get most recent snapshot for a player."""
        snapshots = self.get_player_snapshots(username)
        return snapshots[0] if snapshots else None
    
    def get_all_players(self) -> List[Dict]:
        """Get list of all players with their details."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT player_id, username, tracker_uuid, device_tag, tag, created_at, notes FROM players ORDER BY username"
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get players list: {e}")

    def add_computed_metrics(self, snapshot_id: int, player_id: int, metrics: Dict[str, Any]) -> int:
        """
        Add computed metrics for a snapshot.

        Args:
            snapshot_id: ID of the stats snapshot
            player_id: ID of the player
            metrics: Dictionary of computed metrics from MetricsCalculator

        Returns:
            metric_id
        """
        try:
            cursor = self.conn.cursor()

            cursor.execute("""
                INSERT INTO computed_metrics (
                    snapshot_id, player_id,
                    entry_efficiency, aggression_score,
                    clutch_attempt_rate, clutch_1v1_success,
                    clutch_disadvantaged_success, overall_clutch_success,
                    clutch_dropoff_rate, clutch_efficiency_score,
                    teamplay_index,
                    fragger_score, entry_score, support_score, anchor_score,
                    clutch_specialist_score, carry_score,
                    primary_role, primary_confidence, secondary_role, secondary_confidence,
                    impact_rating, wins_per_hour, kd_win_gap
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_id,
                player_id,
                metrics.get('entry_efficiency'),
                metrics.get('aggression_score'),
                metrics.get('clutch_attempt_rate'),
                metrics.get('clutch_1v1_success'),
                metrics.get('clutch_disadvantaged_success'),
                metrics.get('overall_clutch_success'),
                metrics.get('clutch_dropoff_rate'),
                metrics.get('clutch_efficiency_score'),
                metrics.get('teamplay_index'),
                metrics.get('fragger_score'),
                metrics.get('entry_score'),
                metrics.get('support_score'),
                metrics.get('anchor_score'),
                metrics.get('clutch_specialist_score'),
                metrics.get('carry_score'),
                metrics.get('primary_role'),
                metrics.get('primary_confidence'),
                metrics.get('secondary_role'),
                metrics.get('secondary_confidence'),
                metrics.get('impact_rating'),
                metrics.get('wins_per_hour'),
                metrics.get('kd_win_gap')
            ))

            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to add computed metrics for snapshot {snapshot_id}: {e}")

    def get_player(self, username: str) -> Optional[Dict]:
        """Get a single player by username."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT player_id, username, tracker_uuid, device_tag, tag, created_at, notes
                FROM players
                WHERE username = ?
            """, (username,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get player '{username}': {e}")

    def get_player_last_match_synced_at(self, username: str) -> Optional[str]:
        """Get incremental sync watermark timestamp for a player."""
        player = self.get_player(username)
        if not player:
            return None
        return player.get("last_match_synced_at")

    def update_player_last_match_synced_at(self, username: str, ts: str) -> None:
        """Update incremental sync watermark timestamp for a player."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE players SET last_match_synced_at = ? WHERE username = ?",
                (ts, username),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to update last_match_synced_at for '{username}': {e}")

    def update_player_tracker_uuid(self, username: str, tracker_uuid: str) -> None:
        """Persist tracker platform UUID for a player."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE players SET tracker_uuid = ? WHERE username = ?",
                (tracker_uuid, username),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to update tracker_uuid for '{username}': {e}")

    def get_player_id(self, username: str) -> Optional[int]:
        """Get player_id for username, or None if not found."""
        player = self.get_player(username)
        return player["player_id"] if player else None

    def update_player_tag(self, username: str, tag: str) -> None:
        """Update a player's tag."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE players
                SET tag = ?
                WHERE username = ?
            """, (tag, username))
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to update tag for '{username}': {e}")

    def delete_player(self, username: str) -> None:
        """Delete a player and all their snapshots."""
        try:
            cursor = self.conn.cursor()

            # Get player_id first
            cursor.execute("SELECT player_id FROM players WHERE username = ?", (username,))
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(f"Player '{username}' not found")

            player_id = row['player_id']

            # Delete computed metrics for this player
            cursor.execute("DELETE FROM computed_metrics WHERE player_id = ?", (player_id,))

            # Delete snapshots
            cursor.execute("DELETE FROM stats_snapshots WHERE player_id = ?", (player_id,))

            # Delete player
            cursor.execute("DELETE FROM players WHERE player_id = ?", (player_id,))

            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete player '{username}': {e}")

    def get_all_snapshots(self, username: str) -> List[Dict]:
        """Get all snapshots for a player (alias for get_player_snapshots)."""
        return self.get_player_snapshots(username)

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Dict]:
        """Get a specific snapshot by ID."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT s.*, p.username, p.device_tag
                FROM stats_snapshots s
                JOIN players p ON s.player_id = p.player_id
                WHERE s.snapshot_id = ?
            """, (snapshot_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get snapshot {snapshot_id}: {e}")

    def delete_snapshot(self, snapshot_id: int) -> None:
        """Delete a snapshot and its computed metrics."""
        try:
            cursor = self.conn.cursor()

            # Delete computed metrics for this snapshot
            cursor.execute("DELETE FROM computed_metrics WHERE snapshot_id = ?", (snapshot_id,))

            # Delete snapshot
            cursor.execute("DELETE FROM stats_snapshots WHERE snapshot_id = ?", (snapshot_id,))

            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete snapshot {snapshot_id}: {e}")

    def get_computed_metrics(self, snapshot_id: int) -> Optional[Dict]:
        """Get computed metrics for a specific snapshot."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT * FROM computed_metrics
                WHERE snapshot_id = ?
            """, (snapshot_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get metrics for snapshot {snapshot_id}: {e}")

    def get_latest_metrics(self, username: str) -> Optional[Dict]:
        """Get computed metrics for a player's most recent snapshot."""
        try:
            snapshot = self.get_latest_snapshot(username)
            if not snapshot:
                return None
            return self.get_computed_metrics(snapshot['snapshot_id'])
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get latest metrics for '{username}': {e}")

    def player_exists(self, username: str) -> bool:
        """Check if a player exists."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM players WHERE username = ? LIMIT 1", (username,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to check if player exists: {e}")

    def snapshot_count(self, username: str) -> int:
        """Get the number of snapshots for a player."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM stats_snapshots s
                JOIN players p ON s.player_id = p.player_id
                WHERE p.username = ?
            """, (username,))
            row = cursor.fetchone()
            return row['count'] if row else 0
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get snapshot count for '{username}': {e}")

    def get_all_seasons(self) -> List[str]:
        """Get a list of all seasons in the database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT DISTINCT season
                FROM stats_snapshots
                WHERE season IS NOT NULL
                ORDER BY season DESC
            """)
            return [row['season'] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get seasons list: {e}")

    # --- Stack CRUD ---

    def create_stack(self, name: str, stack_type: str = 'named', description: str = None) -> int:
        """Create a new stack."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO stacks (stack_name, stack_type, description) VALUES (?, ?, ?)",
            (name, stack_type, description)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_stack(self, stack_id: int) -> Optional[Dict]:
        """Get a stack by ID."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stacks WHERE stack_id = ?", (stack_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_stack_by_name(self, name: str) -> Optional[Dict]:
        """Get a stack by name."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stacks WHERE stack_name = ?", (name,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_stacks(self) -> List[Dict]:
        """Get all stacks."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stacks ORDER BY created_at DESC")
        return [dict(row) for row in cursor.fetchall()]

    def update_stack(self, stack_id: int, name: str = None, description: str = None) -> None:
        """Update stack name and/or description."""
        cursor = self.conn.cursor()
        if name is not None:
            cursor.execute("UPDATE stacks SET stack_name = ?, updated_at = CURRENT_TIMESTAMP WHERE stack_id = ?", (name, stack_id))
        if description is not None:
            cursor.execute("UPDATE stacks SET description = ?, updated_at = CURRENT_TIMESTAMP WHERE stack_id = ?", (description, stack_id))
        self.conn.commit()

    def delete_stack(self, stack_id: int) -> None:
        """Delete a stack and its members."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM stack_members WHERE stack_id = ?", (stack_id,))
        cursor.execute("DELETE FROM stack_analyses WHERE stack_id = ?", (stack_id,))
        cursor.execute("DELETE FROM stacks WHERE stack_id = ?", (stack_id,))
        self.conn.commit()

    def add_member_to_stack(self, stack_id: int, player_id: int, role_override: str = None) -> int:
        """Add a player to a stack."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO stack_members (stack_id, player_id, role_override) VALUES (?, ?, ?)",
            (stack_id, player_id, role_override)
        )
        self.conn.commit()
        return cursor.lastrowid

    def remove_member_from_stack(self, stack_id: int, player_id: int) -> None:
        """Remove a player from a stack."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM stack_members WHERE stack_id = ? AND player_id = ?",
            (stack_id, player_id)
        )
        self.conn.commit()

    def get_stack_members(self, stack_id: int) -> List[Dict]:
        """Get all members of a stack with player info."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT sm.*, p.username
            FROM stack_members sm
            JOIN players p ON sm.player_id = p.player_id
            WHERE sm.stack_id = ?
            ORDER BY sm.joined_at
        """, (stack_id,))
        return [dict(row) for row in cursor.fetchall()]

    def get_stack_size(self, stack_id: int) -> int:
        """Get number of players in a stack."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM stack_members WHERE stack_id = ?", (stack_id,))
        return cursor.fetchone()['cnt']

    def delete_stacks_by_type(self, stack_type: str) -> None:
        """Delete all stacks of a given type (e.g. 'quick')."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT stack_id FROM stacks WHERE stack_type = ?", (stack_type,))
        for row in cursor.fetchall():
            self.delete_stack(row['stack_id'])

    def save_stack_analysis(self, stack_id: int, analysis: Dict[str, Any]) -> int:
        """Save a team analysis result."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO stack_analyses (
                stack_id, role_distribution, roles_covered, roles_missing,
                composition_score, team_avg_kd, team_avg_win_pct,
                team_avg_hs_pct, team_avg_kpr, team_avg_apr,
                team_entry_efficiency, team_first_blood_rate, dedicated_entry_count,
                team_clutch_success, team_1v1_success, primary_clutch_player,
                clutch_gap, carry_player, carry_dependency,
                team_strengths, team_weaknesses, team_insights
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stack_id,
            json.dumps(analysis.get('role_distribution')),
            json.dumps(analysis.get('roles_covered')),
            json.dumps(analysis.get('roles_missing')),
            analysis.get('composition_score'),
            analysis.get('team_avg_kd'),
            analysis.get('team_avg_win_pct'),
            analysis.get('team_avg_hs_pct'),
            analysis.get('team_avg_kpr'),
            analysis.get('team_avg_apr'),
            analysis.get('team_entry_efficiency'),
            analysis.get('team_first_blood_rate'),
            analysis.get('dedicated_entry_count'),
            analysis.get('team_clutch_success'),
            analysis.get('team_1v1_success'),
            analysis.get('primary_clutch_player'),
            analysis.get('clutch_gap'),
            analysis.get('carry_player'),
            analysis.get('carry_dependency'),
            json.dumps(analysis.get('team_strengths')),
            json.dumps(analysis.get('team_weaknesses')),
            json.dumps(analysis.get('team_insights'))
        ))
        self.conn.commit()
        return cursor.lastrowid

    def save_matchup_analysis(self, matchup: Dict[str, Any]) -> int:
        """Save a matchup analysis result."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO matchup_analyses (
                stack_a_id, stack_b_id,
                kd_advantage, entry_advantage, clutch_advantage,
                support_advantage, hs_advantage, win_rate_advantage,
                predicted_winner, confidence,
                recommendations, key_battlegrounds, role_matchups
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            matchup['stack_a_id'],
            matchup['stack_b_id'],
            json.dumps(matchup.get('kd_advantage')),
            json.dumps(matchup.get('entry_advantage')),
            json.dumps(matchup.get('clutch_advantage')),
            json.dumps(matchup.get('support_advantage')),
            json.dumps(matchup.get('hs_advantage')),
            json.dumps(matchup.get('win_rate_advantage')),
            matchup.get('predicted_winner'),
            matchup.get('confidence'),
            json.dumps(matchup.get('recommendations')),
            json.dumps(matchup.get('key_battlegrounds')),
            json.dumps(matchup.get('role_matchups'))
        ))
        self.conn.commit()
        return cursor.lastrowid

    # --- Scraper data persistence ---

    @staticmethod
    def _summary_stat_value(raw: Any) -> Any:
        """Return tracker stat value, supporting either scalar or {'value': ...} objects."""
        if isinstance(raw, dict):
            return raw.get("value")
        return raw

    @classmethod
    def _summary_stat_int(cls, raw: Any) -> int:
        value = cls._summary_stat_value(raw)
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _summary_stat_float(cls, raw: Any) -> float:
        value = cls._summary_stat_value(raw)
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _round_payload_has_rounds(value: Any) -> bool:
        if not isinstance(value, dict) or not value:
            return False
        data = value.get("data", {}) if isinstance(value.get("data"), dict) else {}
        rounds = value.get("rounds") or data.get("rounds")
        return isinstance(rounds, list) and len(rounds) > 0

    def _parse_rounds_from_summary(self, summary_json: Any) -> Dict[str, Any]:
        """
        Build an ow-ingest-like round payload directly from summary segments.

        Returns a dict with keys:
        - players: [{id, nickname}]
        - killfeed: [{roundId, attackerId, victimId, attackerOperatorName, victimOperatorName}]
        - rounds: [{id, winner, roundOutcome, players, killEvents}]
        """
        payload = summary_json
        if isinstance(summary_json, str):
            try:
                payload = json.loads(summary_json)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}

        data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        segments = data.get("segments", []) if isinstance(data.get("segments"), list) else []

        usernames_by_tracker_id: Dict[str, str] = {}
        operator_by_round_and_player: Dict[int, Dict[str, str]] = {}
        round_meta_by_id: Dict[int, Dict[str, Any]] = {}
        player_rows_by_round: Dict[int, List[Dict[str, Any]]] = {}

        for seg in segments:
            if not isinstance(seg, dict):
                continue
            seg_type = seg.get("type")
            attrs = seg.get("attributes", {}) if isinstance(seg.get("attributes"), dict) else {}
            meta = seg.get("metadata", {}) if isinstance(seg.get("metadata"), dict) else {}

            if seg_type == "overview":
                tracker_id = str(attrs.get("playerId") or meta.get("platformUserId") or "").strip()
                username = str(meta.get("platformUserHandle") or meta.get("displayName") or "").strip()
                if tracker_id and username:
                    usernames_by_tracker_id[tracker_id] = username
                continue

            round_id_raw = attrs.get("roundId")
            try:
                round_id = int(round_id_raw)
            except (TypeError, ValueError):
                continue

            if seg_type == "round-overview":
                killfeed = meta.get("killfeed", [])
                round_meta_by_id[round_id] = {
                    "winner_side": str(attrs.get("winnerSideId") or "").strip(),
                    "end_reason": str(attrs.get("roundEndReasonId") or "").strip(),
                    "killfeed": killfeed if isinstance(killfeed, list) else [],
                }
                continue

            if seg_type == "player-round":
                tracker_id = str(attrs.get("playerId") or "").strip()
                username = str(meta.get("platformUserHandle") or "").strip()
                if tracker_id and username:
                    usernames_by_tracker_id[tracker_id] = username
                operator_name = str(meta.get("operatorName") or attrs.get("operatorId") or "").strip()
                if tracker_id and operator_name:
                    operator_by_round_and_player.setdefault(round_id, {})[tracker_id] = operator_name
                player_rows_by_round.setdefault(round_id, []).append(
                    {
                        "id": tracker_id,
                        "nickname": usernames_by_tracker_id.get(tracker_id, tracker_id),
                        "teamId": attrs.get("teamId"),
                        "sideId": attrs.get("sideId"),
                        "operatorName": operator_name,
                        "resultId": attrs.get("resultId"),
                    }
                )

        players = [{"id": pid, "nickname": name} for pid, name in usernames_by_tracker_id.items()]
        killfeed_rows: List[Dict[str, Any]] = []
        rounds: List[Dict[str, Any]] = []

        round_ids = sorted(set(round_meta_by_id.keys()) | set(player_rows_by_round.keys()))
        for round_id in round_ids:
            overview = round_meta_by_id.get(round_id, {})
            per_round_ops = operator_by_round_and_player.get(round_id, {})
            per_round_events: List[Dict[str, Any]] = []

            for ev in overview.get("killfeed", []):
                if not isinstance(ev, dict):
                    continue
                attacker_id = str(ev.get("killerId") or ev.get("attackerId") or "").strip()
                victim_id = str(ev.get("victimId") or "").strip()
                attacker_op = per_round_ops.get(attacker_id, "")
                victim_op = per_round_ops.get(victim_id, "")
                killfeed_rows.append(
                    {
                        "roundId": round_id,
                        "attackerId": attacker_id,
                        "victimId": victim_id,
                        "attackerOperatorName": attacker_op,
                        "victimOperatorName": victim_op,
                    }
                )
                per_round_events.append(
                    {
                        "timestamp": ev.get("timestamp"),
                        "killerId": attacker_id,
                        "victimId": victim_id,
                        "killerName": usernames_by_tracker_id.get(attacker_id, attacker_id or "Unknown"),
                        "victimName": usernames_by_tracker_id.get(victim_id, victim_id or "Unknown"),
                        "killerOperator": attacker_op,
                        "victimOperator": victim_op,
                    }
                )

            rounds.append(
                {
                    "id": round_id,
                    "winner": overview.get("winner_side", ""),
                    "roundOutcome": overview.get("end_reason", ""),
                    "players": player_rows_by_round.get(round_id, []),
                    "killEvents": per_round_events,
                }
            )

        return {"players": players, "killfeed": killfeed_rows, "rounds": rounds}

    def _unpack_summary_segments(self, match_id: str, summary_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a scraped summary payload into normalized row payloads for:
        - match_detail_players
        - round_outcomes
        - player_rounds
        """
        data = summary_payload.get("data", {}) if isinstance(summary_payload, dict) else {}
        segments = data.get("segments", []) if isinstance(data, dict) else []
        if not isinstance(segments, list):
            segments = []

        # Index killfeed per round to derive first blood / first death in player-round rows.
        killfeed_by_round: Dict[int, List[Dict[str, Any]]] = {}
        for seg in segments:
            if not isinstance(seg, dict) or seg.get("type") != "round-overview":
                continue
            attrs = seg.get("attributes", {}) if isinstance(seg.get("attributes"), dict) else {}
            meta = seg.get("metadata", {}) if isinstance(seg.get("metadata"), dict) else {}
            round_id = attrs.get("roundId")
            try:
                round_id_int = int(round_id)
            except (TypeError, ValueError):
                continue
            killfeed_raw = meta.get("killfeed", [])
            killfeed_by_round[round_id_int] = killfeed_raw if isinstance(killfeed_raw, list) else []

        detail_rows: List[Dict[str, Any]] = []
        round_rows: List[Dict[str, Any]] = []
        player_round_rows: List[Dict[str, Any]] = []
        usernames_by_tracker_id: Dict[str, str] = {}

        for seg in segments:
            if not isinstance(seg, dict):
                continue
            seg_type = seg.get("type")
            attrs = seg.get("attributes", {}) if isinstance(seg.get("attributes"), dict) else {}
            meta = seg.get("metadata", {}) if isinstance(seg.get("metadata"), dict) else {}
            stats = seg.get("stats", {}) if isinstance(seg.get("stats"), dict) else {}

            if seg_type == "overview":
                tracker_id = attrs.get("playerId") or meta.get("platformUserId") or ""
                tracker_id = str(tracker_id).strip() if tracker_id is not None else ""
                username = str(meta.get("platformUserHandle") or meta.get("displayName") or "").strip()
                if tracker_id and username:
                    usernames_by_tracker_id[tracker_id] = username

                detail_rows.append(
                    {
                        "player_id_tracker": tracker_id,
                        "username": username,
                        "team_id": attrs.get("teamId", -1),
                        "result": meta.get("result", ""),
                        "kills": self._summary_stat_int(stats.get("kills")),
                        "deaths": self._summary_stat_int(stats.get("deaths")),
                        "assists": self._summary_stat_int(stats.get("assists")),
                        "headshots": self._summary_stat_int(stats.get("headshots")),
                        "first_bloods": self._summary_stat_int(stats.get("firstBloods")),
                        "first_deaths": self._summary_stat_int(stats.get("firstDeaths")),
                        "clutches_won": self._summary_stat_int(stats.get("clutches")),
                        "clutches_lost": self._summary_stat_int(stats.get("clutchesLost")),
                        "clutches_1v1": self._summary_stat_int(stats.get("clutches1v1")),
                        "clutches_1v2": self._summary_stat_int(stats.get("clutches1v2")),
                        "clutches_1v3": self._summary_stat_int(stats.get("clutches1v3")),
                        "clutches_1v4": self._summary_stat_int(stats.get("clutches1v4")),
                        "clutches_1v5": self._summary_stat_int(stats.get("clutches1v5")),
                        "kills_1k": self._summary_stat_int(stats.get("kills1K")),
                        "kills_2k": self._summary_stat_int(stats.get("kills2K")),
                        "kills_3k": self._summary_stat_int(stats.get("kills3K")),
                        "kills_4k": self._summary_stat_int(stats.get("kills4K")),
                        "kills_5k": self._summary_stat_int(stats.get("kills5K")),
                        "rounds_won": self._summary_stat_int(stats.get("roundsWon")),
                        "rounds_lost": self._summary_stat_int(stats.get("roundsLost")),
                        "rank_points": self._summary_stat_int(stats.get("rankPoints")),
                        "rank_points_delta": self._summary_stat_int(stats.get("rankPointsDelta")),
                        "rank_points_previous": self._summary_stat_int(stats.get("rankPointsPrevious")),
                        "kd_ratio": self._summary_stat_float(stats.get("kdRatio")),
                        "hs_pct": self._summary_stat_float(stats.get("headshotPct")),
                        "esr": self._summary_stat_float(stats.get("esr")),
                        "kills_per_round": self._summary_stat_float(stats.get("killsPerRound")),
                        "time_played_ms": self._summary_stat_int(stats.get("timePlayed")),
                        "elo": self._summary_stat_int(stats.get("elo")),
                        "elo_delta": self._summary_stat_int(stats.get("eloDelta")),
                    }
                )
            elif seg_type == "round-overview":
                round_rows.append(
                    {
                        "round_id": attrs.get("roundId"),
                        "end_reason": attrs.get("roundEndReasonId", ""),
                        "winner_side": attrs.get("winnerSideId", ""),
                    }
                )
            elif seg_type == "player-round":
                round_id = attrs.get("roundId")
                tracker_id = attrs.get("playerId") or ""
                tracker_id = str(tracker_id).strip() if tracker_id is not None else ""
                if tracker_id and meta.get("platformUserHandle"):
                    usernames_by_tracker_id[tracker_id] = str(meta.get("platformUserHandle")).strip()

                first_blood = self._summary_stat_int(stats.get("firstBloods"))
                first_death = self._summary_stat_int(stats.get("firstDeaths"))
                try:
                    round_id_int = int(round_id)
                except (TypeError, ValueError):
                    round_id_int = None
                if round_id_int is not None and first_blood == 0 and first_death == 0:
                    killfeed = killfeed_by_round.get(round_id_int, [])
                    if killfeed:
                        first = killfeed[0] if isinstance(killfeed[0], dict) else {}
                        killer_id = str(first.get("killerId", "")).strip()
                        victim_id = str(first.get("victimId", "")).strip()
                        if killer_id and killer_id == tracker_id:
                            first_blood = 1
                        if victim_id and victim_id == tracker_id:
                            first_death = 1

                player_round_rows.append(
                    {
                        "round_id": round_id,
                        "player_id_tracker": tracker_id,
                        "team_id": attrs.get("teamId", -1),
                        "side": attrs.get("sideId", ""),
                        "operator": meta.get("operatorName", "") or attrs.get("operatorId", ""),
                        "result": attrs.get("resultId", ""),
                        "is_disconnected": 1 if attrs.get("isDisconnected", False) else 0,
                        "kills": self._summary_stat_int(stats.get("kills")),
                        "deaths": self._summary_stat_int(stats.get("deaths")),
                        "assists": self._summary_stat_int(stats.get("assists")),
                        "headshots": self._summary_stat_int(stats.get("headshots")),
                        "first_blood": first_blood,
                        "first_death": first_death,
                        "clutch_won": self._summary_stat_int(stats.get("clutches")),
                        "clutch_lost": self._summary_stat_int(stats.get("clutchesLost")),
                        "hs_pct": self._summary_stat_float(stats.get("headshotPct")),
                        "esr": self._summary_stat_float(stats.get("esr")),
                    }
                )

        return {
            "detail_rows": detail_rows,
            "round_rows": round_rows,
            "player_round_rows": player_round_rows,
            "usernames_by_tracker_id": usernames_by_tracker_id,
        }

    def unpack_pending_scraped_match_cards(
        self,
        username: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Unpack scraped match cards with summary_json that have not yet been normalized.

        A card is considered unpacked when all three normalized match tables have at least
        one row for (player_id, match_id).
        """
        cursor = self.conn.cursor()
        params: List[Any] = []
        query = """
            SELECT id, username, match_id, mode, summary_json, round_data_json, round_data_source
            FROM scraped_match_cards
            WHERE summary_json IS NOT NULL
              AND TRIM(summary_json) != ''
              AND TRIM(summary_json) != '{}'
              AND LOWER(TRIM(summary_json)) != 'null'
              AND match_id IS NOT NULL
              AND TRIM(match_id) != ''
        """
        if username:
            query += " AND username = ?"
            params.append(username)
        query += " ORDER BY id DESC"
        if isinstance(limit, int) and limit > 0:
            query += " LIMIT ?"
            params.append(limit)

        cursor.execute(query, tuple(params))
        cards = cursor.fetchall()

        stats = {
            "scanned": len(cards),
            "unpacked_matches": 0,
            "inserted_detail_rows": 0,
            "inserted_round_rows": 0,
            "inserted_player_round_rows": 0,
            "skipped": 0,
            "errors": 0,
        }

        for row in cards:
            owner_username = str(row["username"] or "").strip()
            match_id = str(row["match_id"] or "").strip()
            match_type = str(row["mode"] or "").strip()
            summary_raw = row["summary_json"]
            if not owner_username or not match_id or not summary_raw:
                stats["skipped"] += 1
                continue

            try:
                owner_player_id = self.get_player_id(owner_username)
                if owner_player_id is None:
                    owner_player_id = self.add_player(owner_username)

                cursor.execute(
                    "SELECT 1 FROM match_detail_players WHERE player_id = ? AND match_id = ? LIMIT 1",
                    (owner_player_id, match_id),
                )
                has_detail = cursor.fetchone() is not None
                cursor.execute(
                    "SELECT 1 FROM round_outcomes WHERE player_id = ? AND match_id = ? LIMIT 1",
                    (owner_player_id, match_id),
                )
                has_round_outcomes = cursor.fetchone() is not None
                cursor.execute(
                    "SELECT 1 FROM player_rounds WHERE player_id = ? AND match_id = ? LIMIT 1",
                    (owner_player_id, match_id),
                )
                has_player_rounds = cursor.fetchone() is not None
                if has_detail and has_round_outcomes and has_player_rounds:
                    stats["skipped"] += 1
                    continue

                summary_payload = json.loads(summary_raw)
                round_data_payload = {}
                try:
                    round_data_payload = json.loads(row["round_data_json"] or "{}")
                except Exception:
                    round_data_payload = {}
                parsed_summary_round_payload = self._parse_rounds_from_summary(summary_payload)
                round_data_source = None
                if self._round_payload_has_rounds(parsed_summary_round_payload):
                    round_data_source = "summary"
                elif self._round_payload_has_rounds(round_data_payload):
                    round_data_source = "ow-ingest"
                else:
                    existing_source = str(row["round_data_source"] or "").strip()
                    if existing_source:
                        round_data_source = existing_source

                unpacked = self._unpack_summary_segments(match_id, summary_payload)
                detail_rows = unpacked["detail_rows"]
                round_rows = unpacked["round_rows"]
                player_round_rows = unpacked["player_round_rows"]
                usernames_by_tracker_id = unpacked["usernames_by_tracker_id"]

                if not detail_rows and not round_rows and not player_round_rows:
                    stats["skipped"] += 1
                    continue

                # Replace per-match normalized rows for this owner to keep data consistent.
                self.save_match_detail_players(owner_player_id, match_id, detail_rows, match_type=match_type)
                self.save_round_outcomes(owner_player_id, match_id, round_rows, match_type=match_type)
                self.save_player_rounds(
                    owner_player_id,
                    match_id,
                    player_round_rows,
                    match_type=match_type,
                    usernames_by_tracker_id=usernames_by_tracker_id,
                )
                update_round_data_json = row["round_data_json"]
                if round_data_source == "summary":
                    update_round_data_json = json.dumps(parsed_summary_round_payload)
                cursor.execute(
                    "UPDATE scraped_match_cards SET round_data_source = ?, round_data_json = ? WHERE id = ?",
                    (round_data_source, update_round_data_json, row["id"]),
                )
                self.conn.commit()

                stats["unpacked_matches"] += 1
                stats["inserted_detail_rows"] += len(detail_rows)
                stats["inserted_round_rows"] += len(round_rows)
                stats["inserted_player_round_rows"] += len(player_round_rows)
            except Exception:
                stats["errors"] += 1

        return stats

    def save_scraped_match_cards(self, username: str, matches: List[Dict]) -> None:
        """Persist scraped match cards for one username without wiping prior rows."""
        cursor = self.conn.cursor()

        def _safe_json_load(raw: Any, fallback: Any) -> Any:
            try:
                if raw is None:
                    return fallback
                return json.loads(raw)
            except Exception:
                return fallback

        def _has_round_list(value: Any) -> bool:
            return isinstance(value, list) and len(value) > 0

        def _has_round_payload(value: Any) -> bool:
            if not isinstance(value, dict) or not value:
                return False
            rounds = value.get("rounds") or value.get("data", {}).get("rounds")
            return isinstance(rounds, list) and len(rounds) > 0

        for item in matches:
            match_id = (item.get("match_id") or "").strip()
            if match_id:
                cursor.execute(
                    """
                    SELECT id, map_name, mode, score_team_a, score_team_b, duration, match_date,
                           players_json, rounds_json, summary_json, round_data_json, round_data_source
                    FROM scraped_match_cards
                    WHERE username = ? AND match_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (username, match_id),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    existing_players = _safe_json_load(existing["players_json"], [])
                    existing_rounds = _safe_json_load(existing["rounds_json"], [])
                    existing_summary = _safe_json_load(existing["summary_json"], {})
                    existing_round_data = _safe_json_load(existing["round_data_json"], {})
                    existing_round_data_source = str(existing["round_data_source"] or "").strip()

                    new_players = item.get("players", [])
                    new_rounds = item.get("rounds", [])
                    new_summary = item.get("match_summary", {})
                    new_round_data = item.get("round_data", {})

                    updated = False

                    players_json = json.dumps(existing_players)
                    rounds_json = json.dumps(existing_rounds)
                    summary_json = json.dumps(existing_summary)
                    round_data_json = json.dumps(existing_round_data)
                    round_data_source = existing_round_data_source or None

                    if (not isinstance(existing_players, list) or not existing_players) and isinstance(new_players, list) and new_players:
                        players_json = json.dumps(new_players)
                        updated = True
                    if (not _has_round_list(existing_rounds)) and _has_round_list(new_rounds):
                        rounds_json = json.dumps(new_rounds)
                        updated = True
                    if (not isinstance(existing_summary, dict) or not existing_summary) and isinstance(new_summary, dict) and new_summary:
                        summary_json = json.dumps(new_summary)
                        updated = True
                    if (not _has_round_payload(existing_round_data)) and _has_round_payload(new_round_data):
                        round_data_json = json.dumps(new_round_data)
                        round_data_source = "ow-ingest"
                        updated = True

                    map_name = existing["map_name"]
                    if (not str(map_name or "").strip()) and str(item.get("map") or "").strip():
                        map_name = item.get("map")
                        updated = True
                    mode = existing["mode"]
                    if (not str(mode or "").strip()) and str(item.get("mode") or "").strip():
                        mode = item.get("mode")
                        updated = True
                    match_date = existing["match_date"]
                    if (not str(match_date or "").strip()) and str(item.get("date") or "").strip():
                        match_date = item.get("date")
                        updated = True
                    duration = existing["duration"]
                    if (not str(duration or "").strip()) and str(item.get("duration") or "").strip():
                        duration = item.get("duration")
                        updated = True

                    score_team_a = existing["score_team_a"]
                    score_team_b = existing["score_team_b"]
                    new_a = item.get("score_team_a")
                    new_b = item.get("score_team_b")
                    if (not score_team_a and not score_team_b) and (new_a or new_b):
                        score_team_a = new_a
                        score_team_b = new_b
                        updated = True

                    if updated:
                        cursor.execute(
                            """
                            UPDATE scraped_match_cards
                            SET map_name = ?, mode = ?, score_team_a = ?, score_team_b = ?,
                                duration = ?, match_date = ?, players_json = ?, rounds_json = ?,
                                summary_json = ?, round_data_json = ?, round_data_source = ?
                            WHERE id = ?
                            """,
                            (
                                map_name,
                                mode,
                                score_team_a,
                                score_team_b,
                                duration,
                                match_date,
                                players_json,
                                rounds_json,
                                summary_json,
                                round_data_json,
                                round_data_source,
                                existing["id"],
                            ),
                        )
                    continue
            cursor.execute("""
                INSERT INTO scraped_match_cards (
                    username, match_id, map_name, mode, score_team_a, score_team_b,
                    duration, match_date, players_json, rounds_json, summary_json, round_data_json, round_data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                username,
                item.get("match_id"),
                item.get("map"),
                item.get("mode"),
                item.get("score_team_a"),
                item.get("score_team_b"),
                item.get("duration"),
                item.get("date"),
                json.dumps(item.get("players", [])),
                json.dumps(item.get("rounds", [])),
                json.dumps(item.get("match_summary", {})),
                json.dumps(item.get("round_data", {})),
                "ow-ingest" if _has_round_payload(item.get("round_data", {})) else None,
            ))

        self.conn.commit()
        # Automatically normalize any new/legacy cards that still need unpacking.
        self.unpack_pending_scraped_match_cards(username=username)

    @staticmethod
    def _normalize_team_label(raw_team: Any) -> str:
        """Convert assorted team identifiers to canonical 'A'/'B' labels."""
        if raw_team is None:
            return ""
        text = str(raw_team).strip().lower()
        if not text:
            return ""
        if text in {"a", "team_a", "teama", "blue", "0"}:
            return "A"
        if text in {"b", "team_b", "teamb", "orange", "1"}:
            return "B"
        if "blue" in text or text.startswith("team a"):
            return "A"
        if "orange" in text or text.startswith("team b"):
            return "B"
        return ""

    def _build_players_from_detail_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map normalized match_detail_players rows back into the scraped players shape."""
        out: List[Dict[str, Any]] = []
        for row in rows:
            username = str(row.get("username") or "").strip()
            if not username:
                continue
            out.append(
                {
                    "team": self._normalize_team_label(row.get("team_id")),
                    "username": username,
                    "rank_points": row.get("rank_points") or 0,
                    "kd": row.get("kd_ratio") or 0.0,
                    "kills": row.get("kills") or 0,
                    "deaths": row.get("deaths") or 0,
                    "assists": row.get("assists") or 0,
                    "hs_percent": row.get("hs_pct") or 0.0,
                    "operators": [],
                }
            )
        return out

    def get_scraped_match_cards(self, username: str, limit: int = 50) -> List[Dict]:
        """Fetch persisted scraped match cards for one username."""
        # Keep normalized tables current whenever stored cards are accessed.
        self.unpack_pending_scraped_match_cards(username=username)
        owner_player_id = self.get_player_id(username)
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM scraped_match_cards
            WHERE username = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (username, limit),
        )

        out = []
        repaired_players_json = False
        for row in cursor.fetchall():
            item = dict(row)
            try:
                players = json.loads(item.get("players_json") or "[]")
            except json.JSONDecodeError:
                players = []
            if not isinstance(players, list):
                players = []
            try:
                rounds = json.loads(item.get("rounds_json") or "[]")
            except json.JSONDecodeError:
                rounds = []
            try:
                summary = json.loads(item.get("summary_json") or "{}")
            except json.JSONDecodeError:
                summary = {}
            try:
                round_data = json.loads(item.get("round_data_json") or "{}")
            except json.JSONDecodeError:
                round_data = {}
            has_round_list = isinstance(rounds, list) and len(rounds) > 0
            has_round_payload = self._round_payload_has_rounds(round_data)

            # Repair stale scraped players payloads from normalized rows so UI insights stay current.
            has_named_players = any(
                isinstance(p, dict) and str(p.get("username") or p.get("name") or "").strip()
                for p in players
            )
            has_team_labels = any(
                isinstance(p, dict) and str(p.get("team") or "").strip().upper() in {"A", "B"}
                for p in players
            )
            needs_player_rebuild = (not has_named_players) or (has_named_players and not has_team_labels)
            if needs_player_rebuild and owner_player_id and item.get("match_id"):
                detail_rows = self.get_match_detail_players(owner_player_id, str(item.get("match_id")))
                rebuilt_players = self._build_players_from_detail_rows(detail_rows)
                if rebuilt_players:
                    players = rebuilt_players
                    cursor.execute(
                        "UPDATE scraped_match_cards SET players_json = ? WHERE id = ?",
                        (json.dumps(players), item.get("id")),
                    )
                    repaired_players_json = True

            out.append(
                {
                    "username": item.get("username"),
                    "match_id": item.get("match_id") or "",
                    "map": item.get("map_name") or "",
                    "mode": item.get("mode") or "",
                    "score_team_a": item.get("score_team_a") or 0,
                    "score_team_b": item.get("score_team_b") or 0,
                    "duration": item.get("duration") or "",
                    "date": item.get("match_date") or "",
                    "players": players,
                    "rounds": rounds,
                    "match_summary": summary,
                    "round_data": round_data,
                    "round_data_source": item.get("round_data_source"),
                    "round_data_missing": not (has_round_list or has_round_payload),
                    "scraped_at": item.get("scraped_at"),
                }
            )

        if repaired_players_json:
            self.conn.commit()

        return out

    def delete_bad_scraped_matches(self, username: str) -> Dict[str, int]:
        """
        Delete stored match cards missing round-level data for one username.
        Also removes normalized rows for those match IDs for this owner player.
        """
        owner_username = str(username or "").strip()
        if not owner_username:
            return {
                "deleted_cards": 0,
                "deleted_match_ids": 0,
                "deleted_detail_rows": 0,
                "deleted_round_rows": 0,
                "deleted_player_round_rows": 0,
            }

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, match_id, rounds_json, round_data_json
            FROM scraped_match_cards
            WHERE username = ?
            """,
            (owner_username,),
        )
        rows = cursor.fetchall()

        bad_card_ids: List[int] = []
        bad_match_ids: List[str] = []
        for row in rows:
            try:
                rounds = json.loads(row["rounds_json"] or "[]")
            except Exception:
                rounds = []
            try:
                round_data = json.loads(row["round_data_json"] or "{}")
            except Exception:
                round_data = {}
            has_round_list = isinstance(rounds, list) and len(rounds) > 0
            has_round_payload = self._round_payload_has_rounds(round_data)
            if has_round_list or has_round_payload:
                continue
            bad_card_ids.append(int(row["id"]))
            match_id = str(row["match_id"] or "").strip()
            if match_id:
                bad_match_ids.append(match_id)

        if not bad_card_ids:
            return {
                "deleted_cards": 0,
                "deleted_match_ids": 0,
                "deleted_detail_rows": 0,
                "deleted_round_rows": 0,
                "deleted_player_round_rows": 0,
            }

        owner_player_id = self.get_player_id(owner_username)
        unique_match_ids = sorted(set(bad_match_ids))

        deleted_detail_rows = 0
        deleted_round_rows = 0
        deleted_player_round_rows = 0

        if owner_player_id and unique_match_ids:
            placeholders = ",".join(["?"] * len(unique_match_ids))
            params = [owner_player_id, *unique_match_ids]
            cursor.execute(
                f"DELETE FROM match_detail_players WHERE player_id = ? AND match_id IN ({placeholders})",
                tuple(params),
            )
            deleted_detail_rows = cursor.rowcount if cursor.rowcount >= 0 else 0
            cursor.execute(
                f"DELETE FROM round_outcomes WHERE player_id = ? AND match_id IN ({placeholders})",
                tuple(params),
            )
            deleted_round_rows = cursor.rowcount if cursor.rowcount >= 0 else 0
            cursor.execute(
                f"DELETE FROM player_rounds WHERE player_id = ? AND match_id IN ({placeholders})",
                tuple(params),
            )
            deleted_player_round_rows = cursor.rowcount if cursor.rowcount >= 0 else 0

        placeholders = ",".join(["?"] * len(bad_card_ids))
        cursor.execute(
            f"DELETE FROM scraped_match_cards WHERE id IN ({placeholders})",
            tuple(bad_card_ids),
        )
        deleted_cards = cursor.rowcount if cursor.rowcount >= 0 else len(bad_card_ids)
        self.conn.commit()

        return {
            "deleted_cards": int(deleted_cards),
            "deleted_match_ids": len(unique_match_ids),
            "deleted_detail_rows": int(deleted_detail_rows),
            "deleted_round_rows": int(deleted_round_rows),
            "deleted_player_round_rows": int(deleted_player_round_rows),
        }

    def get_existing_scraped_match_ids(self, username: str) -> set[str]:
        """Return distinct non-empty scraped match IDs for a username."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT match_id
            FROM scraped_match_cards
            WHERE username = ?
              AND match_id IS NOT NULL
              AND TRIM(match_id) != ''
            """,
            (username,),
        )
        return {str(row["match_id"]).strip() for row in cursor.fetchall() if row["match_id"]}

    def get_fully_scraped_match_ids(self, username: str) -> set[str]:
        """Return match IDs with summary data and either ow-ingest or summary-derived round data."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT match_id
            FROM scraped_match_cards
            WHERE username = ?
              AND match_id IS NOT NULL
              AND TRIM(match_id) != ''
              AND summary_json IS NOT NULL
              AND TRIM(summary_json) NOT IN ('', '{}', 'null')
              AND (
                    (round_data_json IS NOT NULL AND TRIM(round_data_json) NOT IN ('', '{}', 'null'))
                    OR LOWER(TRIM(COALESCE(round_data_source, ''))) = 'summary'
                  )
            """,
            (username,),
        )
        return {str(row["match_id"]).strip() for row in cursor.fetchall() if row["match_id"]}

    @staticmethod
    def _normalize_match_mode_key(raw_mode: Any) -> str:
        text = str(raw_mode or "").strip().lower()
        if "unranked" in text:
            return "unranked"
        if "ranked" in text:
            return "ranked"
        if "quick" in text:
            return "quick"
        if "standard" in text:
            return "standard"
        if "event" in text:
            return "event"
        return "other"

    def count_fully_scraped_match_ids(self, username: str, allowed_mode_keys: Optional[set[str]] = None) -> int:
        """
        Count distinct fully-scraped match IDs for a user.
        If allowed_mode_keys is provided, only count rows whose mode normalizes into that set.
        """
        allowed = {str(m or "").strip().lower() for m in (allowed_mode_keys or set()) if str(m or "").strip()}
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT match_id, mode
            FROM scraped_match_cards
            WHERE username = ?
              AND match_id IS NOT NULL
              AND TRIM(match_id) != ''
              AND summary_json IS NOT NULL
              AND TRIM(summary_json) NOT IN ('', '{}', 'null')
              AND (
                    (round_data_json IS NOT NULL AND TRIM(round_data_json) NOT IN ('', '{}', 'null'))
                    OR LOWER(TRIM(COALESCE(round_data_source, ''))) = 'summary'
                  )
            """,
            (username,),
        )
        rows = cursor.fetchall()
        if not allowed:
            return len(rows)
        count = 0
        for row in rows:
            mode_key = self._normalize_match_mode_key(row["mode"])
            if mode_key in allowed:
                count += 1
        return count

    def get_scrape_checkpoint_skip_count(self, username: str, mode_key: str, filter_key: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT skip_count
            FROM scrape_checkpoints
            WHERE username = ? AND mode_key = ? AND filter_key = ?
            LIMIT 1
            """,
            (str(username or "").strip(), str(mode_key or "").strip(), str(filter_key or "").strip()),
        )
        row = cursor.fetchone()
        if not row:
            return 0
        try:
            return max(0, int(row["skip_count"]))
        except Exception:
            return 0

    def set_scrape_checkpoint_skip_count(
        self,
        username: str,
        mode_key: str,
        filter_key: str,
        skip_count: int,
    ) -> None:
        safe_count = max(0, int(skip_count or 0))
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO scrape_checkpoints (username, mode_key, filter_key, skip_count, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username, mode_key, filter_key)
            DO UPDATE SET
                skip_count = excluded.skip_count,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                str(username or "").strip(),
                str(mode_key or "").strip(),
                str(filter_key or "").strip(),
                safe_count,
            ),
        )
        self.conn.commit()

    def save_map_stats(self, player_id: int, maps: List[Dict], snapshot_id: int = None, season: str = 'Y10S4') -> None:
        """Persist scraped map stats for a player and season."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM map_stats WHERE player_id = ? AND season = ?",
            (player_id, season),
        )
        for item in maps:
            cursor.execute("""
                INSERT INTO map_stats (
                    player_id, snapshot_id, season, map_name, matches, win_pct, wins, losses,
                    kd, atk_win_pct, def_win_pct, hs_pct, esr
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                snapshot_id,
                season,
                item.get('map_name'),
                item.get('matches'),
                item.get('win_pct'),
                item.get('wins'),
                item.get('losses'),
                item.get('kd'),
                item.get('atk_win_pct'),
                item.get('def_win_pct'),
                item.get('hs_pct'),
                item.get('esr'),
            ))
        self.conn.commit()

    def get_map_stats(self, player_id: int, season: str = None) -> List[Dict]:
        """Fetch map stats for a player (optionally by season)."""
        cursor = self.conn.cursor()
        if season:
            cursor.execute(
                "SELECT * FROM map_stats WHERE player_id = ? AND season = ? ORDER BY map_name",
                (player_id, season),
            )
        else:
            cursor.execute(
                "SELECT * FROM map_stats WHERE player_id = ? ORDER BY scraped_at DESC, map_name",
                (player_id,),
            )
        return [dict(row) for row in cursor.fetchall()]

    def save_operator_stats(self, player_id: int, operators: List[Dict], snapshot_id: int = None, season: str = 'Y10S4') -> None:
        """Persist scraped operator stats for a player and season."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM operator_stats WHERE player_id = ? AND season = ?",
            (player_id, season),
        )
        for item in operators:
            cursor.execute("""
                INSERT INTO operator_stats (
                    player_id, snapshot_id, season, operator_name, rounds, win_pct, kd, hs_pct,
                    kills, deaths, wins, losses, assists, aces, teamkills
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                snapshot_id,
                season,
                item.get('operator_name'),
                item.get('rounds'),
                item.get('win_pct'),
                item.get('kd'),
                item.get('hs_pct'),
                item.get('kills'),
                item.get('deaths'),
                item.get('wins'),
                item.get('losses'),
                item.get('assists'),
                item.get('aces'),
                item.get('teamkills'),
            ))
        self.conn.commit()

    def get_operator_stats(self, player_id: int, season: str = None) -> List[Dict]:
        """Fetch operator stats for a player (optionally by season)."""
        cursor = self.conn.cursor()
        if season:
            cursor.execute(
                "SELECT * FROM operator_stats WHERE player_id = ? AND season = ? ORDER BY rounds DESC, operator_name",
                (player_id, season),
            )
        else:
            cursor.execute(
                "SELECT * FROM operator_stats WHERE player_id = ? ORDER BY scraped_at DESC, rounds DESC, operator_name",
                (player_id,),
            )
        return [dict(row) for row in cursor.fetchall()]

    def save_match_history(self, player_id: int, matches: List[Dict]) -> None:
        """Persist scraped match history and replace previous history snapshot."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM match_history WHERE player_id = ?", (player_id,))
        for item in matches:
            cursor.execute("""
                INSERT INTO match_history (
                    player_id, time_ago, map_name, mode, score, result, rp, rp_change, kd, kda,
                    hs_pct, had_ace, had_4k, had_3k, had_2k
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                item.get('time_ago'),
                item.get('map_name'),
                item.get('mode'),
                item.get('score'),
                item.get('result'),
                item.get('rp'),
                item.get('rp_change'),
                item.get('kd'),
                item.get('kda'),
                item.get('hs_pct'),
                1 if item.get('had_ace') else 0,
                1 if item.get('had_4k') else 0,
                1 if item.get('had_3k') else 0,
                1 if item.get('had_2k') else 0,
            ))
        self.conn.commit()

    def get_match_history(self, player_id: int, limit: int = 40) -> List[Dict]:
        """Fetch recent match history for a player."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM match_history WHERE player_id = ? ORDER BY match_id DESC LIMIT ?",
            (player_id, limit),
        )
        return [dict(row) for row in cursor.fetchall()]

    def save_match_players(self, match_id: int, team_a: List[Dict], team_b: List[Dict]) -> None:
        """Persist match-detail leaderboard players for both teams."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM match_players WHERE match_id = ?", (match_id,))

        def _insert_team(team_label: str, players: List[Dict]) -> None:
            for p in players:
                cursor.execute("""
                    INSERT INTO match_players (
                        match_id, team, username, rp, rp_change, kd, kills, deaths, assists,
                        hs_pct, first_kills, first_deaths, clutches, operators
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    match_id,
                    team_label,
                    p.get('username'),
                    p.get('rp'),
                    p.get('rp_change'),
                    p.get('kd'),
                    p.get('kills'),
                    p.get('deaths'),
                    p.get('assists'),
                    p.get('hs_pct'),
                    p.get('first_kills'),
                    p.get('first_deaths'),
                    p.get('clutches'),
                    json.dumps(p.get('operators', [])),
                ))

        _insert_team('A', team_a)
        _insert_team('B', team_b)
        self.conn.commit()

    def get_match_players(self, match_id: int) -> Dict[str, List[Dict]]:
        """Fetch match-detail leaderboard grouped by team."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM match_players WHERE match_id = ? ORDER BY match_player_id",
            (match_id,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        out = {'team_a': [], 'team_b': []}
        for row in rows:
            operators = row.get('operators')
            try:
                row['operators'] = json.loads(operators) if operators else []
            except json.JSONDecodeError:
                row['operators'] = []

            if row.get('team') == 'A':
                out['team_a'].append(row)
            else:
                out['team_b'].append(row)
        return out

    def save_match_detail_players(
        self,
        player_id: int,
        match_id: str,
        players: List[Dict],
        match_type: Optional[str] = None,
    ) -> None:
        """Persist parsed API player overviews for one match."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM match_detail_players WHERE player_id = ? AND match_id = ?",
            (player_id, match_id),
        )

        for p in players:
            cursor.execute("""
                INSERT INTO match_detail_players (
                    player_id, match_id, match_type, player_id_tracker, username, team_id, result,
                    kills, deaths, assists, headshots, first_bloods, first_deaths,
                    clutches_won, clutches_lost, clutches_1v1, clutches_1v2, clutches_1v3,
                    clutches_1v4, clutches_1v5, kills_1k, kills_2k, kills_3k, kills_4k,
                    kills_5k, rounds_won, rounds_lost, rank_points, rank_points_delta,
                    rank_points_previous, kd_ratio, hs_pct, esr, kills_per_round,
                    time_played_ms, elo, elo_delta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                match_id,
                match_type,
                p.get("player_id_tracker"),
                p.get("username"),
                p.get("team_id"),
                p.get("result"),
                p.get("kills"),
                p.get("deaths"),
                p.get("assists"),
                p.get("headshots"),
                p.get("first_bloods"),
                p.get("first_deaths"),
                p.get("clutches_won"),
                p.get("clutches_lost"),
                p.get("clutches_1v1"),
                p.get("clutches_1v2"),
                p.get("clutches_1v3"),
                p.get("clutches_1v4"),
                p.get("clutches_1v5"),
                p.get("kills_1k"),
                p.get("kills_2k"),
                p.get("kills_3k"),
                p.get("kills_4k"),
                p.get("kills_5k"),
                p.get("rounds_won"),
                p.get("rounds_lost"),
                p.get("rank_points"),
                p.get("rank_points_delta"),
                p.get("rank_points_previous"),
                p.get("kd_ratio"),
                p.get("hs_pct"),
                p.get("esr"),
                p.get("kills_per_round"),
                p.get("time_played_ms"),
                p.get("elo"),
                p.get("elo_delta"),
            ))
        self.conn.commit()

    def get_match_detail_players(self, player_id: int, match_id: Optional[str] = None) -> List[Dict]:
        """Fetch API player overviews for one player, optionally filtered by match_id."""
        cursor = self.conn.cursor()
        if match_id:
            cursor.execute(
                "SELECT * FROM match_detail_players WHERE player_id = ? AND match_id = ? ORDER BY id",
                (player_id, match_id),
            )
        else:
            cursor.execute(
                "SELECT * FROM match_detail_players WHERE player_id = ? ORDER BY scraped_at DESC, id",
                (player_id,),
            )
        return [dict(row) for row in cursor.fetchall()]

    def get_existing_match_detail_ids(self, player_id: int) -> set[str]:
        """Return match IDs already stored in match_detail_players for a player."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT DISTINCT match_id FROM match_detail_players WHERE player_id = ?",
            (player_id,),
        )
        return {row["match_id"] for row in cursor.fetchall() if row["match_id"]}

    def save_round_outcomes(
        self,
        player_id: int,
        match_id: str,
        rounds: List[Dict],
        match_type: Optional[str] = None,
    ) -> None:
        """Persist parsed API round outcomes for one match."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM round_outcomes WHERE player_id = ? AND match_id = ?",
            (player_id, match_id),
        )
        for r in rounds:
            cursor.execute("""
                INSERT INTO round_outcomes (player_id, match_id, match_type, round_id, end_reason, winner_side)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                match_id,
                match_type,
                r.get("round_id"),
                r.get("end_reason"),
                r.get("winner_side"),
            ))
        self.conn.commit()

    def get_round_outcomes(self, player_id: int, match_id: Optional[str] = None) -> List[Dict]:
        """Fetch stored round outcomes."""
        cursor = self.conn.cursor()
        if match_id:
            cursor.execute(
                "SELECT * FROM round_outcomes WHERE player_id = ? AND match_id = ? ORDER BY round_id",
                (player_id, match_id),
            )
        else:
            cursor.execute(
                "SELECT * FROM round_outcomes WHERE player_id = ? ORDER BY scraped_at DESC, id",
                (player_id,),
            )
        return [dict(row) for row in cursor.fetchall()]

    def save_player_rounds(
        self,
        player_id: int,
        match_id: str,
        player_rounds: List[Dict],
        match_type: Optional[str] = None,
        usernames_by_tracker_id: Optional[Dict[str, str]] = None,
    ) -> None:
        """Persist parsed API player-round rows for one match."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM player_rounds WHERE player_id = ? AND match_id = ?",
            (player_id, match_id),
        )
        usernames_by_tracker_id = usernames_by_tracker_id or {}

        for pr in player_rounds:
            tracker_id = pr.get("player_id_tracker")
            cursor.execute("""
                INSERT INTO player_rounds (
                    player_id, match_id, match_type, round_id, player_id_tracker, username, team_id, side,
                    operator, result, is_disconnected, kills, deaths, assists, headshots,
                    first_blood, first_death, clutch_won, clutch_lost, hs_pct, esr
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                match_id,
                match_type,
                pr.get("round_id"),
                tracker_id,
                usernames_by_tracker_id.get(tracker_id),
                pr.get("team_id"),
                pr.get("side"),
                pr.get("operator"),
                pr.get("result"),
                pr.get("is_disconnected", 0),
                pr.get("kills"),
                pr.get("deaths"),
                pr.get("assists"),
                pr.get("headshots"),
                pr.get("first_blood"),
                pr.get("first_death"),
                pr.get("clutch_won"),
                pr.get("clutch_lost"),
                pr.get("hs_pct"),
                pr.get("esr"),
            ))
        self.conn.commit()

    def get_player_rounds(self, player_id: int, match_id: Optional[str] = None) -> List[Dict]:
        """Fetch stored player-round rows."""
        cursor = self.conn.cursor()
        if match_id:
            cursor.execute(
                "SELECT * FROM player_rounds WHERE player_id = ? AND match_id = ? ORDER BY round_id, id",
                (player_id, match_id),
            )
        else:
            cursor.execute(
                "SELECT * FROM player_rounds WHERE player_id = ? ORDER BY scraped_at DESC, id",
                (player_id,),
            )
        return [dict(row) for row in cursor.fetchall()]

    def save_full_match_detail_history(self, player_id: int, details: List[Dict]) -> Dict[str, int]:
        """Persist a batch of API match detail payloads for one player."""
        saved_matches = 0
        saved_round_rows = 0
        for detail in details:
            match_id = detail.get("match_id")
            if not match_id:
                continue
            players = detail.get("players", [])
            rounds = detail.get("round_outcomes", [])
            player_rounds = detail.get("player_rounds", [])
            usernames_by_tracker_id = {
                p.get("player_id_tracker"): p.get("username")
                for p in players
                if p.get("player_id_tracker")
            }
            meta = detail.get("match_meta") if isinstance(detail.get("match_meta"), dict) else {}
            match_type = (
                detail.get("mode")
                or meta.get("mode")
                or meta.get("sessionTypeName")
                or ""
            )

            self.save_match_detail_players(player_id, match_id, players, match_type=match_type)
            self.save_round_outcomes(player_id, match_id, rounds, match_type=match_type)
            self.save_player_rounds(
                player_id,
                match_id,
                player_rounds,
                match_type=match_type,
                usernames_by_tracker_id=usernames_by_tracker_id,
            )
            saved_matches += 1
            saved_round_rows += len(player_rounds)

        return {"matches": saved_matches, "round_rows": saved_round_rows}

    def player_has_map_stats(self, player_id: int) -> bool:
        """Return True if player has any stored map stats."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM map_stats WHERE player_id = ? LIMIT 1", (player_id,))
        return cursor.fetchone() is not None

    def player_has_match_history(self, player_id: int) -> bool:
        """Return True if player has any stored match history."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM match_history WHERE player_id = ? LIMIT 1", (player_id,))
        return cursor.fetchone() is not None

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
