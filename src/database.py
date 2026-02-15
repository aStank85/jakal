# src/database.py

import sqlite3
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

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

        project_root = Path(__file__).resolve().parents[1]
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

            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Access columns by name
            self.conn.execute("PRAGMA foreign_keys = ON")

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

            self.conn.commit()
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
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to migrate database schema: {e}")

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

    def save_match_detail_players(self, player_id: int, match_id: str, players: List[Dict]) -> None:
        """Persist parsed API player overviews for one match."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM match_detail_players WHERE player_id = ? AND match_id = ?",
            (player_id, match_id),
        )

        for p in players:
            cursor.execute("""
                INSERT INTO match_detail_players (
                    player_id, match_id, player_id_tracker, username, team_id, result,
                    kills, deaths, assists, headshots, first_bloods, first_deaths,
                    clutches_won, clutches_lost, clutches_1v1, clutches_1v2, clutches_1v3,
                    clutches_1v4, clutches_1v5, kills_1k, kills_2k, kills_3k, kills_4k,
                    kills_5k, rounds_won, rounds_lost, rank_points, rank_points_delta,
                    rank_points_previous, kd_ratio, hs_pct, esr, kills_per_round,
                    time_played_ms, elo, elo_delta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                match_id,
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

    def save_round_outcomes(self, player_id: int, match_id: str, rounds: List[Dict]) -> None:
        """Persist parsed API round outcomes for one match."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM round_outcomes WHERE player_id = ? AND match_id = ?",
            (player_id, match_id),
        )
        for r in rounds:
            cursor.execute("""
                INSERT INTO round_outcomes (player_id, match_id, round_id, end_reason, winner_side)
                VALUES (?, ?, ?, ?, ?)
            """, (
                player_id,
                match_id,
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
                    player_id, match_id, round_id, player_id_tracker, username, team_id, side,
                    operator, result, is_disconnected, kills, deaths, assists, headshots,
                    first_blood, first_death, clutch_won, clutch_lost, hs_pct, esr
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id,
                match_id,
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

            self.save_match_detail_players(player_id, match_id, players)
            self.save_round_outcomes(player_id, match_id, rounds)
            self.save_player_rounds(
                player_id,
                match_id,
                player_rounds,
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
