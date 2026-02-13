# src/database.py

import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

class Database:
    """Handle all database operations."""
    
    def __init__(self, db_path: str = 'data/jakal.db'):
        self.db_path = db_path
        self.conn = None
        self.init_database()
    
    def init_database(self):
        """Create tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Access columns by name
            self.conn.execute("PRAGMA foreign_keys = ON")

            cursor = self.conn.cursor()

            # Players table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    device_tag TEXT DEFAULT 'pc',
                    tag TEXT DEFAULT 'untagged',
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

            self.conn.commit()
            self._migrate_schema()
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to initialize database: {e}")

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
        self._add_column_if_missing("players", "device_tag TEXT DEFAULT 'pc'", "device_tag")
        self._add_column_if_missing("players", "tag TEXT DEFAULT 'untagged'", "tag")
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
                "SELECT player_id, username, device_tag, tag, created_at, notes FROM players ORDER BY username"
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
                SELECT player_id, username, device_tag, tag, created_at, notes
                FROM players
                WHERE username = ?
            """, (username,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to get player '{username}': {e}")

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

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
