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

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
