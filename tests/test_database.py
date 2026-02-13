# tests/test_database.py

import pytest
import os
import tempfile
import json
from src.database import Database


class TestDatabase:
    """Test suite for database operations."""

    @pytest.fixture
    def db(self):
        """Create a temporary database for testing."""
        # Create temporary file
        fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)

        # Initialize database
        database = Database(db_path)

        yield database

        # Cleanup
        database.close()
        if os.path.exists(db_path):
            os.remove(db_path)

    @pytest.fixture
    def sample_stats(self):
        """Sample parsed stats for testing."""
        return {
            'game': {
                'abandons': 0,
                'matches': 117,
                'wins': 61,
                'losses': 56,
                'match_win_pct': 52.1,
                'time_played_hours': 288.0,
                'score': 0
            },
            'rounds': {
                'disconnected': 3,
                'rounds_played': 702,
                'rounds_wins': 364,
                'rounds_losses': 338,
                'win_pct': 51.9
            },
            'combat': {
                'kills': 680,
                'deaths': 493,
                'assists': 193,
                'kd': 1.38,
                'kills_per_round': 0.97,
                'deaths_per_round': 0.70,
                'assists_per_round': 0.27,
                'kills_per_game': 5.81,
                'headshots': 408,
                'headshots_per_round': 0.58,
                'hs_pct': 60.0,
                'first_bloods': 96,
                'first_deaths': 57,
                'teamkills': 11,
                'esr': 0.63
            },
            'clutches': {
                'total': 28,
                '1v1': 16,
                '1v2': 7,
                '1v3': 5,
                '1v4': 0,
                '1v5': 0,
                'lost_total': 81,
                'lost_1v1': 6,
                'lost_1v2': 14,
                'lost_1v3': 24,
                'lost_1v4': 25,
                'lost_1v5': 12
            },
            'multikills': {
                'aces': 2,
                '1k': 224,
                '2k': 121,
                '3k': 44,
                '4k': 18
            },
            'ranked': {
                'current_rank': 29,
                'max_rank': 29,
                'top_rank_position': 0
            },
            'uncategorized': {
                'rank_points': 3806,
                'max_rank_points': 3869,
                'trn_elo': 1229
            }
        }

    def test_create_tables(self, db):
        """Test that tables are created on init."""
        cursor = db.conn.cursor()

        # Check players table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='players'")
        assert cursor.fetchone() is not None

        # Check stats_snapshots table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stats_snapshots'")
        assert cursor.fetchone() is not None

        # Check computed_metrics table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='computed_metrics'")
        assert cursor.fetchone() is not None

    def test_add_player(self, db):
        """Test adding a new player."""
        player_id = db.add_player("TestPlayer")
        assert player_id > 0

        # Verify player was added
        player = db.get_player("TestPlayer")
        assert player is not None
        assert player['username'] == "TestPlayer"
        assert player['tag'] == 'untagged'

    def test_add_duplicate_player(self, db):
        """Test that adding duplicate player returns existing id."""
        player_id1 = db.add_player("TestPlayer")
        player_id2 = db.add_player("TestPlayer")

        assert player_id1 == player_id2

    def test_get_player(self, db):
        """Test retrieving a player."""
        db.add_player("TestPlayer")
        player = db.get_player("TestPlayer")

        assert player is not None
        assert player['username'] == "TestPlayer"
        assert 'player_id' in player
        assert 'created_at' in player

    def test_get_nonexistent_player(self, db):
        """Test retrieving nonexistent player returns None."""
        player = db.get_player("NonExistent")
        assert player is None

    def test_add_stats_snapshot(self, db, sample_stats):
        """Test adding a stats snapshot."""
        snapshot_id = db.add_stats_snapshot(
            username="TestPlayer",
            stats=sample_stats,
            snapshot_date="2024-02-12",
            snapshot_time="14:30",
            season="Y10S4"
        )

        assert snapshot_id > 0

        # Verify snapshot was added
        snapshot = db.get_snapshot_by_id(snapshot_id)
        assert snapshot is not None
        assert snapshot['username'] == "TestPlayer"

    def test_field_mapping(self, db, sample_stats):
        """Test that all fields are stored correctly."""
        snapshot_id = db.add_stats_snapshot(
            username="TestPlayer",
            stats=sample_stats,
            snapshot_date="2024-02-12",
            snapshot_time="14:30",
            season="Y10S4"
        )

        snapshot = db.get_snapshot_by_id(snapshot_id)

        # Game stats
        assert snapshot['matches'] == 117
        assert snapshot['wins'] == 61
        assert snapshot['match_win_pct'] == 52.1
        assert snapshot['score'] == 0

        # Round stats - check correct field names
        assert snapshot['rounds_played'] == 702
        assert snapshot['rounds_wins'] == 364
        assert snapshot['rounds_losses'] == 338

        # Combat stats
        assert snapshot['kills'] == 680
        assert snapshot['kd'] == 1.38
        assert snapshot['first_bloods'] == 96

        # Ranked stats
        assert snapshot['current_rank'] == 29
        assert snapshot['top_rank_position'] == 0

    def test_clutch_json_storage(self, db, sample_stats):
        """Test that clutches are stored as JSON string."""
        snapshot_id = db.add_stats_snapshot(
            username="TestPlayer",
            stats=sample_stats,
            snapshot_date="2024-02-12",
            season="Y10S4"
        )

        snapshot = db.get_snapshot_by_id(snapshot_id)

        # Check clutch data is JSON string
        assert isinstance(snapshot['clutches_data'], str)

        # Parse and verify
        clutches = json.loads(snapshot['clutches_data'])
        assert clutches['total'] == 28
        assert clutches['1v1'] == 16
        assert clutches['lost_total'] == 81

    def test_rank_points_from_uncategorized(self, db, sample_stats):
        """Test that rank points come from uncategorized section."""
        snapshot_id = db.add_stats_snapshot(
            username="TestPlayer",
            stats=sample_stats,
            snapshot_date="2024-02-12",
            season="Y10S4"
        )

        snapshot = db.get_snapshot_by_id(snapshot_id)

        # Verify rank points from uncategorized
        assert snapshot['rank_points'] == 3806
        assert snapshot['max_rank_points'] == 3869
        assert snapshot['trn_elo'] == 1229

    def test_rounds_field_names(self, db, sample_stats):
        """Test that rounds use correct field names."""
        snapshot_id = db.add_stats_snapshot(
            username="TestPlayer",
            stats=sample_stats,
            snapshot_date="2024-02-12",
            season="Y10S4"
        )

        snapshot = db.get_snapshot_by_id(snapshot_id)

        # Must be rounds_played, not played
        assert 'rounds_played' in snapshot
        assert snapshot['rounds_played'] == 702

        # Must be rounds_wins/rounds_losses, not wins/losses
        assert 'rounds_wins' in snapshot
        assert 'rounds_losses' in snapshot

    def test_get_latest_snapshot(self, db, sample_stats):
        """Test getting most recent snapshot."""
        # Add multiple snapshots
        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-01-01", None, "Y10S4")
        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-01", None, "Y10S4")
        snapshot_id3 = db.add_stats_snapshot("TestPlayer", sample_stats, "2024-03-01", None, "Y10S4")

        latest = db.get_latest_snapshot("TestPlayer")

        assert latest is not None
        assert latest['snapshot_id'] == snapshot_id3
        assert latest['snapshot_date'] == "2024-03-01"

    def test_get_all_snapshots(self, db, sample_stats):
        """Test getting all snapshots for a player."""
        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-01-01", None, "Y10S4")
        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-01", None, "Y10S4")
        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-03-01", None, "Y10S4")

        snapshots = db.get_all_snapshots("TestPlayer")

        assert len(snapshots) == 3
        # Should be ordered by date DESC
        assert snapshots[0]['snapshot_date'] == "2024-03-01"
        assert snapshots[2]['snapshot_date'] == "2024-01-01"

    def test_add_computed_metrics(self, db, sample_stats):
        """Test adding computed metrics."""
        snapshot_id = db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-12", None, "Y10S4")
        player = db.get_player("TestPlayer")

        metrics = {
            'entry_efficiency': 0.63,
            'clutch_1v1_success': 0.73,
            'teamplay_index': 0.22,
            'primary_role': 'Fragger',
            'primary_confidence': 120.0,
            'secondary_role': 'Carry',
            'secondary_confidence': 90.0,
            'aggression_score': 0.22,
            'clutch_attempt_rate': 0.16,
            'clutch_disadvantaged_success': 0.24,
            'overall_clutch_success': 0.26,
            'clutch_dropoff_rate': 0.10,
            'clutch_efficiency_score': 50.0,
            'fragger_score': 110.0,
            'entry_score': 45.0,
            'support_score': 50.0,
            'anchor_score': 40.0,
            'clutch_specialist_score': 35.0,
            'carry_score': 90.0,
            'impact_rating': 1.5,
            'wins_per_hour': 0.21,
            'kd_win_gap': 0.34
        }

        metric_id = db.add_computed_metrics(snapshot_id, player['player_id'], metrics)
        assert metric_id > 0

        # Verify metrics were saved
        saved_metrics = db.get_computed_metrics(snapshot_id)
        assert saved_metrics is not None
        assert saved_metrics['primary_role'] == 'Fragger'
        assert saved_metrics['entry_efficiency'] == 0.63

    def test_get_computed_metrics(self, db, sample_stats):
        """Test retrieving computed metrics."""
        snapshot_id = db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-12", None, "Y10S4")
        player = db.get_player("TestPlayer")

        metrics = {
            'primary_role': 'Fragger',
            'primary_confidence': 120.0,
            'entry_efficiency': 0.63
        }

        db.add_computed_metrics(snapshot_id, player['player_id'], metrics)

        retrieved = db.get_computed_metrics(snapshot_id)
        assert retrieved['primary_role'] == 'Fragger'

    def test_player_exists(self, db):
        """Test player_exists method."""
        assert not db.player_exists("TestPlayer")

        db.add_player("TestPlayer")

        assert db.player_exists("TestPlayer")

    def test_snapshot_count(self, db, sample_stats):
        """Test snapshot count."""
        assert db.snapshot_count("TestPlayer") == 0

        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-01-01", None, "Y10S4")
        assert db.snapshot_count("TestPlayer") == 1

        db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-01", None, "Y10S4")
        assert db.snapshot_count("TestPlayer") == 2

    def test_update_player_tag(self, db):
        """Test updating player tag."""
        db.add_player("TestPlayer")

        player = db.get_player("TestPlayer")
        assert player['tag'] == 'untagged'

        db.update_player_tag("TestPlayer", "pro")

        player = db.get_player("TestPlayer")
        assert player['tag'] == 'pro'

    def test_delete_player(self, db, sample_stats):
        """Test deleting player and cascading snapshots."""
        # Add player with snapshot
        snapshot_id = db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-12", None, "Y10S4")
        player = db.get_player("TestPlayer")

        # Add metrics
        db.add_computed_metrics(snapshot_id, player['player_id'], {'primary_role': 'Fragger'})

        # Verify exists
        assert db.player_exists("TestPlayer")
        assert db.get_snapshot_by_id(snapshot_id) is not None

        # Delete player
        db.delete_player("TestPlayer")

        # Verify deleted
        assert not db.player_exists("TestPlayer")
        assert db.get_snapshot_by_id(snapshot_id) is None

    def test_delete_snapshot(self, db, sample_stats):
        """Test deleting a snapshot."""
        snapshot_id = db.add_stats_snapshot("TestPlayer", sample_stats, "2024-02-12", None, "Y10S4")

        # Verify exists
        assert db.get_snapshot_by_id(snapshot_id) is not None

        # Delete
        db.delete_snapshot(snapshot_id)

        # Verify deleted
        assert db.get_snapshot_by_id(snapshot_id) is None

    def test_get_all_seasons(self, db, sample_stats):
        """Test getting all seasons."""
        db.add_stats_snapshot("Player1", sample_stats, "2024-01-01", None, "Y10S3")
        db.add_stats_snapshot("Player2", sample_stats, "2024-02-01", None, "Y10S4")
        db.add_stats_snapshot("Player3", sample_stats, "2024-03-01", None, "Y10S4")

        seasons = db.get_all_seasons()

        assert 'Y10S3' in seasons
        assert 'Y10S4' in seasons
        assert len(seasons) == 2

    def test_close_connection(self, db):
        """Test closing database connection."""
        db.close()
        # Should not raise error
        assert True
