# tests/test_integration.py

import pytest
import os
import tempfile
from src.parser import R6TrackerParser
from src.database import Database
from src.calculator import MetricsCalculator
from src.comparator import PlayerComparator


class TestIntegration:
    """Integration tests for full workflow."""

    @pytest.fixture
    def components(self):
        """Create all components with temporary database."""
        # Create temporary database
        fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)

        parser = R6TrackerParser()
        db = Database(db_path)
        calculator = MetricsCalculator()
        comparator = PlayerComparator()

        yield {
            'parser': parser,
            'db': db,
            'calculator': calculator,
            'comparator': comparator
        }

        # Cleanup
        db.close()
        if os.path.exists(db_path):
            os.remove(db_path)

    @pytest.fixture
    def sample_paste_1(self):
        """Sample R6 Tracker paste data - Player 1."""
        return """
Game
Abandons
0
Losses
56
Match Win %
52.1%
Matches
117
Score
0
Time Played
288h
Wins
61
Rounds
Disconnected
3
Losses
338
Rounds Played
702
Win %
51.9%
Wins
364
Combat
Assists
193
Assists/Round
0.27
Deaths
493
Deaths/Round
0.70
ESR
0.63
First Bloods
96
First Deaths
57
Headshots
408
Headshots/Round
0.58
HS %
60.0%
K/D
1.38
Kills
680
Kills/Game
5.81
Kills/Round
0.97
TKs
11
Rounds - Clutches
Clutches
28
Clutches 1v1
16
Clutches 1v2
7
Clutches 1v3
5
Clutches 1v4
0
Clutches 1v5
0
Clutches Lost
81
Clutches Lost 1v1
6
Clutches Lost 1v2
14
Clutches Lost 1v3
24
Clutches Lost 1v4
25
Clutches Lost 1v5
12
Multikills
Aces
2
Kills 1K
224
Kills 2K
121
Kills 3K
44
Kills 4K
18
Ranked
Max Rank
29
Rank
29
Top Rank Position
0
Uncategorized
Max Rank Points
3,869
Rank Points
3,806
TRN Elo
1,229
"""

    @pytest.fixture
    def sample_paste_2(self):
        """Sample R6 Tracker paste data - Player 2."""
        return """
Game
Abandons
1
Losses
102
Match Win %
47.5%
Matches
200
Score
0
Time Played
450h
Wins
98
Rounds
Disconnected
5
Losses
610
Rounds Played
1180
Win %
48.3%
Wins
570
Combat
Assists
350
Assists/Round
0.30
Deaths
850
Deaths/Round
0.72
ESR
0.58
First Bloods
140
First Deaths
120
Headshots
550
Headshots/Round
0.47
HS %
55.0%
K/D
1.11
Kills
945
Kills/Game
4.73
Kills/Round
0.80
TKs
15
Rounds - Clutches
Clutches
45
Clutches 1v1
25
Clutches 1v2
12
Clutches 1v3
6
Clutches 1v4
2
Clutches 1v5
0
Clutches Lost
125
Clutches Lost 1v1
12
Clutches Lost 1v2
30
Clutches Lost 1v3
45
Clutches Lost 1v4
28
Clutches Lost 1v5
10
Multikills
Aces
1
Kills 1K
380
Kills 2K
195
Kills 3K
52
Kills 4K
12
Ranked
Max Rank
25
Rank
24
Top Rank Position
0
Uncategorized
Max Rank Points
3,150
Rank Points
3,050
TRN Elo
1,105
"""

    def test_full_workflow(self, components, sample_paste_1):
        """Test complete workflow: parse -> store -> calculate -> retrieve."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']

        # 1. Parse stats
        stats = parser.parse(sample_paste_1)
        assert 'game' in stats
        assert 'combat' in stats
        assert stats['game']['matches'] == 117

        # 2. Store in database
        snapshot_id = db.add_stats_snapshot(
            username="TestPlayer1",
            stats=stats,
            snapshot_date="2024-02-12",
            snapshot_time="14:30",
            season="Y10S4"
        )
        assert snapshot_id > 0

        # 3. Calculate metrics
        snapshot = db.get_latest_snapshot("TestPlayer1")
        assert snapshot is not None

        metrics = calculator.calculate_all(snapshot)
        assert 'primary_role' in metrics
        assert 'entry_efficiency' in metrics

        # Verify expected values
        assert pytest.approx(metrics['entry_efficiency'], rel=0.01) == 0.627  # 96/(96+57)

        # 4. Store metrics
        player = db.get_player("TestPlayer1")
        db.add_computed_metrics(snapshot_id, player['player_id'], metrics)

        # 5. Retrieve metrics from database
        saved_metrics = db.get_computed_metrics(snapshot_id)
        assert saved_metrics is not None
        assert saved_metrics['primary_role'] == metrics['primary_role']

    def test_two_player_comparison(self, components, sample_paste_1, sample_paste_2):
        """Test end-to-end comparison of two players."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']
        comparator = components['comparator']

        # Add Player 1
        stats1 = parser.parse(sample_paste_1)
        snapshot_id1 = db.add_stats_snapshot(
            username="Player1",
            stats=stats1,
            snapshot_date="2024-02-12",
            season="Y10S4"
        )
        snapshot1 = db.get_latest_snapshot("Player1")
        metrics1 = calculator.calculate_all(snapshot1)
        player1 = db.get_player("Player1")
        db.add_computed_metrics(snapshot_id1, player1['player_id'], metrics1)

        # Add Player 2
        stats2 = parser.parse(sample_paste_2)
        snapshot_id2 = db.add_stats_snapshot(
            username="Player2",
            stats=stats2,
            snapshot_date="2024-02-12",
            season="Y10S4"
        )
        snapshot2 = db.get_latest_snapshot("Player2")
        metrics2 = calculator.calculate_all(snapshot2)
        player2 = db.get_player("Player2")
        db.add_computed_metrics(snapshot_id2, player2['player_id'], metrics2)

        # Compare
        snapshots = [snapshot1, snapshot2]
        metrics_list = [metrics1, metrics2]

        comparison = comparator.compare(snapshots, metrics_list)

        # Verify comparison structure
        assert 'players' in comparison
        assert 'stats' in comparison
        assert 'winners' in comparison

        assert len(comparison['players']) == 2
        assert comparison['players'][0]['username'] == "Player1"
        assert comparison['players'][1]['username'] == "Player2"

        # Verify some expected winners
        # Player1 should win K/D (1.38 vs 1.11)
        kd_stat = next((s for s in comparison['stats'] if s['name'] == 'K/D'), None)
        assert kd_stat is not None
        assert kd_stat['winner_index'] == 0  # Player1

        # Player1 should win Win% (52.1 vs 47.5)
        win_stat = next((s for s in comparison['stats'] if s['name'] == 'Win %'), None)
        assert win_stat is not None
        assert win_stat['winner_index'] == 0  # Player1

    def test_multiple_snapshots(self, components, sample_paste_1):
        """Test handling multiple snapshots for same player."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']

        stats = parser.parse(sample_paste_1)

        # Add multiple snapshots
        db.add_stats_snapshot("TestPlayer", stats, "2024-01-01", None, "Y10S3")
        db.add_stats_snapshot("TestPlayer", stats, "2024-02-01", None, "Y10S4")
        snapshot_id3 = db.add_stats_snapshot("TestPlayer", stats, "2024-03-01", None, "Y10S4")

        # Verify count
        assert db.snapshot_count("TestPlayer") == 3

        # Verify latest is correct
        latest = db.get_latest_snapshot("TestPlayer")
        assert latest['snapshot_id'] == snapshot_id3
        assert latest['snapshot_date'] == "2024-03-01"

        # Verify all snapshots retrievable
        all_snapshots = db.get_all_snapshots("TestPlayer")
        assert len(all_snapshots) == 3

        # Calculate metrics for latest
        metrics = calculator.calculate_all(latest)
        player = db.get_player("TestPlayer")
        db.add_computed_metrics(snapshot_id3, player['player_id'], metrics)

        # Retrieve and verify
        saved_metrics = db.get_latest_metrics("TestPlayer")
        assert saved_metrics is not None
        assert saved_metrics['primary_role'] == metrics['primary_role']

    def test_all_metrics_calculated(self, components, sample_paste_1):
        """Verify all required metrics are calculated."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']

        stats = parser.parse(sample_paste_1)
        snapshot_id = db.add_stats_snapshot("TestPlayer", stats, "2024-02-12", None, "Y10S4")
        snapshot = db.get_latest_snapshot("TestPlayer")

        metrics = calculator.calculate_all(snapshot)

        # Verify all required metrics exist
        required_metrics = [
            'entry_efficiency',
            'aggression_score',
            'clutch_attempt_rate',
            'clutch_1v1_success',
            'clutch_disadvantaged_success',
            'overall_clutch_success',
            'clutch_dropoff_rate',
            'clutch_efficiency_score',
            'teamplay_index',
            'fragger_score',
            'entry_score',
            'support_score',
            'anchor_score',
            'clutch_specialist_score',
            'carry_score',
            'primary_role',
            'primary_confidence',
            'secondary_role',
            'secondary_confidence',
            'impact_rating',
            'wins_per_hour',
            'kd_win_gap'
        ]

        for metric in required_metrics:
            assert metric in metrics, f"Missing metric: {metric}"

    def test_metrics_persistence(self, components, sample_paste_1):
        """Test that metrics are correctly persisted and retrieved."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']

        stats = parser.parse(sample_paste_1)
        snapshot_id = db.add_stats_snapshot("TestPlayer", stats, "2024-02-12", None, "Y10S4")
        snapshot = db.get_latest_snapshot("TestPlayer")

        # Calculate and save metrics
        metrics = calculator.calculate_all(snapshot)
        player = db.get_player("TestPlayer")
        db.add_computed_metrics(snapshot_id, player['player_id'], metrics)

        # Retrieve and compare
        saved = db.get_computed_metrics(snapshot_id)

        # Check persisted metrics match for schema-backed keys
        for key, saved_value in saved.items():
            if key in metrics:
                metric_value = metrics[key]
                if isinstance(metric_value, float):
                    assert pytest.approx(saved_value, rel=0.01) == metric_value
                else:
                    assert saved_value == metric_value

    def test_backfilled_snapshot_calculates_from_inserted_id(self, components, sample_paste_1):
        """Test that backfilled inserts use inserted snapshot row for calculations."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']

        stats = parser.parse(sample_paste_1)

        # Insert newer snapshot first.
        db.add_stats_snapshot("BackfillUser", stats, "2024-03-01", None, "Y10S4")

        # Mutate wins and insert older snapshot (backfill).
        backfill_stats = {section: values.copy() for section, values in stats.items()}
        backfill_stats['game']['wins'] = 10
        backfill_snapshot_id = db.add_stats_snapshot("BackfillUser", backfill_stats, "2024-01-01", None, "Y10S3")

        inserted_snapshot = db.get_snapshot_by_id(backfill_snapshot_id)
        latest_snapshot = db.get_latest_snapshot("BackfillUser")

        # Sanity check we are truly backfilling an older row.
        assert inserted_snapshot['snapshot_date'] == "2024-01-01"
        assert latest_snapshot['snapshot_date'] == "2024-03-01"
        assert inserted_snapshot['wins'] == 10
        assert latest_snapshot['wins'] != inserted_snapshot['wins']

        inserted_metrics = calculator.calculate_all(inserted_snapshot)
        latest_metrics = calculator.calculate_all(latest_snapshot)
        assert inserted_metrics['wins_per_hour'] != latest_metrics['wins_per_hour']

    def test_comparison_tie_has_no_winner(self, components, sample_paste_1):
        """Test ties return no winner instead of biasing player order."""
        parser = components['parser']
        db = components['db']
        calculator = components['calculator']
        comparator = components['comparator']

        stats = parser.parse(sample_paste_1)
        db.add_stats_snapshot("TieA", stats, "2024-02-12", None, "Y10S4")
        db.add_stats_snapshot("TieB", stats, "2024-02-12", None, "Y10S4")

        snapshot_a = db.get_latest_snapshot("TieA")
        snapshot_b = db.get_latest_snapshot("TieB")
        metrics_a = calculator.calculate_all(snapshot_a)
        metrics_b = calculator.calculate_all(snapshot_b)

        comparison = comparator.compare([snapshot_a, snapshot_b], [metrics_a, metrics_b])
        kd_stat = next((item for item in comparison['stats'] if item['name'] == 'K/D'), None)

        assert kd_stat is not None
        assert kd_stat['winner_index'] is None
