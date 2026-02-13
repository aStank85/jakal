# tests/test_matchup_analyzer.py

import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import Database
from src.stack_manager import StackManager
from src.matchup_analyzer import MatchupAnalyzer
from tests.helpers import create_test_db, add_sample_players, add_opponent_players


class TestMatchupAnalyzer(unittest.TestCase):
    """Tests for MatchupAnalyzer."""

    def setUp(self):
        self.db = create_test_db()
        self.mgr = StackManager(self.db)
        self.analyzer = MatchupAnalyzer(self.db)

        add_sample_players(self.db)
        add_opponent_players(self.db)

        # Create Stack A (your team)
        self.stack_a_id = self.mgr.create_stack("Main Stack")
        for name in ["PlayerA", "PlayerB", "PlayerC", "PlayerD", "PlayerE"]:
            self.mgr.add_player_to_stack(self.stack_a_id, name)

        # Create Stack B (opponents)
        self.stack_b_id = self.mgr.create_stack("Enemy Stack")
        for name in ["EnemyA", "EnemyB", "EnemyC", "EnemyD", "EnemyE"]:
            self.mgr.add_player_to_stack(self.stack_b_id, name)

    def tearDown(self):
        self.db.close()
        if os.path.exists('data/test_jakal.db'):
            os.remove('data/test_jakal.db')

    # --- Full matchup analysis ---

    def test_analyze_matchup_returns_dict(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertIsInstance(result, dict)

    def test_matchup_has_required_keys(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        required = [
            'stack_a_id', 'stack_b_id', 'stack_a', 'stack_b',
            'category_comparisons', 'role_matchups',
            'predicted_winner', 'confidence',
            'recommendations', 'key_battlegrounds'
        ]
        for key in required:
            self.assertIn(key, result, f"Missing key: {key}")

    # --- Category comparisons ---

    def test_category_comparisons_has_all_categories(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        expected = ['kd', 'entry', 'clutch', 'support', 'hs_pct', 'win_rate']
        for cat in expected:
            self.assertIn(cat, result['category_comparisons'])

    def test_compare_category_a_wins(self):
        comp = self.analyzer.compare_category(1.5, 1.0, 'K/D')
        self.assertEqual(comp['winner'], 'A')
        self.assertGreater(comp['margin'], 0)

    def test_compare_category_b_wins(self):
        comp = self.analyzer.compare_category(0.8, 1.2, 'K/D')
        self.assertEqual(comp['winner'], 'B')

    def test_compare_category_even(self):
        comp = self.analyzer.compare_category(1.00, 1.01, 'K/D')
        self.assertEqual(comp['winner'], 'Even')

    def test_compare_category_significance_large(self):
        comp = self.analyzer.compare_category(1.5, 1.0, 'K/D')
        self.assertEqual(comp['significance'], 'large')

    def test_compare_category_significance_small(self):
        comp = self.analyzer.compare_category(1.05, 1.02, 'K/D')
        self.assertEqual(comp['significance'], 'small')

    # --- Role matchups ---

    def test_role_matchups_is_list(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertIsInstance(result['role_matchups'], list)

    def test_role_matchup_structure(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        if result['role_matchups']:
            rm = result['role_matchups'][0]
            self.assertIn('role', rm)
            self.assertIn('your_player', rm)
            self.assertIn('their_player', rm)
            self.assertIn('advantage', rm)

    # --- Prediction ---

    def test_predicted_winner_valid(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertIn(result['predicted_winner'], ['A', 'B', 'Even'])

    def test_confidence_range(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertGreaterEqual(result['confidence'], 30)
        self.assertLessEqual(result['confidence'], 70)

    def test_predict_outcome_balanced(self):
        # Perfectly equal categories should be Even
        comps = {
            'kd': {'winner': 'Even', 'margin': 0.01, 'category': 'K/D'},
            'entry': {'winner': 'Even', 'margin': 0.01, 'category': 'Entry'},
            'clutch': {'winner': 'Even', 'margin': 0.01, 'category': 'Clutch'},
            'support': {'winner': 'Even', 'margin': 0.01, 'category': 'Support'},
            'hs_pct': {'winner': 'Even', 'margin': 0.01, 'category': 'HS'},
            'win_rate': {'winner': 'Even', 'margin': 0.01, 'category': 'Win Rate'},
        }
        pred = self.analyzer.predict_outcome(comps)
        self.assertEqual(pred['predicted_winner'], 'Even')

    # --- Recommendations ---

    def test_recommendations_is_list(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertIsInstance(result['recommendations'], list)
        self.assertGreater(len(result['recommendations']), 0)

    # --- Key battlegrounds ---

    def test_battlegrounds_is_list(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertIsInstance(result['key_battlegrounds'], list)
        self.assertGreater(len(result['key_battlegrounds']), 0)

    def test_battlegrounds_max_3(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertLessEqual(len(result['key_battlegrounds']), 3)

    # --- Database persistence ---

    def test_matchup_saved_to_db(self):
        self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM matchup_analyses")
        count = cursor.fetchone()['cnt']
        self.assertGreater(count, 0)

    def test_matchup_stores_correct_stack_ids(self):
        self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT stack_a_id, stack_b_id FROM matchup_analyses LIMIT 1")
        row = cursor.fetchone()
        self.assertEqual(row['stack_a_id'], self.stack_a_id)
        self.assertEqual(row['stack_b_id'], self.stack_b_id)


if __name__ == '__main__':
    unittest.main()
