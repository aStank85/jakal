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

    def test_carry_metric_weights_win_rate_heavily(self):
        player_a = {
            'role': 'Carry',
            'snapshot': {'match_win_pct': 58.6, 'kd': 0.73},
            'metrics': {}
        }
        player_b = {
            'role': 'Carry',
            'snapshot': {'match_win_pct': 57.1, 'kd': 0.83},
            'metrics': {}
        }

        metric = self.analyzer._pair_metric(player_a, player_b)
        advantage = self.analyzer._advantage_from_values(
            metric['value_a'], metric['value_b'], metric['threshold']
        )
        self.assertIn(advantage, ['yours', 'even'])

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

    def test_role_matchups_include_all_players(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        matchups = result['role_matchups']
        yours = {m['your_player'] for m in matchups if m['your_player'] != '-'}
        theirs = {m['their_player'] for m in matchups if m['their_player'] != '-'}

        self.assertEqual(len(yours), 5)
        self.assertEqual(len(theirs), 5)
        self.assertEqual(len(matchups), 5)

    def test_support_row_aligns_with_teamplay_winner(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        support_winner = result['category_comparisons']['support']['winner']
        support_rows = [
            m for m in result['role_matchups']
            if 'Support' in m['role']
        ]
        self.assertGreater(len(support_rows), 0)

        if support_winner == 'A':
            self.assertTrue(all(m['advantage'] != 'theirs' for m in support_rows))
        elif support_winner == 'B':
            self.assertTrue(all(m['advantage'] != 'yours' for m in support_rows))
        else:
            self.assertTrue(all(m['advantage'] == 'even' for m in support_rows))

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

    def test_battlegrounds_max_3(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        self.assertLessEqual(len(result['key_battlegrounds']), 3)

    def test_battlegrounds_are_category_labels_only(self):
        result = self.analyzer.analyze_matchup(self.stack_a_id, self.stack_b_id)
        for bg in result['key_battlegrounds']:
            self.assertNotIn('decides', bg.lower())
            self.assertNotIn('closest margin', bg.lower())

    def test_battlegrounds_clear_advantage_message(self):
        comps = {
            'kd': {'winner': 'A', 'margin': 0.10, 'category': 'K/D'},
            'entry': {'winner': 'A', 'margin': 0.12, 'category': 'Entry Eff.'},
            'clutch': {'winner': 'A', 'margin': 0.08, 'category': 'Clutch 1v1'},
            'support': {'winner': 'A', 'margin': 0.09, 'category': 'Teamplay'},
            'hs_pct': {'winner': 'A', 'margin': 0.05, 'category': 'HS %'},
            'win_rate': {'winner': 'B', 'margin': 0.01, 'category': 'Win Rate'},
        }
        battlegrounds = self.analyzer.identify_key_battlegrounds(comps)
        self.assertEqual(battlegrounds, ["Clear advantage - no significant battlegrounds"])

    def test_battlegrounds_require_split_category_wins(self):
        comps = {
            'kd': {'winner': 'A', 'margin': 0.04, 'category': 'K/D'},
            'entry': {'winner': 'A', 'margin': 0.03, 'category': 'Entry Eff.'},
            'clutch': {'winner': 'A', 'margin': 0.05, 'category': 'Clutch 1v1'},
            'support': {'winner': 'A', 'margin': 0.02, 'category': 'Teamplay'},
            'hs_pct': {'winner': 'Even', 'margin': 0.01, 'category': 'HS %'},
            'win_rate': {'winner': 'B', 'margin': 0.02, 'category': 'Win Rate'},
        }
        battlegrounds = self.analyzer.identify_key_battlegrounds(comps)
        self.assertEqual(battlegrounds, [])

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
