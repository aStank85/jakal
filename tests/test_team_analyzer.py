# tests/test_team_analyzer.py

import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import Database
from src.stack_manager import StackManager
from src.team_analyzer import TeamAnalyzer
from tests.helpers import create_test_db, add_sample_players


class TestTeamAnalyzer(unittest.TestCase):
    """Tests for TeamAnalyzer."""

    def setUp(self):
        self.db = create_test_db()
        self.mgr = StackManager(self.db)
        self.analyzer = TeamAnalyzer(self.db)
        add_sample_players(self.db)
        # Create a stack with all 5 sample players
        self.stack_id = self.mgr.create_stack("Test Stack")
        for name in ["PlayerA", "PlayerB", "PlayerC", "PlayerD", "PlayerE"]:
            self.mgr.add_player_to_stack(self.stack_id, name)

    def tearDown(self):
        self.db.close()
        if os.path.exists('data/test_jakal.db'):
            os.remove('data/test_jakal.db')

    # --- Full analysis ---

    def test_analyze_stack_returns_dict(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertIsInstance(result, dict)

    def test_analyze_stack_has_required_keys(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        required = [
            'stack', 'members', 'role_distribution', 'roles_covered',
            'roles_missing', 'composition_score', 'team_avg_kd',
            'team_avg_win_pct', 'team_avg_hs_pct', 'team_avg_kpr',
            'team_avg_apr', 'team_entry_efficiency', 'carry_player',
            'carry_dependency', 'team_strengths', 'team_weaknesses',
            'team_insights', 'data_quality_warnings'
        ]
        for key in required:
            self.assertIn(key, result, f"Missing key: {key}")

    def test_analyze_nonexistent_stack_raises(self):
        with self.assertRaises(ValueError):
            self.analyzer.analyze_stack(999)

    def test_analyze_empty_stack_raises(self):
        empty_id = self.mgr.create_stack("Empty")
        with self.assertRaises(ValueError):
            self.analyzer.analyze_stack(empty_id)

    # --- Composition ---

    def test_role_distribution(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        dist = result['role_distribution']
        self.assertIsInstance(dist, dict)
        total = sum(dist.values())
        self.assertEqual(total, 5)

    def test_roles_covered_is_list(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertIsInstance(result['roles_covered'], list)

    def test_roles_missing_is_list(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertIsInstance(result['roles_missing'], list)

    def test_composition_score_range(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        score = result['composition_score']
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)

    def test_perfect_composition_score(self):
        dist = {'Fragger': 1, 'Entry': 1, 'Support': 1, 'Anchor': 1, 'Carry': 1}
        score = self.analyzer.calculate_composition_score(dist)
        self.assertGreaterEqual(score, 80)  # Should be high with all unique roles + critical covered

    def test_poor_composition_score(self):
        dist = {'Fragger': 5}
        score = self.analyzer.calculate_composition_score(dist)
        self.assertLess(score, 50)

    # --- Team averages ---

    def test_team_averages_positive(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertGreater(result['team_avg_kd'], 0)
        self.assertGreater(result['team_avg_win_pct'], 0)

    # --- Entry analysis ---

    def test_entry_efficiency_range(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        ee = result['team_entry_efficiency']
        self.assertGreaterEqual(ee, 0)
        self.assertLessEqual(ee, 1)

    def test_dedicated_entry_count_non_negative(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertGreaterEqual(result['dedicated_entry_count'], 0)

    # --- Clutch analysis ---

    def test_clutch_hierarchy_sorted(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        hierarchy = result['clutch_hierarchy']
        self.assertEqual(len(hierarchy), 5)
        # Verify sorted by clutch_1v1 descending
        for i in range(len(hierarchy) - 1):
            self.assertGreaterEqual(hierarchy[i]['clutch_1v1'], hierarchy[i+1]['clutch_1v1'])

    def test_clutch_gap_non_negative(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertGreaterEqual(result['clutch_gap'], 0)

    # --- Carry analysis ---

    def test_carry_player_is_string(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertIsInstance(result['carry_player'], str)
        self.assertTrue(len(result['carry_player']) > 0)

    def test_carry_dependency_range(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        dep = result['carry_dependency']
        self.assertGreaterEqual(dep, 0)
        self.assertLessEqual(dep, 100)

    # --- Insights ---

    def test_insights_are_list(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertIsInstance(result['team_insights'], list)

    def test_insight_structure(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        if result['team_insights']:
            insight = result['team_insights'][0]
            self.assertIn('severity', insight)
            self.assertIn('category', insight)
            self.assertIn('message', insight)
            self.assertIn('action', insight)

    def test_strengths_and_weaknesses(self):
        result = self.analyzer.analyze_stack(self.stack_id)
        self.assertIsInstance(result['team_strengths'], list)
        self.assertIsInstance(result['team_weaknesses'], list)

    # --- Database persistence ---

    def test_analysis_saved_to_db(self):
        self.analyzer.analyze_stack(self.stack_id)
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM stack_analyses WHERE stack_id = ?", (self.stack_id,))
        count = cursor.fetchone()['cnt']
        self.assertGreater(count, 0)

    def test_analysis_reports_defaulted_fields_warning(self):
        cursor = self.db.conn.cursor()
        cursor.execute("""
            UPDATE stats_snapshots
            SET rounds_played = NULL
            WHERE snapshot_id = (
                SELECT s.snapshot_id
                FROM stats_snapshots s
                JOIN players p ON p.player_id = s.player_id
                WHERE p.username = 'PlayerA'
                ORDER BY s.snapshot_date DESC, s.snapshot_time DESC
                LIMIT 1
            )
        """)
        self.db.conn.commit()

        result = self.analyzer.analyze_stack(self.stack_id)

        self.assertTrue(result['data_quality_warnings'])
        messages = [w['message'] for w in result['data_quality_warnings']]
        self.assertTrue(any('PlayerA' in m and 'rounds_played' in m for m in messages))


if __name__ == '__main__':
    unittest.main()
