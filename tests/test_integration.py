# tests/test_integration.py

import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import Database
from src.stack_manager import StackManager
from src.team_analyzer import TeamAnalyzer
from src.matchup_analyzer import MatchupAnalyzer
from tests.helpers import create_test_db, add_sample_players, add_opponent_players


class TestIntegration(unittest.TestCase):
    """End-to-end integration test for v0.4 stack flows."""

    def setUp(self):
        self.db = create_test_db()
        self.mgr = StackManager(self.db)
        self.team_analyzer = TeamAnalyzer(self.db)
        self.matchup_analyzer = MatchupAnalyzer(self.db)
        add_sample_players(self.db)
        add_opponent_players(self.db)

    def tearDown(self):
        self.db.close()
        if os.path.exists('data/test_jakal.db'):
            os.remove('data/test_jakal.db')

    def test_full_stack_flow(self):
        """Create two 5-player stacks, analyze each, run matchup."""
        # 1. Create stacks
        stack_a_id = self.mgr.create_stack("Main Stack", description="Our team")
        stack_b_id = self.mgr.create_stack("Enemy Stack", description="Opponents")

        # 2. Add players
        for name in ["PlayerA", "PlayerB", "PlayerC", "PlayerD", "PlayerE"]:
            self.mgr.add_player_to_stack(stack_a_id, name)
        for name in ["EnemyA", "EnemyB", "EnemyC", "EnemyD", "EnemyE"]:
            self.mgr.add_player_to_stack(stack_b_id, name)

        # 3. Validate
        val_a = self.mgr.validate_stack(stack_a_id)
        val_b = self.mgr.validate_stack(stack_b_id)
        self.assertTrue(val_a['valid'])
        self.assertTrue(val_b['valid'])
        self.assertEqual(val_a['size'], 5)
        self.assertEqual(val_b['size'], 5)

        # 4. Analyze Stack A
        analysis_a = self.team_analyzer.analyze_stack(stack_a_id)
        self.assertIsNotNone(analysis_a)
        self.assertEqual(len(analysis_a['members']), 5)
        self.assertGreater(analysis_a['team_avg_kd'], 0)
        self.assertIsInstance(analysis_a['team_strengths'], list)
        self.assertIsInstance(analysis_a['team_weaknesses'], list)
        self.assertIsInstance(analysis_a['team_insights'], list)

        # 5. Analyze Stack B
        analysis_b = self.team_analyzer.analyze_stack(stack_b_id)
        self.assertIsNotNone(analysis_b)
        self.assertEqual(len(analysis_b['members']), 5)

        # 6. Run 5v5 matchup
        matchup = self.matchup_analyzer.analyze_matchup(stack_a_id, stack_b_id)
        self.assertIsNotNone(matchup)
        self.assertIn(matchup['predicted_winner'], ['A', 'B', 'Even'])
        self.assertGreaterEqual(matchup['confidence'], 30)
        self.assertLessEqual(matchup['confidence'], 70)
        self.assertGreater(len(matchup['recommendations']), 0)
        self.assertGreater(len(matchup['key_battlegrounds']), 0)

        # 7. Verify database persistence
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM stacks")
        self.assertEqual(cursor.fetchone()['cnt'], 2)

        cursor.execute("SELECT COUNT(*) as cnt FROM stack_members")
        self.assertEqual(cursor.fetchone()['cnt'], 10)

        cursor.execute("SELECT COUNT(*) as cnt FROM stack_analyses")
        # analyze_matchup calls analyze_stack for each, plus our 2 direct calls = 4 total
        self.assertGreaterEqual(cursor.fetchone()['cnt'], 2)

        cursor.execute("SELECT COUNT(*) as cnt FROM matchup_analyses")
        self.assertEqual(cursor.fetchone()['cnt'], 1)

    def test_quick_stack_cleanup(self):
        """Quick stacks should be deleted on cleanup."""
        self.mgr.create_quick_stack(["PlayerA", "PlayerB", "PlayerC"])
        self.mgr.create_stack("Permanent Stack")

        self.mgr.cleanup_quick_stacks()

        stacks = self.mgr.get_all_stacks()
        types = [s['stack_type'] for s in stacks]
        self.assertNotIn('quick', types)
        self.assertIn('named', types)

    def test_tagged_stack_build_and_analyze(self):
        """Build a tagged stack and run analysis."""
        stack_id = self.mgr.build_tagged_stack('teammate')
        size = self.mgr.get_stack_size(stack_id)
        self.assertGreaterEqual(size, 2)

        analysis = self.team_analyzer.analyze_stack(stack_id)
        self.assertIsNotNone(analysis)
        self.assertGreater(analysis['team_avg_kd'], 0)


if __name__ == '__main__':
    unittest.main()
