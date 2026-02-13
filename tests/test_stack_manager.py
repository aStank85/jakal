# tests/test_stack_manager.py

import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import Database
from src.stack_manager import StackManager
from tests.helpers import create_test_db, add_sample_players


class TestStackManager(unittest.TestCase):
    """Tests for StackManager."""

    def setUp(self):
        self.db = create_test_db()
        self.mgr = StackManager(self.db)
        add_sample_players(self.db)

    def tearDown(self):
        self.db.close()
        if os.path.exists('data/test_jakal.db'):
            os.remove('data/test_jakal.db')

    # --- Stack CRUD ---

    def test_create_stack(self):
        stack_id = self.mgr.create_stack("Main Stack", description="My team")
        self.assertIsNotNone(stack_id)
        stack = self.mgr.get_stack(stack_id)
        self.assertEqual(stack['stack_name'], "Main Stack")
        self.assertEqual(stack['stack_type'], 'named')

    def test_create_duplicate_stack_raises(self):
        self.mgr.create_stack("Team Alpha")
        with self.assertRaises(ValueError):
            self.mgr.create_stack("Team Alpha")

    def test_get_stack_by_name(self):
        self.mgr.create_stack("Ranked Team")
        stack = self.mgr.get_stack_by_name("Ranked Team")
        self.assertIsNotNone(stack)
        self.assertEqual(stack['stack_name'], "Ranked Team")

    def test_get_stack_by_name_not_found(self):
        result = self.mgr.get_stack_by_name("Nonexistent")
        self.assertIsNone(result)

    def test_get_all_stacks(self):
        self.mgr.create_stack("Stack A")
        self.mgr.create_stack("Stack B")
        stacks = self.mgr.get_all_stacks()
        self.assertEqual(len(stacks), 2)

    def test_update_stack_name(self):
        stack_id = self.mgr.create_stack("Old Name")
        self.mgr.update_stack(stack_id, name="New Name")
        stack = self.mgr.get_stack(stack_id)
        self.assertEqual(stack['stack_name'], "New Name")

    def test_update_nonexistent_stack_raises(self):
        with self.assertRaises(ValueError):
            self.mgr.update_stack(999, name="Fail")

    def test_delete_stack(self):
        stack_id = self.mgr.create_stack("Delete Me")
        self.mgr.delete_stack(stack_id)
        self.assertIsNone(self.mgr.get_stack(stack_id))

    def test_delete_nonexistent_stack_raises(self):
        with self.assertRaises(ValueError):
            self.mgr.delete_stack(999)

    # --- Member management ---

    def test_add_player_to_stack(self):
        stack_id = self.mgr.create_stack("Team")
        self.mgr.add_player_to_stack(stack_id, "PlayerA")
        members = self.mgr.get_stack_members(stack_id)
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]['username'], "PlayerA")

    def test_add_duplicate_player_raises(self):
        stack_id = self.mgr.create_stack("Team")
        self.mgr.add_player_to_stack(stack_id, "PlayerA")
        with self.assertRaises(ValueError):
            self.mgr.add_player_to_stack(stack_id, "PlayerA")

    def test_add_unknown_player_raises(self):
        stack_id = self.mgr.create_stack("Team")
        with self.assertRaises(ValueError):
            self.mgr.add_player_to_stack(stack_id, "NonexistentPlayer")

    def test_remove_player_from_stack(self):
        stack_id = self.mgr.create_stack("Team")
        self.mgr.add_player_to_stack(stack_id, "PlayerA")
        self.mgr.remove_player_from_stack(stack_id, "PlayerA")
        self.assertEqual(self.mgr.get_stack_size(stack_id), 0)

    def test_get_stack_size(self):
        stack_id = self.mgr.create_stack("Team")
        self.mgr.add_player_to_stack(stack_id, "PlayerA")
        self.mgr.add_player_to_stack(stack_id, "PlayerB")
        self.assertEqual(self.mgr.get_stack_size(stack_id), 2)

    # --- Stack type operations ---

    def test_create_quick_stack(self):
        stack_id = self.mgr.create_quick_stack(["PlayerA", "PlayerB", "PlayerC"])
        stack = self.mgr.get_stack(stack_id)
        self.assertEqual(stack['stack_type'], 'quick')
        self.assertEqual(self.mgr.get_stack_size(stack_id), 3)

    def test_quick_stack_needs_2_players(self):
        with self.assertRaises(ValueError):
            self.mgr.create_quick_stack(["PlayerA"])

    def test_quick_stack_unknown_player_raises(self):
        with self.assertRaises(ValueError):
            self.mgr.create_quick_stack(["PlayerA", "GhostPlayer"])

    def test_build_tagged_stack(self):
        stack_id = self.mgr.build_tagged_stack('teammate')
        stack = self.mgr.get_stack(stack_id)
        self.assertEqual(stack['stack_type'], 'tagged')
        # Should include all players since tagging isn't implemented yet
        size = self.mgr.get_stack_size(stack_id)
        self.assertGreaterEqual(size, 2)

    def test_cleanup_quick_stacks(self):
        self.mgr.create_quick_stack(["PlayerA", "PlayerB"])
        self.mgr.create_quick_stack(["PlayerC", "PlayerD"])
        self.mgr.create_stack("Permanent")

        self.mgr.cleanup_quick_stacks()

        stacks = self.mgr.get_all_stacks()
        for s in stacks:
            self.assertNotEqual(s['stack_type'], 'quick')

    # --- Validation ---

    def test_validate_stack_valid(self):
        stack_id = self.mgr.create_stack("Valid Team")
        self.mgr.add_player_to_stack(stack_id, "PlayerA")
        self.mgr.add_player_to_stack(stack_id, "PlayerB")
        result = self.mgr.validate_stack(stack_id)
        self.assertTrue(result['valid'])
        self.assertEqual(result['size'], 2)
        self.assertEqual(len(result['missing_players']), 0)

    def test_validate_empty_stack(self):
        stack_id = self.mgr.create_stack("Empty")
        result = self.mgr.validate_stack(stack_id)
        self.assertFalse(result['valid'])
        self.assertIn("Stack is empty", result['warnings'][0])

    def test_validate_nonexistent_stack(self):
        result = self.mgr.validate_stack(999)
        self.assertFalse(result['valid'])


if __name__ == '__main__':
    unittest.main()
