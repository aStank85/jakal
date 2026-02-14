# tests/test_scraper.py
"""
Tests for web scraper module (v0.5).

Note: Most tests avoid importing Playwright to keep them lightweight.
Web integration tests are skipped unless RUN_WEB_TESTS env var is set.
"""

import os
import unittest
from typing import Dict, Any

from src.scraper.validation import is_valid_snapshot
from src.scraper.drawer import normalize_drawer_text, slice_from_game_section
from src.parser import R6TrackerParser


class TestValidation(unittest.TestCase):
    """Test snapshot validation logic."""

    def test_valid_snapshot_passes(self):
        """Test that valid snapshot passes validation."""
        stats = {
            'rounds': {'rounds_played': 702},
            'combat': {'kills': 680},
            'game': {'matches': 117, 'time_played_hours': 288.0}
        }

        is_valid, warnings = is_valid_snapshot(stats, min_rounds=10)

        self.assertTrue(is_valid)
        self.assertEqual(len([w for w in warnings if 'REJECTED' in w]), 0)

    def test_hard_fail_rounds_played_zero(self):
        """Test hard-fail when rounds_played is 0."""
        stats = {
            'rounds': {'rounds_played': 0},
            'combat': {'kills': 100},
            'game': {'matches': 50, 'time_played_hours': 10.0}
        }

        is_valid, warnings = is_valid_snapshot(stats)

        self.assertFalse(is_valid)
        self.assertTrue(any('rounds_played is 0' in w for w in warnings))

    def test_hard_fail_both_matches_and_time_zero(self):
        """Test hard-fail when both matches and time_played_hours are 0."""
        stats = {
            'rounds': {'rounds_played': 100},
            'combat': {'kills': 50},
            'game': {'matches': 0, 'time_played_hours': 0}
        }

        is_valid, warnings = is_valid_snapshot(stats)

        self.assertFalse(is_valid)
        self.assertTrue(any('Both matches and time_played_hours are 0' in w for w in warnings))

    def test_soft_validation_low_rounds_with_missing_fields(self):
        """Test soft validation rejects low rounds + missing fields."""
        stats = {
            'rounds': {'rounds_played': 5},  # Below threshold
            'combat': {'kills': 0},  # Missing
            'game': {'matches': 0, 'time_played_hours': 1.0}  # One missing
        }

        is_valid, warnings = is_valid_snapshot(stats, min_rounds=10)

        self.assertFalse(is_valid)
        self.assertTrue(any('REJECTED' in w for w in warnings))

    def test_soft_validation_passes_with_enough_data(self):
        """Test soft validation passes if enough data present."""
        stats = {
            'rounds': {'rounds_played': 8},  # Below threshold but...
            'combat': {'kills': 15},  # Has kills
            'game': {'matches': 5, 'time_played_hours': 2.0}  # Has both
        }

        is_valid, warnings = is_valid_snapshot(stats, min_rounds=10)

        # Should pass because kills, matches, time_played all present
        self.assertTrue(is_valid)

    def test_validation_handles_missing_sections(self):
        """Test validation handles missing sections gracefully."""
        stats = {
            'rounds': {},  # Empty section
            'combat': {'kills': 0},
            'game': {}
        }

        is_valid, warnings = is_valid_snapshot(stats)

        self.assertFalse(is_valid)


class TestTextProcessing(unittest.TestCase):
    """Test text normalization and extraction."""

    def test_normalize_drawer_text_removes_empty_lines(self):
        """Test normalization removes empty lines."""
        raw = """
        Game

        Matches
        117

        Wins
        61
        """

        normalized = normalize_drawer_text(raw)

        self.assertNotIn('\n\n', normalized)
        self.assertIn('Game', normalized)
        self.assertIn('Matches', normalized)

    def test_normalize_preserves_content(self):
        """Test normalization preserves actual content."""
        raw = "Game\nMatches\n117\nWins\n61"

        normalized = normalize_drawer_text(raw)

        self.assertEqual(normalized, raw)

    def test_slice_from_game_section_extracts_drawer(self):
        """Test slicing extracts drawer portion from full page."""
        full_page = """
        Header stuff
        Navigation
        Game
        Matches
        117
        Wins
        61
        Privacy Policy
        Footer
        """

        sliced = slice_from_game_section(full_page)

        self.assertTrue(sliced.startswith("Game"))
        self.assertIn("Matches", sliced)
        self.assertNotIn("Privacy Policy", sliced)
        self.assertNotIn("Footer", sliced)

    def test_slice_raises_on_missing_game_section(self):
        """Test slice raises ValueError if Game section not found."""
        full_page = "Some random text\nNo game section here\nPrivacy Policy"

        with self.assertRaises(ValueError) as context:
            slice_from_game_section(full_page)

        self.assertIn("Could not find 'Game' section", str(context.exception))


class TestParserIntegration(unittest.TestCase):
    """Test integration with parser using fixtures."""

    def test_parse_drawer_sample_fixture(self):
        """Test parsing of sample drawer fixture."""
        fixture_path = os.path.join(
            os.path.dirname(__file__),
            'fixtures',
            'drawer_sample.txt'
        )

        with open(fixture_path, 'r', encoding='utf-8') as f:
            drawer_text = f.read()

        # Parse using existing parser
        parser = R6TrackerParser()
        stats = parser.parse(drawer_text)

        # Verify key stats
        self.assertEqual(stats['game']['matches'], 117)
        self.assertEqual(stats['game']['wins'], 61)
        self.assertEqual(stats['rounds']['rounds_played'], 702)
        self.assertEqual(stats['combat']['kills'], 680)
        self.assertAlmostEqual(stats['combat']['kd'], 1.38, places=2)

        # Validate it would pass validation
        is_valid, warnings = is_valid_snapshot(stats)
        self.assertTrue(is_valid)

    def test_parse_blocked_fixture_fails_validation(self):
        """Test that blocked page fixture fails validation."""
        fixture_path = os.path.join(
            os.path.dirname(__file__),
            'fixtures',
            'drawer_blocked.txt'
        )

        with open(fixture_path, 'r', encoding='utf-8') as f:
            drawer_text = f.read()

        # Parse (should work)
        parser = R6TrackerParser()
        stats = parser.parse(drawer_text)

        # Should fail validation
        is_valid, warnings = is_valid_snapshot(stats)
        self.assertFalse(is_valid)
        self.assertTrue(any('REJECTED' in w for w in warnings))


@unittest.skipUnless(os.getenv('RUN_WEB_TESTS'), "Skipping web integration test")
class TestWebIntegration(unittest.TestCase):
    """
    Web integration tests (skipped by default).

    Run with: RUN_WEB_TESTS=1 pytest tests/test_scraper.py
    """

    def test_scrape_live_profile(self):
        """Test scraping against live R6 Tracker (integration test)."""
        # Import only when running web tests
        from src.scraper import scrape_profile_drawer

        # Scrape a known profile (this will actually hit the web)
        stats, meta = scrape_profile_drawer(
            username="SaucedZyn",
            platform="ubi",
            season="Y10S4",
            headed=False,  # Headless for CI
            min_rounds=10
        )

        # Verify we got data
        self.assertIsNotNone(stats)
        self.assertIn('url', meta)
        self.assertIn('timestamp', meta)

        # Verify stats structure
        self.assertIn('game', stats)
        self.assertIn('combat', stats)

        # Verify validation passed
        self.assertEqual(len([w for w in meta['warnings'] if 'REJECTED' in w]), 0)


if __name__ == '__main__':
    unittest.main()
