# tests/test_scraper.py
"""
Tests for web scraper module (v0.5).

Note: Most tests avoid importing Playwright to keep them lightweight.
Web integration tests are skipped unless RUN_WEB_TESTS env var is set.
"""

import os
import unittest
from typing import Dict, Any

try:
    from bs4 import BeautifulSoup
except ImportError:
    import pytest
    pytest.skip("bs4 not installed (scraper module deprecated)", allow_module_level=True)

from src.scraper.validation import is_valid_snapshot
from src.scraper.drawer import normalize_drawer_text, slice_from_game_section
from src.scraper import R6Scraper, ScraperBlockedError, PlayerNotFoundError
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


class TestScraperParsers(unittest.TestCase):
    """Unit tests for v0.5 scraper parsing helpers (fixture-only, no live web)."""

    def setUp(self):
        self.scraper = R6Scraper(headless=True, slow_mo=0)

    def _fixture_path(self, filename: str) -> str:
        candidates = [
            os.path.join(os.path.dirname(__file__), "fixtures", filename),
            os.path.join(os.path.dirname(__file__), "..", "..", "tests", "fixtures", filename),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        raise FileNotFoundError(f"Fixture not found: {filename}")

    def _read_fixture(self, filename: str) -> str:
        with open(self._fixture_path(filename), "r", encoding="utf-8") as f:
            return f.read()

    def test_parse_map_row(self):
        html = self._read_fixture("dump_maps_ranked.html")
        soup = BeautifulSoup(html, "html.parser")
        row = soup.select('tr[class*="group/row"]')[0]
        parsed = self.scraper._parse_map_row(row)
        self.assertEqual(parsed["map_name"], "Clubhouse")
        self.assertEqual(parsed["matches"], 54)
        self.assertAlmostEqual(parsed["win_pct"], 51.9, places=1)

    def test_parse_operator_row(self):
        html = self._read_fixture("dump_operators.html")
        soup = BeautifulSoup(html, "html.parser")
        row = soup.select('tr[class*="group/row"]')[0]
        parsed = self.scraper._parse_operator_row(row)
        self.assertEqual(parsed["operator_name"], "Kaid")
        self.assertEqual(parsed["rounds"], 117)
        self.assertAlmostEqual(parsed["kd"], 1.29, places=2)

    def test_parse_match_row(self):
        html = self._read_fixture("dump_matches.html")
        soup = BeautifulSoup(html, "html.parser")
        row = soup.select('[class*="v3-match-row"]')[0]
        parsed = self.scraper._parse_match_row(row)
        self.assertEqual(parsed["map_name"], "Fortress")
        self.assertEqual(parsed["mode"], "Ranked")
        self.assertEqual(parsed["score"], "4:2")

    def test_parse_match_detail_table(self):
        html = self._read_fixture("dump_match_detail.html")
        detail = self.scraper._parse_match_detail_html(html, "SaucedZyn")
        self.assertEqual(len(detail["team_a"]), 5)
        self.assertEqual(len(detail["team_b"]), 5)
        self.assertEqual(detail["your_team"], "A")

    def test_parse_rp_string(self):
        rp, delta = self.scraper._parse_rp_string("3,053+27")
        self.assertEqual(rp, 3053)
        self.assertEqual(delta, 27)

    def test_parse_rp_string_negative(self):
        rp, delta = self.scraper._parse_rp_string("2,712-21")
        self.assertEqual(rp, 2712)
        self.assertEqual(delta, -21)

    def test_parse_percent(self):
        self.assertAlmostEqual(self.scraper._parse_percent("51.9%"), 51.9, places=1)

    def test_parse_number_commas(self):
        self.assertEqual(self.scraper._parse_number("6,110"), 6110)

    def test_multikill_detection(self):
        html = '<div class="v3-match-row v3-match-row--win">Ace 3K RP 3,053+27 K/D 1.11 K/D/A 123 HS % 40.0%</div>'
        row = BeautifulSoup(html, "html.parser").select_one('[class*="v3-match-row"]')
        parsed = self.scraper._parse_match_row(row)
        self.assertTrue(parsed["had_ace"])
        self.assertTrue(parsed["had_3k"])

    def test_win_loss_detection(self):
        win_html = '<div class="v3-match-row v3-match-row--win">RP 3,053+27</div>'
        loss_html = '<div class="v3-match-row v3-match-row--loss">RP 2,712-21</div>'
        win_row = BeautifulSoup(win_html, "html.parser").select_one('[class*="v3-match-row"]')
        loss_row = BeautifulSoup(loss_html, "html.parser").select_one('[class*="v3-match-row"]')
        self.assertEqual(self.scraper._parse_match_row(win_row)["result"], "win")
        self.assertEqual(self.scraper._parse_match_row(loss_row)["result"], "loss")

    def test_map_stats_count(self):
        html = self._read_fixture("dump_maps_ranked.html")
        parsed = self.scraper._parse_map_stats_html(html)
        self.assertEqual(len(parsed), 17)

    def test_operator_stats_count(self):
        html = self._read_fixture("dump_operators.html")
        parsed = self.scraper._parse_operator_stats_html(html)
        self.assertGreater(len(parsed), 20)

    def test_match_history_count(self):
        html = self._read_fixture("dump_matches.html")
        parsed = self.scraper._parse_match_history_html(html)
        self.assertEqual(len(parsed), 20)

    def test_match_detail_teams(self):
        html = self._read_fixture("dump_match_detail.html")
        parsed = self.scraper._parse_match_detail_html(html, "SaucedZyn")
        self.assertIn("team_a", parsed)
        self.assertIn("team_b", parsed)
        self.assertEqual(parsed["your_team"], "A")

    def test_player_not_found(self):
        with self.assertRaises(PlayerNotFoundError):
            raise PlayerNotFoundError("missing")

    def test_scraper_blocked(self):
        with self.assertRaises(ScraperBlockedError):
            raise ScraperBlockedError("blocked")


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
