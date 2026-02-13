# tests/test_parser.py

import pytest
from src.parser import R6TrackerParser


class TestR6TrackerParser:
    """Test suite for R6 Tracker stats parser."""

    @pytest.fixture
    def parser(self):
        """Create parser instance."""
        return R6TrackerParser()

    @pytest.fixture
    def sample_stats(self):
        """Sample R6 Tracker paste data."""
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

    def test_parse_game_section(self, parser, sample_stats):
        """Test parsing of Game section."""
        result = parser.parse(sample_stats)

        assert result['game']['matches'] == 117
        assert result['game']['wins'] == 61
        assert result['game']['losses'] == 56
        assert result['game']['match_win_pct'] == 52.1
        assert result['game']['time_played_hours'] == 288.0
        assert result['game']['abandons'] == 0

    def test_parse_rounds_section(self, parser, sample_stats):
        """Test parsing of Rounds section."""
        result = parser.parse(sample_stats)

        assert result['rounds']['rounds_played'] == 702
        assert result['rounds']['rounds_wins'] == 364
        assert result['rounds']['rounds_losses'] == 338
        assert result['rounds']['win_pct'] == 51.9
        assert result['rounds']['disconnected'] == 3

    def test_parse_combat_section(self, parser, sample_stats):
        """Test parsing of Combat section."""
        result = parser.parse(sample_stats)

        assert result['combat']['kills'] == 680
        assert result['combat']['deaths'] == 493
        assert result['combat']['kd'] == 1.38
        assert result['combat']['assists'] == 193
        assert result['combat']['hs_pct'] == 60.0
        assert result['combat']['first_bloods'] == 96
        assert result['combat']['first_deaths'] == 57
        assert result['combat']['teamkills'] == 11

    def test_parse_clutches_section(self, parser, sample_stats):
        """Test parsing of Clutches section."""
        result = parser.parse(sample_stats)

        assert result['clutches']['total'] == 28
        assert result['clutches']['1v1'] == 16
        assert result['clutches']['1v2'] == 7
        assert result['clutches']['1v3'] == 5
        assert result['clutches']['lost_total'] == 81
        assert result['clutches']['lost_1v1'] == 6

    def test_parse_multikills_section(self, parser, sample_stats):
        """Test parsing of Multikills section."""
        result = parser.parse(sample_stats)

        assert result['multikills']['aces'] == 2
        assert result['multikills']['4k'] == 18
        assert result['multikills']['3k'] == 44
        assert result['multikills']['2k'] == 121

    def test_parse_ranked_section(self, parser, sample_stats):
        """Test parsing of Ranked section."""
        result = parser.parse(sample_stats)

        assert result['ranked']['current_rank'] == 29
        assert result['ranked']['max_rank'] == 29

    def test_parse_uncategorized_section(self, parser, sample_stats):
        """Test parsing of Uncategorized section."""
        result = parser.parse(sample_stats)

        assert result['uncategorized']['rank_points'] == 3806
        assert result['uncategorized']['max_rank_points'] == 3869
        assert result['uncategorized']['trn_elo'] == 1229

    def test_parse_percentage_values(self, parser, sample_stats):
        """Test parsing of percentage values."""
        result = parser.parse(sample_stats)

        # Percentages should be parsed as floats without the % sign
        assert isinstance(result['game']['match_win_pct'], float)
        assert result['game']['match_win_pct'] == 52.1
        assert result['combat']['hs_pct'] == 60.0

    def test_parse_time_values(self, parser, sample_stats):
        """Test parsing of time values."""
        result = parser.parse(sample_stats)

        # Time should be converted to hours as float
        assert isinstance(result['game']['time_played_hours'], float)
        assert result['game']['time_played_hours'] == 288.0

    def test_parse_comma_separated_numbers(self, parser, sample_stats):
        """Test parsing of numbers with commas."""
        result = parser.parse(sample_stats)

        # Commas should be removed and parsed as integers
        assert result['uncategorized']['max_rank_points'] == 3869
        assert result['uncategorized']['rank_points'] == 3806

    def test_parse_invalid_data_raises_error(self, parser):
        """Test that invalid data raises ValueError."""
        invalid_data = "This is not valid stats data"

        with pytest.raises(ValueError):
            parser.parse(invalid_data)

    def test_parse_missing_critical_stats(self, parser):
        """Test that missing critical stats raises ValueError."""
        incomplete_data = """
Game
Matches
100
"""
        with pytest.raises(ValueError) as exc_info:
            parser.parse(incomplete_data)

        assert "Missing critical stat" in str(exc_info.value)

    def test_normalize_stat_name(self, parser):
        """Test stat name normalization."""
        assert parser._normalize_stat_name('K/D', 'combat') == 'kd'
        assert parser._normalize_stat_name('Win %', 'rounds') == 'win_pct'
        assert parser._normalize_stat_name('Wins', 'rounds') == 'rounds_wins'
        assert parser._normalize_stat_name('Wins', 'game') == 'wins'

    def test_parse_time_with_minutes(self, parser):
        """Test parsing time with hours and minutes."""
        # Test just hours
        assert parser._parse_time('288h') == 288.0

        # Test hours and minutes (if this format exists)
        assert parser._parse_time('2h 30m') == 2.5
        assert parser._parse_time('1h 15m') == 1.25


class TestParserEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_sections(self):
        """Test handling of empty sections."""
        parser = R6TrackerParser()

        # Should raise error if critical sections are empty
        data_with_empty_combat = """
Game
Matches
100
Wins
50
Combat
"""
        with pytest.raises(ValueError):
            parser.parse(data_with_empty_combat)

    def test_duplicate_stat_handling(self):
        """Test that context-dependent duplicates are handled correctly."""
        parser = R6TrackerParser()

        data = """
Game
Wins
61
Losses
56
Rounds
Wins
364
Losses
338
Combat
K/D
1.38
Kills
680
Deaths
493
Rounds - Clutches
Clutches
28
Multikills
Aces
2
Ranked
Rank
29
"""
        result = parser.parse(data)

        # Game section should have 'wins', Rounds should have 'rounds_wins'
        assert result['game']['wins'] == 61
        assert result['rounds']['rounds_wins'] == 364
