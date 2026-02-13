# src/parser.py

import re
from typing import Dict, Any, Optional


class R6TrackerParser:
    """
    Parse stats from copy-pasted R6 Tracker season drawer.
    
    Handles the exact format from R6 Tracker with sections:
    - Game
    - Rounds
    - Combat
    - Rounds - Clutches
    - Multikills
    - Ranked
    - Uncategorized
    """
    
    def __init__(self):
        # Section headers as they appear in the paste
        self.section_headers = {
            'Game': 'game',
            'Rounds': 'rounds',
            'Combat': 'combat',
            'Rounds - Clutches': 'clutches',
            'Multikills': 'multikills',
            'Ranked': 'ranked',
            'Uncategorized': 'uncategorized'
        }
    
    def parse(self, pasted_text: str) -> Dict[str, Any]:
        """
        Parse pasted R6 Tracker stats into structured dictionary.
        
        Args:
            pasted_text: Raw text copied from R6 Tracker season drawer
            
        Returns:
            Dictionary with sections: game, rounds, combat, clutches, multikills, ranked
            
        Raises:
            ValueError: If parsing fails or required data is missing
        """
        # Initialize result structure
        result = {
            'game': {},
            'rounds': {},
            'combat': {},
            'clutches': {},
            'multikills': {},
            'ranked': {},
            'uncategorized': {}
        }
        
        # Split into lines and clean
        lines = [line.strip() for line in pasted_text.strip().split('\n') if line.strip()]
        
        # State machine for parsing
        current_section = None
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Check if this is a section header
            if line in self.section_headers:
                current_section = self.section_headers[line]
                i += 1
                continue
            
            # If we're in a section and this looks like a stat name
            if current_section and i + 1 < len(lines):
                stat_name = line
                stat_value_raw = lines[i + 1]
                
                # Try to parse this as a stat
                parsed_value = self._parse_stat_value(stat_name, stat_value_raw, current_section)
                
                if parsed_value is not None:
                    # Normalize stat name to snake_case key
                    stat_key = self._normalize_stat_name(stat_name, current_section)
                    result[current_section][stat_key] = parsed_value
                    i += 2  # Skip both stat name and value
                    continue
            
            # If we didn't parse anything, just move forward
            i += 1
        
        # Validate that we got the essential data
        self._validate_parsed_data(result)
        
        return result
    
    def _parse_stat_value(self, stat_name: str, stat_value_raw: str, section: str) -> Optional[Any]:
        """
        Parse a stat value based on its name and expected type.
        
        Args:
            stat_name: Name of the stat (e.g., "K/D", "Win %")
            stat_value_raw: Raw value as string (e.g., "1.38", "52.1%", "288h")
            section: Which section we're in
            
        Returns:
            Parsed value (int, float, or None if parsing failed)
        """
        try:
            # Handle percentages
            if '%' in stat_value_raw:
                return float(stat_value_raw.replace('%', '').replace(',', ''))
            
            # Handle time (e.g., "288h")
            if 'h' in stat_value_raw.lower():
                return self._parse_time(stat_value_raw)
            
            # Handle numbers with commas (e.g., "3,869")
            if ',' in stat_value_raw:
                stat_value_raw = stat_value_raw.replace(',', '')
            
            # Try to determine if int or float
            if '.' in stat_value_raw:
                return float(stat_value_raw)
            else:
                return int(stat_value_raw)
                
        except ValueError:
            # If parsing fails, return None
            return None
    
    def _parse_time(self, time_str: str) -> float:
        """
        Parse time string like '288h' or '2h 30m' to hours as float.
        
        Args:
            time_str: Time string from R6 Tracker
            
        Returns:
            Hours as float
        """
        time_str = time_str.lower().replace(' ', '')
        
        hours = 0.0
        
        # Extract hours
        if 'h' in time_str:
            h_match = re.search(r'(\d+)h', time_str)
            if h_match:
                hours += float(h_match.group(1))
        
        # Extract minutes
        if 'm' in time_str:
            m_match = re.search(r'(\d+)m', time_str)
            if m_match:
                hours += float(m_match.group(1)) / 60.0
        
        return hours
    
    def _normalize_stat_name(self, stat_name: str, section: str) -> str:
        """
        Convert stat name to consistent snake_case key.
        
        Args:
            stat_name: Display name from R6 Tracker
            section: Current section
            
        Returns:
            Normalized key for dictionary
        """
        # Special mappings for specific stats
        mappings = {
            # Game section
            'Abandons': 'abandons',
            'Losses': 'losses',
            'Match Win %': 'match_win_pct',
            'Matches': 'matches',
            'Score': 'score',
            'Time Played': 'time_played_hours',
            'Wins': 'wins',
            
            # Rounds section (note: Losses/Wins appear here too)
            'Disconnected': 'disconnected',
            'Rounds Played': 'rounds_played',
            'Win %': 'win_pct',
            
            # Combat section
            'Assists': 'assists',
            'Assists/Round': 'assists_per_round',
            'Deaths': 'deaths',
            'Deaths/Round': 'deaths_per_round',
            'ESR': 'esr',
            'First Bloods': 'first_bloods',
            'First Deaths': 'first_deaths',
            'Headshots': 'headshots',
            'Headshots/Round': 'headshots_per_round',
            'HS %': 'hs_pct',
            'K/D': 'kd',
            'Kills': 'kills',
            'Kills/Game': 'kills_per_game',
            'Kills/Round': 'kills_per_round',
            'TKs': 'teamkills',
            
            # Clutches section
            'Clutches': 'total',
            'Clutches 1v1': '1v1',
            'Clutches 1v2': '1v2',
            'Clutches 1v3': '1v3',
            'Clutches 1v4': '1v4',
            'Clutches 1v5': '1v5',
            'Clutches Lost': 'lost_total',
            'Clutches Lost 1v1': 'lost_1v1',
            'Clutches Lost 1v2': 'lost_1v2',
            'Clutches Lost 1v3': 'lost_1v3',
            'Clutches Lost 1v4': 'lost_1v4',
            'Clutches Lost 1v5': 'lost_1v5',
            
            # Multikills section
            'Aces': 'aces',
            'Kills 1K': '1k',
            'Kills 2K': '2k',
            'Kills 3K': '3k',
            'Kills 4K': '4k',
            
            # Ranked section
            'Max Rank': 'max_rank',
            'Rank': 'current_rank',
            'Top Rank Position': 'top_rank_position',
            
            # Uncategorized section
            'Max Rank Points': 'max_rank_points',
            'Rank Points': 'rank_points',
            'TRN Elo': 'trn_elo',
        }
        
        # Handle context-dependent duplicates (Wins/Losses appear in Game and Rounds)
        if stat_name in ['Losses', 'Wins'] and section == 'rounds':
            return 'rounds_' + stat_name.lower()
        
        return mappings.get(stat_name, self._snake_case(stat_name))
    
    def _snake_case(self, text: str) -> str:
        """Convert any text to snake_case."""
        # Replace special characters and spaces with underscore
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', '_', text)
        return text.lower()
    
    def _validate_parsed_data(self, result: Dict[str, Any]) -> None:
        """
        Validate that essential data was parsed.
        
        Args:
            result: Parsed stats dictionary
            
        Raises:
            ValueError: If critical data is missing
        """
        # Check that we have data in key sections
        required_sections = ['game', 'combat', 'clutches']
        
        for section in required_sections:
            if not result[section]:
                raise ValueError(f"Failed to parse {section} section - no data found")
        
        # Check for specific critical stats
        critical_stats = [
            ('game', 'matches'),
            ('game', 'wins'),
            ('combat', 'kills'),
            ('combat', 'deaths'),
            ('combat', 'kd')
        ]
        
        for section, stat in critical_stats:
            if stat not in result[section]:
                raise ValueError(f"Missing critical stat: {section}.{stat}")


def pretty_print_stats(stats: Dict[str, Any]) -> None:
    """
    Pretty print parsed stats for debugging.
    
    Args:
        stats: Parsed stats dictionary
    """
    print("\n" + "="*60)
    print("PARSED R6 TRACKER STATS")
    print("="*60)
    
    sections = [
        ('GAME', 'game'),
        ('ROUNDS', 'rounds'),
        ('COMBAT', 'combat'),
        ('CLUTCHES', 'clutches'),
        ('MULTIKILLS', 'multikills'),
        ('RANKED', 'ranked'),
        ('UNCATEGORIZED', 'uncategorized')
    ]
    
    for title, key in sections:
        if stats.get(key):
            print(f"\n{title}:")
            print("-" * 60)
            
            for stat_key, stat_value in stats[key].items():
                # Format based on type
                if isinstance(stat_value, float):
                    if stat_value < 10:
                        formatted = f"{stat_value:.2f}"
                    else:
                        formatted = f"{stat_value:.0f}"
                else:
                    formatted = str(stat_value)
                
                print(f"  {stat_key:<25} {formatted:>10}")
    
    print("\n" + "="*60)


def test_parser():
    """Test the parser with sample data."""
    
    sample_data = """
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
    
    parser = R6TrackerParser()
    
    try:
        result = parser.parse(sample_data)
        
        print("✅ Parse successful!")
        
        # Use pretty print
        pretty_print_stats(result)
        
        print("\n" + "="*50)
        print("VALIDATION:")
        print("="*50)
        
        # Validate some key stats
        assert result['game']['matches'] == 117
        assert result['game']['match_win_pct'] == 52.1
        assert result['game']['time_played_hours'] == 288.0
        assert result['combat']['kd'] == 1.38
        assert result['combat']['hs_pct'] == 60.0
        assert result['clutches']['1v1'] == 16
        assert result['multikills']['aces'] == 2
        assert result['uncategorized']['max_rank_points'] == 3869
        
        print("✅ All validations passed!")
        
        return result
        
    except Exception as e:
        print(f"❌ Parse failed: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    test_parser()