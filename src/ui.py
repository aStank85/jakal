# src/ui.py

from typing import List, Dict, Any
import sys
import re
from datetime import datetime

from src.thresholds import MIN_RELIABLE_ROUNDS_PER_HOUR


class TerminalUI:
    """Simple terminal-based UI."""

    @staticmethod
    def _format_metric(value: Any, decimals: int = 2, suppressed: bool = False) -> str:
        if suppressed or value is None:
            return 'N/A'
        if isinstance(value, float):
            return f'{value:.{decimals}f}'
        return str(value)

    def show_menu(self) -> str:
        """Show main menu and get validated user choice."""
        print("\n" + "="*50)
        print("JAKAL - R6 Stats Analyzer")
        print("="*50)
        print("1. Add new stats snapshot")
        print("2. View all players")
        print("3. Compare players")
        print("4. View player details")
        print("5. Exit")
        print("="*50)

        while True:
            choice = input("Choose an option (1-5): ").strip()
            if choice in ['1', '2', '3', '4', '5']:
                return choice
            print("Error: Please enter a number between 1 and 5")

    def get_paste_input(self) -> str:
        """Get pasted stats from user."""
        print("\n" + "-"*50)
        print("Paste your R6 Tracker stats below.")
        print("When done, type 'END' on a new line and press Enter.")
        print("-"*50)

        lines = []
        while True:
            line = input()
            if line.strip().upper() == 'END':
                break
            lines.append(line)

        return '\n'.join(lines)

    def get_metadata(self) -> Dict[str, str]:
        """Get player metadata with validation."""
        print("\n" + "-"*50)
        now = datetime.now()
        date = now.strftime("%Y-%m-%d")
        time = now.strftime("%H:%M:%S")

        # Username validation
        while True:
            username = input("Enter player username: ").strip()
            if username and len(username) >= 2:
                break
            print("Error: Username must be at least 2 characters long")

        # Device tag validation
        device_aliases = {
            'pc': 'pc',
            'xbox': 'xbox',
            'xb': 'xbox',
            'playstation': 'playstation',
            'ps': 'playstation',
            'psn': 'playstation'
        }
        while True:
            device_input = input(
                "Enter device tag [pc/xbox/playstation] (default: pc): "
            ).strip().lower()
            if not device_input:
                device_tag = 'pc'
                break
            if device_input in device_aliases:
                device_tag = device_aliases[device_input]
                break
            print("Error: Device must be one of: pc, xbox, playstation")

        print(f"Snapshot timestamp auto-set: {date} {time}")

        # Season validation
        while True:
            season = input("Enter season (e.g., Y10S4) or press Enter for default: ").strip()
            if not season:
                season = "Y10S4"
                break

            # Validate season format (e.g., Y10S4)
            if re.match(r'^Y\d{1,2}S\d{1}$', season):
                break
            else:
                print("Error: Invalid season format. Use format like Y10S4")

        return {
            'username': username,
            'device_tag': device_tag,
            'date': date,
            'time': time,
            'season': season
        }

    def show_players(self, players: List[Dict]):
        """Display list of players."""
        print("\n" + "="*50)
        print("Players in Database:")
        print("="*50)
        for i, player in enumerate(players, 1):
            username = player['username']
            tag = player.get('tag', 'untagged')
            device_tag = player.get('device_tag', 'pc')
            print(f"{i}. {username} [{device_tag}] [{tag}]")
        print("="*50)

    def select_players_for_comparison(self, all_players: List[Dict]) -> List[str]:
        """Let user select players to compare with validation."""
        print("\n" + "-"*50)
        print("Select players to compare (enter numbers separated by commas)")
        print("Example: 1,3,5")
        print("-"*50)

        self.show_players(all_players)

        while True:
            selection = input("\nYour selection: ").strip()

            # Validate format (numbers and commas)
            if not re.match(r'^[\d,\s]+$', selection):
                print("Error: Please enter numbers separated by commas (e.g., 1,3,5)")
                continue

            try:
                # Parse indices
                indices = [int(x.strip()) - 1 for x in selection.split(',') if x.strip()]

                # Validate all indices are in range
                invalid_indices = [i+1 for i in indices if i < 0 or i >= len(all_players)]
                if invalid_indices:
                    print(f"Error: Invalid player number(s): {', '.join(map(str, invalid_indices))}")
                    print(f"Please select numbers between 1 and {len(all_players)}")
                    continue

                # Check for duplicates
                if len(indices) != len(set(indices)):
                    print("Error: You selected the same player multiple times")
                    continue

                # Check minimum selection
                if len(indices) < 2:
                    print("Error: Please select at least 2 players to compare")
                    continue

                # All validations passed - return usernames
                selected = [all_players[i]['username'] for i in indices]
                return selected

            except ValueError:
                print("Error: Invalid input. Please enter numbers separated by commas (e.g., 1,3,5)")
                continue

    def show_comparison(self, comparison: Dict[str, Any]):
        """Display comparison results."""
        print("\n" + "="*50)
        print("PLAYER COMPARISON")
        print("="*50)

        # Player header
        print("\nPlayers:")
        for i, player in enumerate(comparison['players']):
            role = player['primary_role']
            print(f"  {i+1}. {player['username']} ({role}) - {player['snapshot_date']}")

        print("\n" + "-"*50)
        print(f"{'Stat':<25} ", end='')
        for i in range(len(comparison['players'])):
            print(f"{'P'+str(i+1):<12}", end='')
        print("Winner")
        print("-"*50)

        # Stats comparison
        for stat in comparison['stats']:
            print(f"{stat['name']:<25} ", end='')

            for value in stat['values']:
                formatted = self._format_metric(value)
                print(f"{formatted:<12}", end='')

            if stat['winner_index'] is not None:
                winner_num = stat['winner_index'] + 1
                print(f"P{winner_num}")
            else:
                print("-")

        print("-"*50)
        print("\nOverall Advantages:")
        for i, count in comparison['winners'].items():
            username = comparison['players'][i]['username']
            print(f"  {username}: {count} stats")

        print("="*50)

    def show_error(self, message: str):
        """Display error message."""
        print(f"\nERROR: {message}\n")

    def show_success(self, message: str):
        """Display success message."""
        print(f"\n{message}\n")

    def show_player_details(
        self,
        snapshot: Dict[str, Any],
        metrics: Dict[str, Any],
        insights: List[Dict[str, str]] = None
    ):
        """Display detailed player stats and metrics."""
        print("\n" + "="*50)
        print(f"PLAYER DETAILS: {snapshot['username']}")
        print("="*50)

        print(f"\nSnapshot Date: {snapshot['snapshot_date']}")
        if snapshot.get('snapshot_time'):
            print(f"Snapshot Time: {snapshot['snapshot_time']}")
        print(f"Device: {snapshot.get('device_tag', 'pc')}")
        print(f"Season: {snapshot.get('season', 'N/A')}")

        # Game Stats
        print("\n" + "-"*50)
        print("GAME STATS")
        print("-"*50)
        print(f"Matches:          {snapshot.get('matches', 0)}")
        print(f"Wins:             {snapshot.get('wins', 0)}")
        print(f"Losses:           {snapshot.get('losses', 0)}")
        print(f"Win %:            {snapshot.get('match_win_pct', 0):.1f}%")
        print(f"Time Played:      {snapshot.get('time_played_hours', 0):.1f}h")

        # Round Stats
        print("\n" + "-"*50)
        print("ROUND STATS")
        print("-"*50)
        print(f"Rounds Played:    {snapshot.get('rounds_played', 0)}")
        print(f"Round Wins:       {snapshot.get('rounds_wins', 0)}")
        print(f"Round Losses:     {snapshot.get('rounds_losses', 0)}")
        print(f"Round Win %:      {snapshot.get('rounds_win_pct', 0):.1f}%")

        # Combat Stats
        print("\n" + "-"*50)
        print("COMBAT STATS")
        print("-"*50)
        print(f"K/D:              {snapshot.get('kd', 0):.2f}")
        print(f"Kills:            {snapshot.get('kills', 0)}")
        print(f"Deaths:           {snapshot.get('deaths', 0)}")
        print(f"Assists:          {snapshot.get('assists', 0)}")
        print(f"Kills/Round:      {snapshot.get('kills_per_round', 0):.2f}")
        print(f"Deaths/Round:     {snapshot.get('deaths_per_round', 0):.2f}")
        print(f"Assists/Round:    {snapshot.get('assists_per_round', 0):.2f}")
        print(f"HS %:             {snapshot.get('hs_pct', 0):.1f}%")
        print(f"First Bloods:     {snapshot.get('first_bloods', 0)}")
        print(f"First Deaths:     {snapshot.get('first_deaths', 0)}")

        # Derived Metrics
        print("\n" + "-"*50)
        print("DERIVED METRICS")
        print("-"*50)
        print(f"Entry Efficiency:         {metrics.get('entry_efficiency', 0):.2f}")
        print(f"Aggression Score:         {metrics.get('aggression_score', 0):.2f}")
        print(f"Clutch Attempt Rate:      {metrics.get('clutch_attempt_rate', 0):.2f}")
        print(f"1v1 Clutch Success:       {metrics.get('clutch_1v1_success', 0):.2f}")
        print(f"1v2 Clutch Success:       {metrics.get('clutch_1v2_success', 0):.2f}")
        print(f"1v3 Clutch Success:       {metrics.get('clutch_1v3_success', 0):.2f}")
        print(f"1v4 Clutch Success:       {metrics.get('clutch_1v4_success', 0):.2f}")
        print(f"1v5 Clutch Success:       {metrics.get('clutch_1v5_success', 0):.2f}")
        print(f"Overall Clutch Success:   {metrics.get('overall_clutch_success', 0):.2f}")
        print(f"Clutch Dropoff Rate:      {metrics.get('clutch_dropoff_rate', 0):.2f}")
        print(f"High Pressure Attempts:   {metrics.get('high_pressure_attempts', 0)}")
        print(f"High Pressure Wins:       {metrics.get('high_pressure_wins', 0)}")
        print(f"Disadv Attempt Share:     {metrics.get('disadv_attempt_share', 0):.2f}")
        print(f"Extreme Attempts (1v4+):  {metrics.get('extreme_attempts', 0)}")
        print(f"Teamplay Index:           {metrics.get('teamplay_index', 0):.2f}")
        print(f"Impact Rating:            {metrics.get('impact_rating', 0):.2f}")
        print(f"Rounds Per Hour:          {self._format_metric(metrics.get('rounds_per_hour', 0.0), suppressed=metrics.get('time_played_unreliable', False))}")
        print(f"Wins Per Hour:            {self._format_metric(metrics.get('wins_per_hour'), suppressed=metrics.get('time_played_unreliable', False))}")
        print(f"K/D Win Gap:              {metrics.get('kd_win_gap', 0):.2f}")

        print("\n" + "-"*50)
        print("DATA QUALITY")
        print("-"*50)
        time_scope = "UNRELIABLE" if metrics.get('time_played_unreliable', False) else "OK"
        rounds_per_hour = self._format_metric(metrics.get('rounds_per_hour', 0.0))
        print(f"Time Scope:       {time_scope} (Rounds/Hour = {rounds_per_hour})")

        clutch_mismatch_parts = []
        if metrics.get('clutch_totals_mismatch', False):
            clutch_mismatch_parts.append("total")
        if metrics.get('clutch_lost_totals_mismatch', False):
            clutch_mismatch_parts.append("lost_total")

        if clutch_mismatch_parts:
            print(f"Clutch Totals:    MISMATCH ({', '.join(clutch_mismatch_parts)})")
        else:
            print("Clutch Totals:    OK")
        if metrics.get('time_played_unreliable', False):
            print(
                f"WARNING: Time-played metrics unreliable (Rounds/Hour < {MIN_RELIABLE_ROUNDS_PER_HOUR:.1f}); per-hour rates suppressed."
            )

        # Role Classification
        print("\n" + "-"*50)
        print("ROLE CLASSIFICATION")
        print("-"*50)
        primary = metrics.get('primary_role', 'Unknown')
        primary_conf = metrics.get('primary_confidence', 0)
        print(f"Primary Role:     {primary} ({primary_conf:.1f})")

        secondary = metrics.get('secondary_role')
        if secondary:
            secondary_conf = metrics.get('secondary_confidence', 0)
            print(f"Secondary Role:   {secondary} ({secondary_conf:.1f})")
        else:
            print("Secondary Role:   None")

        # Role Scores
        print("\n" + "-"*50)
        print("ROLE SCORES")
        print("-"*50)
        print(f"Fragger:          {metrics.get('fragger_score', 0):.1f}")
        print(f"Entry:            {metrics.get('entry_score', 0):.1f}")
        print(f"Support:          {metrics.get('support_score', 0):.1f}")
        print(f"Anchor:           {metrics.get('anchor_score', 0):.1f}")
        print(f"Clutch Specialist:{metrics.get('clutch_specialist_score', 0):.1f}")
        print(f"Carry:            {metrics.get('carry_score', 0):.1f}")

        # Insight Generation
        print("\n" + "-"*50)
        print("INSIGHTS")
        print("-"*50)
        if insights:
            for idx, insight in enumerate(insights, 1):
                print(f"{idx}. [{insight.get('severity', 'info').upper()}] {insight.get('message', '')}")
                print(f"   Evidence: {insight.get('evidence', '')}")
                print(f"   Action:   {insight.get('action', '')}")
        else:
            print("1. [INFO] No major risk flags from current snapshot.")
            print(f"   Evidence: Primary Role: {metrics.get('primary_role', 'Unknown')}")
            print("   Action:   Keep collecting snapshots for trend-based insights.")

        print("="*50)








