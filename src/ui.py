# src/ui.py

from typing import List, Dict, Any, Tuple
import sys


class TerminalUI:
    """Simple terminal-based UI."""

    @staticmethod
    def _player_username(player: Any) -> str:
        """Normalize player object to a display/selection username."""
        if isinstance(player, dict):
            return str(player.get('username', ''))
        return str(player)

    @staticmethod
    def _truncate_username(username: str, max_len: int = 14) -> str:
        """Truncate long usernames for fixed-width tables."""
        name = str(username or "")
        if len(name) <= max_len:
            return name
        return name[:max_len - 1] + "."

    def show_menu(self) -> str:
        """Show main menu and get user choice."""
        print("\n" + "=" * 50)
        print("JAKAL - R6 Stats Analyzer")
        print("=" * 50)
        print("1. Sync player (auto-scrape)")
        print("2. Add stats manually")
        print("3. Sync all players")
        print("4. View all players")
        print("5. Compare players")
        print("6. View player details")
        print("-" * 30)
        print("7. Stack Management")
        print("8. Analyze Stack")
        print("9. 5v5 Matchup Analysis")
        print("-" * 30)
        print("A. Player Analytics")
        print("B. Backfill match history")
        print("-" * 30)
        print("0. Exit")
        print("=" * 50)

        choice = input("Choose an option (0-9, A-B): ").strip()
        return choice.lower()

    def get_paste_input(self) -> str:
        """Get pasted stats from user."""
        print("\n" + "-" * 50)
        print("Paste your R6 Tracker stats below.")
        print("When done, type 'END' on a new line and press Enter.")
        print("-" * 50)

        lines = []
        while True:
            line = input()
            if line.strip().upper() == 'END':
                break
            lines.append(line)

        return '\n'.join(lines)

    def get_metadata(self) -> Dict[str, str]:
        """Get player metadata."""
        print("\n" + "-" * 50)
        username = input("Enter player username: ").strip()
        date = input("Enter date (YYYY-MM-DD) or press Enter for today: ").strip()
        time = input("Enter time (HH:MM) or press Enter to skip: ").strip()
        season = input("Enter season (e.g., Y10S4) or press Enter for default: ").strip()
        device_tag = input("Enter device tag (pc/xbox/playstation) or press Enter for pc: ").strip().lower()

        if not date:
            from datetime import datetime
            date = datetime.now().strftime("%Y-%m-%d")

        if not season:
            season = "Y10S4"

        if device_tag not in ("pc", "xbox", "playstation"):
            device_tag = "pc"

        return {
            'username': username,
            'date': date,
            'time': time if time else None,
            'season': season,
            'device_tag': device_tag
        }

    def show_players(self, players: List[Any]):
        """Display list of players."""
        print("\n" + "=" * 50)
        print("Players in Database:")
        print("=" * 50)
        for i, player in enumerate(players, 1):
            print(f"{i}. {self._player_username(player)}")
        print("=" * 50)

    def select_players_for_comparison(self, all_players: List[Any]) -> List[str]:
        """Let user select players to compare."""
        print("\n" + "-" * 50)
        print("Select players to compare (enter numbers separated by commas)")
        print("Example: 1,3,5")
        print("-" * 50)

        self.show_players(all_players)

        selection = input("\nYour selection: ").strip()
        indices = [int(x.strip()) - 1 for x in selection.split(',')]

        selected = [
            self._player_username(all_players[i])
            for i in indices
            if 0 <= i < len(all_players)
        ]
        return selected

    def show_comparison(self, comparison: Dict[str, Any]):
        """Display comparison results."""
        print("\n" + "=" * 50)
        print("PLAYER COMPARISON")
        print("=" * 50)

        # Player header
        print("\nPlayers:")
        for i, player in enumerate(comparison['players']):
            role = player['primary_role']
            print(f"  {i+1}. {player['username']} ({role}) - {player['snapshot_date']}")

        print("\n" + "-" * 50)
        print(f"{'Stat':<25} ", end='')
        for i in range(len(comparison['players'])):
            print(f"{'P'+str(i+1):<12}", end='')
        print("Winner")
        print("-" * 50)

        # Stats comparison
        for stat in comparison['stats']:
            print(f"{stat['name']:<25} ", end='')

            for value in stat['values']:
                if value is None:
                    print(f"{'-':<12}", end='')
                elif isinstance(value, float):
                    print(f"{value:<12.2f}", end='')
                else:
                    print(f"{value:<12}", end='')

            if stat['winner_index'] is not None:
                winner_num = stat['winner_index'] + 1
                print(f"P{winner_num}")
            else:
                print("-")

        print("-" * 50)
        print("\nOverall Advantages:")
        for i, count in comparison['winners'].items():
            username = comparison['players'][i]['username']
            print(f"  {username}: {count} stats")

        print("=" * 50)

    # --- Stack Management UI ---

    def show_stack_menu(self) -> str:
        """Show stack management submenu."""
        print("\n" + "=" * 50)
        print("STACK MANAGEMENT")
        print("=" * 50)
        print("1. Create named stack")
        print("2. View all stacks")
        print("3. Add player to stack")
        print("4. Remove player from stack")
        print("5. Build stack from tags (auto)")
        print("6. Quick stack (temporary)")
        print("7. Delete stack")
        print("8. Back")
        print("=" * 50)

        choice = input("Choose an option (1-8): ").strip()
        return choice

    def get_stack_name(self) -> str:
        """Prompt user for a stack name."""
        return input("Enter stack name: ").strip()

    def get_stack_description(self) -> str:
        """Prompt user for a stack description."""
        desc = input("Enter description (or press Enter to skip): ").strip()
        return desc if desc else None

    def select_stack(self, stacks: List[Dict]) -> int:
        """Let user select a stack from a list. Returns stack_id."""
        if not stacks:
            print("\nNo stacks found.")
            return -1

        print("\n" + "-" * 50)
        print("Available Stacks:")
        print("-" * 50)
        for i, stack in enumerate(stacks, 1):
            stype = f"[{stack['stack_type']}]" if stack['stack_type'] != 'named' else ''
            print(f"  {i}. {stack['stack_name']} {stype}")
        print("-" * 50)

        selection = input("Select stack (number): ").strip()
        try:
            idx = int(selection) - 1
            if 0 <= idx < len(stacks):
                return stacks[idx]['stack_id']
        except ValueError:
            pass

        print("Invalid selection.")
        return -1

    def select_players_for_stack(self, all_players: List[Any]) -> List[str]:
        """Let user select multiple players for a quick stack."""
        print("\n" + "-" * 50)
        print("Select players (enter numbers separated by commas)")
        print("Example: 1,2,3,4,5")
        print("-" * 50)

        self.show_players(all_players)

        selection = input("\nYour selection: ").strip()
        indices = [int(x.strip()) - 1 for x in selection.split(',')]
        return [
            self._player_username(all_players[i])
            for i in indices
            if 0 <= i < len(all_players)
        ]

    def select_player(self, all_players: List[Any]) -> str:
        """Let user select a single player. Returns username."""
        self.show_players(all_players)
        selection = input("\nSelect player (number): ").strip()
        try:
            idx = int(selection) - 1
            if 0 <= idx < len(all_players):
                return self._player_username(all_players[idx])
        except ValueError:
            pass
        return ""

    def show_stack_details(self, stack: Dict, members: List[Dict]) -> None:
        """Display stack info and its members."""
        print("\n" + "=" * 50)
        print(f"STACK: {stack['stack_name']}")
        if stack.get('description'):
            print(f"  {stack['description']}")
        print(f"  Type: {stack['stack_type']}  |  Members: {len(members)}")
        print("=" * 50)

        if members:
            print(f"  {'#':<4}{'Player':<20}{'Role Override':<15}")
            print("  " + "-" * 40)
            for i, m in enumerate(members, 1):
                role = m.get('role_override') or '-'
                print(f"  {i:<4}{m['username']:<20}{role:<15}")
        else:
            print("  (No members)")
        print("=" * 50)

    def show_all_stacks(self, stacks: List[Dict]) -> None:
        """Display all stacks."""
        print("\n" + "=" * 50)
        print("ALL STACKS")
        print("=" * 50)

        if not stacks:
            print("  No stacks created yet.")
        else:
            print(f"  {'#':<4}{'Name':<25}{'Type':<10}{'Created':<15}")
            print("  " + "-" * 55)
            for i, s in enumerate(stacks, 1):
                created = s.get('created_at', '')[:10]
                print(f"  {i:<4}{s['stack_name']:<25}{s['stack_type']:<10}{created:<15}")

        print("=" * 50)

    def show_team_analysis(self, analysis: Dict) -> None:
        """Display full team analysis."""
        stack = analysis['stack']
        members = analysis['members']

        print("\n" + "=" * 50)
        print(f"STACK ANALYSIS: {stack['stack_name']}")
        print("=" * 50)

        # Roster table
        print("\nROSTER")
        print("-" * 50)
        print(f"{'Player':<14}{'Role':<14}{'K/D':<7}{'Win%':<7}")
        print("-" * 50)
        for m in members:
            kd = m['snapshot'].get('kd', 0) or 0
            win = m['snapshot'].get('match_win_pct', 0) or 0
            print(f"{m['username']:<14}{m['role']:<14}{kd:<7.2f}{str(round(win, 1)) + '%':<7}")
        print("-" * 50)
        print(f"{'TEAM AVG':<28}{analysis['team_avg_kd']:<7.2f}{str(round(analysis['team_avg_win_pct'], 1)) + '%':<7}")

        # Composition
        print(f"\nCOMPOSITION SCORE: {analysis['composition_score']:.0f}/100")
        print("\nROLE COVERAGE")
        all_roles = ['Fragger', 'Entry', 'Support', 'Anchor', 'Clutch', 'Carry']
        for role in all_roles:
            if role in analysis['roles_covered']:
                print(f"  [x] {role}")
            else:
                print(f"  [ ] {role}")

        # Strengths
        if analysis['team_strengths']:
            print("\nTEAM STRENGTHS")
            for s in analysis['team_strengths']:
                print(f"  + {s}")

        # Weaknesses
        if analysis['team_weaknesses']:
            print("\nTEAM WEAKNESSES")
            for w in analysis['team_weaknesses']:
                print(f"  ! {w}")

        # Data quality warnings
        if analysis.get('data_quality_warnings'):
            print("\nDATA QUALITY WARNINGS")
            for warning in analysis['data_quality_warnings']:
                print(f"  ! {warning['message']}")

        # Insights
        if analysis['team_insights']:
            print("\nINSIGHTS")
            print("-" * 50)
            for insight in analysis['team_insights']:
                sev = '+' if insight['severity'] == 'positive' else '!'
                cat = insight['category'].upper()
                print(f"  {sev} [{cat}] {insight['message']}")
                print(f"       -> {insight['action']}")
                print()

        print("=" * 50)

    def show_matchup_analysis(self, matchup: Dict) -> None:
        """Display full 5v5 matchup analysis."""
        print("\n" + "=" * 50)
        print("5v5 MATCHUP ANALYSIS")
        print("=" * 50)

        name_a = matchup['stack_a']['stack_name']
        name_b = matchup['stack_b']['stack_name']
        print(f"\n{'YOUR STACK':<20}  VS  {'OPPONENT STACK'}")
        print(f"{name_a:<20}       {name_b}")

        # Category breakdown
        print("\nCATEGORY BREAKDOWN")
        print("-" * 50)
        print(f"{'Category':<17}{'You':<9}{'Them':<9}{'Edge'}")
        print("-" * 50)

        a_wins = 0
        b_wins = 0
        for cat_key, comp in matchup['category_comparisons'].items():
            label = comp['category']
            val_a = comp['value_a']
            val_b = comp['value_b']
            winner = comp['winner']

            if winner == 'A':
                edge = "YOURS  +"
                a_wins += 1
            elif winner == 'B':
                edge = "THEIRS !"
                b_wins += 1
            else:
                edge = "EVEN"

            # Format values based on category
            if cat_key in ('hs_pct', 'win_rate'):
                va_str = f"{val_a*100:.1f}%"
                vb_str = f"{val_b*100:.1f}%"
            else:
                va_str = f"{val_a:.2f}"
                vb_str = f"{val_b:.2f}"

            print(f"{label:<17}{va_str:<9}{vb_str:<9}{edge}")

        print("-" * 50)
        print(f"{'ADVANTAGES':<17}{a_wins:<9}{b_wins:<9}", end='')
        if a_wins > b_wins:
            print("YOURS")
        elif b_wins > a_wins:
            print("THEIRS")
        else:
            print("EVEN")

        # Role matchups
        self.show_role_matchups(matchup['role_matchups'])

        # Prediction
        pred = matchup['predicted_winner']
        conf = matchup['confidence']
        if pred == 'A':
            print(f"\nPREDICTION: YOUR STACK ({conf:.0f}% confidence)")
        elif pred == 'B':
            print(f"\nPREDICTION: OPPONENT STACK ({conf:.0f}% confidence)")
        else:
            print(f"\nPREDICTION: TOO CLOSE TO CALL ({conf:.0f}%)")

        # Key battlegrounds
        if matchup['key_battlegrounds']:
            print("\nKEY BATTLEGROUNDS")
            for bg in matchup['key_battlegrounds']:
                print(f"  > {bg}")

        # Recommendations
        if matchup['recommendations']:
            print("\nSTRATEGIC RECOMMENDATIONS")
            for rec in matchup['recommendations']:
                print(f"  -> {rec}")

        print("\n" + "=" * 50)

    def show_role_matchups(self, matchups: List[Dict]) -> None:
        """Display role-by-role matchup table."""
        if not matchups:
            return

        print("\nROLE MATCHUPS")
        print("-" * 50)
        print(f"{'Role':<20}{'You':<15}{'Them':<15}{'Edge'}")
        print("-" * 50)
        for rm in matchups:
            adv = rm['advantage'].upper()
            if adv == 'YOURS':
                edge = "YOURS  +"
            elif adv == 'THEIRS':
                edge = "THEIRS !"
            else:
                edge = "EVEN"
            your_name = self._truncate_username(rm['your_player'], 14)
            their_name = self._truncate_username(rm['their_player'], 14)
            print(f"{rm['role']:<20}{your_name:<15}{their_name:<15}{edge}")
        print("-" * 50)

    def show_team_insights(self, insights: List[Dict]) -> None:
        """Display team insights standalone."""
        if not insights:
            print("\nNo insights generated.")
            return

        print("\n" + "=" * 50)
        print("TEAM INSIGHTS")
        print("=" * 50)
        for insight in insights:
            sev = '+' if insight['severity'] == 'positive' else '!'
            cat = insight['category'].upper()
            print(f"  {sev} [{cat}] {insight['message']}")
            print(f"       -> {insight['action']}")
            print()
        print("=" * 50)

    def select_two_stacks(self, stacks: List[Dict]) -> Tuple[int, int]:
        """Let user select two stacks for matchup analysis."""
        if len(stacks) < 2:
            print("\nNeed at least 2 stacks for matchup analysis.")
            return (-1, -1)

        print("\n" + "-" * 50)
        print("Select YOUR stack:")
        stack_a_id = self.select_stack(stacks)
        if stack_a_id == -1:
            return (-1, -1)

        print("\nSelect OPPONENT stack:")
        stack_b_id = self.select_stack(stacks)
        if stack_b_id == -1:
            return (-1, -1)

        if stack_a_id == stack_b_id:
            print("Cannot compare a stack with itself.")
            return (-1, -1)

        return (stack_a_id, stack_b_id)

    def show_error(self, message: str):
        """Display error message."""
        print(f"\n! ERROR: {message}\n")

    def show_success(self, message: str):
        """Display success message."""
        print(f"\n+ {message}\n")
