# main.py

from src.parser import R6TrackerParser
from src.database import Database
from src.calculator import MetricsCalculator
from src.comparator import PlayerComparator
from src.analyzer import InsightAnalyzer
from src.ui import TerminalUI
from src.stack_manager import StackManager
from src.team_analyzer import TeamAnalyzer
from src.matchup_analyzer import MatchupAnalyzer
from src.thresholds import MIN_RELIABLE_ROUNDS_PER_HOUR



def main():
    # Initialize components
    parser = R6TrackerParser()
    db = Database()
    calculator = MetricsCalculator()
    comparator = PlayerComparator()
    analyzer = InsightAnalyzer()
    ui = TerminalUI()
    stack_manager = StackManager(db)
    team_analyzer = TeamAnalyzer(db)
    matchup_analyzer = MatchupAnalyzer(db)

    try:
        while True:
            choice = ui.show_menu()

            if choice == '1':
                # Add new stats
                try:
                    pasted_text = ui.get_paste_input()
                    metadata = ui.get_metadata()

                    # Parse stats
                    stats = parser.parse(pasted_text)

                    # Save to database
                    snapshot_id = db.add_stats_snapshot(
                        username=metadata['username'],
                        stats=stats,
                        snapshot_date=metadata['date'],
                        snapshot_time=metadata['time'],
                        season=metadata['season'],
                        device_tag=metadata['device_tag']
                    )

                    # Get player_id for metrics
                    player = db.get_player(metadata['username'])
                    player_id = player['player_id']

                    # Calculate and save metrics for the inserted snapshot.
                    snapshot = db.get_snapshot_by_id(snapshot_id)
                    if not snapshot:
                        raise RuntimeError(f"Failed to load snapshot {snapshot_id} after insert")
                    metrics = calculator.calculate_all(snapshot)

                    # Store metrics
                    db.add_computed_metrics(snapshot_id, player_id, metrics)

                    ui.show_success(f"Stats added for {metadata['username']}!")

                    # Show role
                    print(f"Role: {metrics['primary_role']} ({metrics['primary_confidence']:.1f})")
                    if metrics['secondary_role']:
                        print(f"Secondary: {metrics['secondary_role']} ({metrics['secondary_confidence']:.1f})")

                    print(f"Rounds/Hour: {metrics.get('rounds_per_hour', 0.0):.2f}")
                    if metrics.get('time_played_unreliable', False):
                        print(
                            f"Warning: time-played metrics unreliable (Rounds/Hour < {MIN_RELIABLE_ROUNDS_PER_HOUR:.1f}); per-hour metrics suppressed."
                        )

                    insights = analyzer.generate_insights(snapshot, metrics)
                    if insights:
                        top_insight = insights[0]
                        print(f"Top Insight [{top_insight['severity']}]: {top_insight['message']}")

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '2':
                # View all players
                try:
                    players = db.get_all_players()
                    if not players:
                        ui.show_error("No players in database yet")
                    else:
                        ui.show_players(players)
                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '3':
                # Compare players
                try:
                    all_players = db.get_all_players()

                    if len(all_players) < 2:
                        ui.show_error("Need at least 2 players to compare")
                        continue

                    selected = ui.select_players_for_comparison(all_players)

                    if len(selected) < 2:
                        ui.show_error("Select at least 2 players")
                        continue

                    # Get latest snapshots
                    snapshots = []
                    metrics_list = []

                    for username in selected:
                        snapshot = db.get_latest_snapshot(username)
                        if snapshot:
                            snapshots.append(snapshot)
                            # Recalculate from snapshot for full consistency with player details view.
                            metrics = calculator.calculate_all(snapshot)
                            metrics_list.append(metrics)

                    if len(snapshots) < 2:
                        ui.show_error("Could not find snapshots for selected players")
                        continue

                    # Compare
                    comparison = comparator.compare(snapshots, metrics_list)
                    ui.show_comparison(comparison)

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '4':
                # View player details
                try:
                    all_players = db.get_all_players()

                    if not all_players:
                        ui.show_error("No players in database yet")
                        continue

                    ui.show_players(all_players)

                    username_input = input("\nEnter player username: ").strip()

                    if not db.player_exists(username_input):
                        ui.show_error(f"Player '{username_input}' not found")
                        continue

                    snapshot = db.get_latest_snapshot(username_input)
                    if not snapshot:
                        ui.show_error(f"No snapshots found for '{username_input}'")
                        continue

                    # Recalculate from snapshot so newer formulas are always available.
                    metrics = calculator.calculate_all(snapshot)

                    insights = analyzer.generate_insights(snapshot, metrics)
                    ui.show_player_details(snapshot, metrics, insights)

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '5':
                # Stack Management
                try:
                    while True:
                        stack_choice = ui.show_stack_menu()

                        if stack_choice == '1':
                            name = ui.get_stack_name()
                            if not name:
                                ui.show_error("Stack name cannot be empty")
                                continue
                            description = ui.get_stack_description()
                            stack_id = stack_manager.create_stack(name, description)
                            ui.show_success(f"Created stack '{name}' (ID: {stack_id})")

                        elif stack_choice == '2':
                            stacks = stack_manager.get_all_stacks()
                            ui.show_all_stacks(stacks)

                        elif stack_choice == '3':
                            stacks = stack_manager.get_all_stacks()
                            stack_id = ui.select_stack(stacks)
                            if stack_id == -1:
                                continue

                            all_players = db.get_all_players()
                            if not all_players:
                                ui.show_error("No players in database yet")
                                continue

                            username = ui.select_player(all_players)
                            if not username:
                                ui.show_error("Invalid player selection")
                                continue

                            role_override = input("Role override (optional): ").strip() or None
                            stack_manager.add_player_to_stack(stack_id, username, role_override)
                            ui.show_success(f"Added {username} to stack")

                        elif stack_choice == '4':
                            stacks = stack_manager.get_all_stacks()
                            stack_id = ui.select_stack(stacks)
                            if stack_id == -1:
                                continue

                            members = stack_manager.get_stack_members(stack_id)
                            if not members:
                                ui.show_error("Stack has no members")
                                continue

                            username = ui.select_player(members)
                            if not username:
                                ui.show_error("Invalid player selection")
                                continue

                            stack_manager.remove_player_from_stack(stack_id, username)
                            ui.show_success(f"Removed {username} from stack")

                        elif stack_choice == '5':
                            tag = input("Enter tag (default teammate): ").strip() or 'teammate'
                            stack_id = stack_manager.build_tagged_stack(tag)
                            ui.show_success(f"Built tagged stack (ID: {stack_id})")

                        elif stack_choice == '6':
                            all_players = db.get_all_players()
                            if len(all_players) < 2:
                                ui.show_error("Need at least 2 players to create a quick stack")
                                continue

                            selected = ui.select_players_for_stack(all_players)
                            if len(selected) < 2:
                                ui.show_error("Select at least 2 players")
                                continue

                            stack_id = stack_manager.create_quick_stack(selected)
                            ui.show_success(f"Created quick stack (ID: {stack_id})")

                        elif stack_choice == '7':
                            stacks = stack_manager.get_all_stacks()
                            stack_id = ui.select_stack(stacks)
                            if stack_id == -1:
                                continue
                            stack_manager.delete_stack(stack_id)
                            ui.show_success("Stack deleted")

                        elif stack_choice == '8':
                            break

                        else:
                            ui.show_error("Invalid option")

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '6':
                # Analyze Stack
                try:
                    stacks = stack_manager.get_all_stacks()
                    stack_id = ui.select_stack(stacks)
                    if stack_id == -1:
                        continue

                    analysis = team_analyzer.analyze_stack(stack_id)
                    ui.show_team_analysis(analysis)

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '7':
                # 5v5 Matchup Analysis
                try:
                    stacks = stack_manager.get_all_stacks()
                    stack_a_id, stack_b_id = ui.select_two_stacks(stacks)
                    if stack_a_id == -1 or stack_b_id == -1:
                        continue

                    matchup = matchup_analyzer.analyze_matchup(stack_a_id, stack_b_id)
                    ui.show_matchup_analysis(matchup)

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '8':
                # Exit
                print("\nGoodbye!")
                break

    finally:
        # Ensure database is always closed
        db.close()


if __name__ == '__main__':
    main()





