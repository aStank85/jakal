# main.py

from src.parser import R6TrackerParser
from src.database import Database
from src.calculator import MetricsCalculator
from src.comparator import PlayerComparator
from src.ui import TerminalUI

def main():
    # Initialize components
    parser = R6TrackerParser()
    db = Database()
    calculator = MetricsCalculator()
    comparator = PlayerComparator()
    ui = TerminalUI()

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
                        season=metadata['season']
                    )

                    # Get player_id for metrics
                    player = db.get_player(metadata['username'])
                    player_id = player['player_id']

                    # Calculate and save metrics
                    snapshot = db.get_latest_snapshot(metadata['username'])
                    metrics = calculator.calculate_all(snapshot)

                    # Store metrics
                    db.add_computed_metrics(snapshot_id, player_id, metrics)

                    ui.show_success(f"Stats added for {metadata['username']}!")

                    # Show role
                    print(f"Role: {metrics['primary_role']} ({metrics['primary_confidence']:.1f})")
                    if metrics['secondary_role']:
                        print(f"Secondary: {metrics['secondary_role']} ({metrics['secondary_confidence']:.1f})")

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
                            # Try to get metrics from DB first, calculate if not found
                            metrics = db.get_latest_metrics(username)
                            if not metrics:
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

                    # Get or calculate metrics
                    metrics = db.get_latest_metrics(username_input)
                    if not metrics:
                        metrics = calculator.calculate_all(snapshot)

                    ui.show_player_details(snapshot, metrics)

                except Exception as e:
                    ui.show_error(str(e))

            elif choice == '5':
                # Exit
                print("\nGoodbye!")
                break

    finally:
        # Ensure database is always closed
        db.close()

if __name__ == '__main__':
    main()
