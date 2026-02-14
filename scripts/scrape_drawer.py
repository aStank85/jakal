#!/usr/bin/env python3
# scripts/scrape_drawer.py
"""
CLI script for scraping R6 Tracker season drawer stats.

Usage:
    python scripts/scrape_drawer.py --username SaucedZyn
    python scripts/scrape_drawer.py --username SaucedZyn --season Y10S4 --headed
    python scripts/scrape_drawer.py --username SaucedZyn --pause --dump-raw
"""

import sys
import os
import argparse
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Check Playwright availability before importing scraper
try:
    from src.scraper import scrape_profile_drawer
except ImportError as e:
    if 'playwright' in str(e).lower():
        print("✗ ERROR: Playwright not installed")
        print("")
        print("Install with:")
        print("  pip install playwright")
        print("Then run:")
        print("  python -m playwright install")
        print("")
        sys.exit(1)
    else:
        raise

# Import other dependencies
from src.database import Database
from src.calculator import MetricsCalculator
from src.analyzer import InsightAnalyzer


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Scrape R6 Tracker season drawer for a player',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/scrape_drawer.py --username SaucedZyn
  python scripts/scrape_drawer.py --username SaucedZyn --season Y10S4
  python scripts/scrape_drawer.py --username SaucedZyn --pause --dump-raw
  python scripts/scrape_drawer.py --username SaucedZyn --headless
        """
    )

    # Required arguments
    parser.add_argument('--username', required=True, help='R6 username')

    # Optional arguments
    parser.add_argument('--season', default='Y10S4', help='Season (default: Y10S4)')
    parser.add_argument(
        '--platform',
        default='ubi',
        choices=['ubi', 'psn', 'xbl'],
        help='Platform (default: ubi)'
    )
    parser.add_argument(
        '--headed',
        action='store_true',
        default=False,
        help='Run in headed mode (visible browser)'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=False,
        help='Run in headless mode (background, faster)'
    )
    parser.add_argument(
        '--pause',
        action='store_true',
        help='Pause for debugging with Playwright Inspector'
    )
    parser.add_argument(
        '--storage-state',
        default='storage_state.json',
        help='Path to storage state file (default: storage_state.json)'
    )
    parser.add_argument(
        '--min-rounds',
        type=int,
        default=10,
        help='Minimum rounds threshold for validation (default: 10)'
    )
    parser.add_argument(
        '--device-tag',
        default='pc',
        choices=['pc', 'xbox', 'playstation'],
        help='Device tag (default: pc)'
    )
    parser.add_argument(
        '--dump-raw',
        action='store_true',
        help='Write raw page text and drawer slice to debug files'
    )
    parser.add_argument(
        '--screenshot',
        metavar='PATH',
        help='Save screenshot to specified path'
    )

    args = parser.parse_args()

    # Determine headed mode (default to headed unless --headless specified)
    headed = not args.headless
    if args.headed:
        headed = True

    # Print banner
    print("\n" + "=" * 60)
    print("JAKAL - R6 Tracker Drawer Scraper")
    print("=" * 60)

    # Print scraping info
    print(f"[SCRAPING] Username: {args.username} | Season: {args.season} | Platform: {args.platform}")
    print(f"[BROWSER] Opening browser ({'headed' if headed else 'headless'} mode)...")

    try:
        # Scrape profile
        print(f"[NAVIGATE] https://r6.tracker.network/r6siege/profile/{args.platform}/{args.username}/overview")
        print("[EXTRACT] Extracting drawer text...")

        stats, meta = scrape_profile_drawer(
            username=args.username,
            platform=args.platform,
            season=args.season,
            storage_state_path=args.storage_state,
            headed=headed,
            pause=args.pause,
            dump_raw=args.dump_raw,
            screenshot_path=args.screenshot,
            min_rounds=args.min_rounds
        )

        print("[PARSE] Parsing stats...")
        print("[VALIDATE] Checking data quality...")

        # Get current date/time for snapshot
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        snapshot_time = datetime.now().strftime("%H:%M:%S")

        # Insert into database
        print("[INSERT] Adding snapshot to database...")
        db = Database()

        try:
            snapshot_id = db.add_stats_snapshot(
                username=args.username,
                stats=stats,
                snapshot_date=snapshot_date,
                snapshot_time=snapshot_time,
                season=args.season,
                device_tag=args.device_tag
            )

            # Calculate metrics
            print("[CALCULATE] Computing metrics...")
            player = db.get_player(args.username)
            snapshot = db.get_snapshot_by_id(snapshot_id)
            calculator = MetricsCalculator()
            metrics = calculator.calculate_all(snapshot)

            # Store metrics
            db.add_computed_metrics(snapshot_id, player['player_id'], metrics)

            # Success!
            print("\n" + "=" * 60)
            print("✓ SUCCESS")
            print("=" * 60)
            print(f"Player: {args.username}")
            print(f"Snapshot ID: {snapshot_id}")
            print(f"Date: {snapshot_date} {snapshot_time}")
            print("")

            # Display key stats
            print("Key Stats:")
            print(f"  Rounds: {snapshot.get('rounds_played', 'N/A')}")
            print(f"  K/D: {snapshot.get('kd', 'N/A')}")
            rounds_win_pct = snapshot.get('rounds_win_pct', 0)
            if rounds_win_pct:
                print(f"  Win %: {rounds_win_pct:.1f}%")
            print(f"  Role: {metrics['primary_role']} ({metrics['primary_confidence']:.1f})")

            if metrics.get('secondary_role'):
                print(f"  Secondary: {metrics['secondary_role']} ({metrics['secondary_confidence']:.1f})")

            # Show warnings if any
            if meta['warnings']:
                print("\nWarnings:")
                for w in meta['warnings']:
                    print(f"  ! {w}")

            # Show top insight
            analyzer = InsightAnalyzer()
            insights = analyzer.generate_insights(snapshot, metrics)
            if insights:
                top_insight = insights[0]
                print(f"\nTop Insight [{top_insight['severity']}]: {top_insight['message']}")

            print("")

        finally:
            db.close()

    except ValueError as e:
        # Validation error
        print("\n" + "=" * 60)
        print("✗ ERROR: Snapshot validation failed")
        print("=" * 60)
        print(str(e))
        print("")
        print("This usually means R6 Tracker blocked the request or the page didn't load correctly.")
        print("Try running with --headed and passing the consent manually.")
        if args.dump_raw:
            print("\nDebug files written:")
            print("  debug_raw.txt - Full page text")
            print("  debug_drawer.txt - Extracted drawer text")
        print("")
        sys.exit(1)

    except Exception as e:
        # Other errors
        print("\n" + "=" * 60)
        print("✗ ERROR")
        print("=" * 60)
        print(str(e))
        print("")

        # Helpful suggestions
        if "timeout" in str(e).lower():
            print("The page may have loaded incorrectly or R6 Tracker changed their layout.")
            print("Use --pause to debug with Playwright Inspector.")
        elif "consent" in str(e).lower() or "cookie" in str(e).lower():
            print("Try running with --headed to pass consent manually.")
        elif "parse" in str(e).lower():
            print("Parser failed. Use --dump-raw to inspect the extracted text.")

        print("")
        sys.exit(1)


if __name__ == '__main__':
    main()
