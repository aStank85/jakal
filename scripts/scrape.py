from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jakal_scraper.config import ScrapeConfig
from src.jakal_scraper.log import Logger
from src.jakal_scraper.scraper.runner import ScrapeRunner

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite DB")
    ap.add_argument("--player", required=True, help="UBI username (e.g., SaucedZyn)")
    ap.add_argument("--headed", action="store_true", help="Run headed (useful for first run / consent)")
    ap.add_argument("--state", type=str, default=None, help="Path to Playwright storage_state.json to load")
    ap.add_argument("--save-state", type=str, default=None, help="Where to write storage_state.json on exit")
    ap.add_argument("--max-pages", type=int, default=None, help="Max list pages to fetch")
    ap.add_argument("--full-sync", action="store_true", help="Paginate full history instead of stopping at the first fully synced page")
    ap.add_argument("--page-sleep", type=float, default=0.75, help="Seconds to sleep between match list page fetches")
    ap.add_argument("--sleep", type=float, default=1.5, help="Seconds to sleep between match detail fetches")
    ap.add_argument("--timeout", type=float, default=30.0, help="Seconds before a match detail task is cancelled")
    ap.add_argument("--detail-timeout", type=float, default=15.0, help="Seconds before a match detail task is cancelled")
    ap.add_argument("--include-rollbacks", action="store_true", help="Include rollback matches")
    ap.add_argument("--force-retry", action="store_true", help="Ignore poison cooldown and re-attempt failing matches")
    ap.add_argument("--max-attempts", type=int, default=3, help="Failures before poison cooldown")
    ap.add_argument("--poison-cooldown-days", type=int, default=7, help="Cooldown after max attempts")
    ap.add_argument("--max-session-restarts", type=int, default=3, help="Browser session restarts allowed after repeated timeouts")
    ap.add_argument("--restart-backoff", type=float, default=5.0, help="Seconds to sleep before restarting the browser session")
    ap.add_argument("--user-agent", type=str, default=None, help="Optional UA override")
    return ap.parse_args()

async def _run(cfg: ScrapeConfig) -> None:
    runner = ScrapeRunner(cfg=cfg, log=Logger())
    await runner.sync_player()

def main() -> None:
    a = parse_args()
    cfg = ScrapeConfig(
        db_path=Path(a.db),
        player=a.player,
        headed=a.headed,
        storage_state_path=Path(a.state) if a.state else None,
        save_storage_state_path=Path(a.save_state) if a.save_state else None,
        max_pages=a.max_pages,
        full_sync=a.full_sync,
        page_sleep_s=a.page_sleep,
        sleep_between_matches_s=a.sleep,
        match_timeout_s=a.timeout,
        match_detail_timeout_s=a.detail_timeout,
        include_rollbacks=a.include_rollbacks,
        force_retry=a.force_retry,
        max_attempts=a.max_attempts,
        poison_cooldown_days=a.poison_cooldown_days,
        max_session_restarts=a.max_session_restarts,
        restart_backoff_s=a.restart_backoff,
        user_agent=a.user_agent,
    )
    asyncio.run(_run(cfg))

if __name__ == "__main__":
    main()
