from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import asyncio
import traceback

from playwright.async_api import Page

from ..config import ScrapeConfig
from ..db import SQLiteStore
from ..log import Logger

from .session import BrowserSession
from .listing import fetch_match_list_page
from .detail import fetch_match_detail
from .parse_v2 import parse_v2_match
from .parse_v1 import parse_v1_ingest
from .merge import merge_v1_v2


@dataclass
class ScrapeRunner:
    cfg: ScrapeConfig
    log: Logger

    async def sync_player(self) -> None:
        store = SQLiteStore.open(self.cfg.db_path)
        from pathlib import Path
        migrations_dir = Path(__file__).resolve().parents[1] / "migrations"
        store.apply_migrations(migrations_dir)
        store.ensure_player(self.cfg.player)

        try:
            cursor: Optional[int] = 0
            pages = 0
            restart_attempts = 0
            force_headless_fresh = False
            consecutive_match_detail_timeouts = 0

            while True:
                restart_reason: Optional[str] = None
                self.log.info(
                    "Starting browser session",
                    player=self.cfg.player,
                    cursor=cursor,
                    pages_completed=pages,
                )
                session = await BrowserSession.start(
                    headed=False if force_headless_fresh else self.cfg.headed,
                    storage_state_path=None if force_headless_fresh else self.cfg.storage_state_path,
                    save_storage_state_path=self.cfg.save_storage_state_path,
                    user_agent=self.cfg.user_agent,
                )
                force_headless_fresh = False
                session.log = self.log

                try:
                    while True:
                        if self.cfg.max_pages is not None and pages >= self.cfg.max_pages:
                            self.log.info("Reached max_pages", max_pages=self.cfg.max_pages)
                            break

                        self.log.info(
                            "Fetching match list page",
                            player=self.cfg.player,
                            cursor=cursor,
                        )
                        try:
                            page_result = await asyncio.wait_for(
                                fetch_match_list_page(
                                    session.page,
                                    self.cfg.player,
                                    cursor,
                                    timeout_ms=int(self.cfg.match_timeout_s * 1000),
                                ),
                                timeout=self.cfg.match_timeout_s,
                            )
                            self.log.info(
                                "Fetched match list page",
                                player=self.cfg.player,
                                cursor=cursor,
                                items=len(page_result.items),
                                next_cursor=page_result.next_cursor,
                            )
                        except asyncio.TimeoutError:
                            self.log.warn(
                                "Match list page timed out",
                                player=self.cfg.player,
                                cursor=cursor,
                                timeout_s=self.cfg.match_timeout_s,
                            )
                            restart_reason = f"match list page timed out at cursor {cursor}"
                            break
                        except Exception as e:
                            self.log.warn(
                                "Match list page failed",
                                player=self.cfg.player,
                                cursor=cursor,
                                error=f"{type(e).__name__}: {e}",
                            )
                            restart_reason = f"match list page failed at cursor {cursor}: {e}"
                            break

                        items = page_result.items
                        if not self.cfg.include_rollbacks:
                            items = [i for i in items if not (i.is_rollback is True)]

                        store.upsert_player_match_index(self.cfg.player, [
                            {
                                "match_id": i.match_id,
                                "timestamp": i.timestamp,
                                "session_type_name": i.session_type_name,
                                "gamemode": i.gamemode,
                                "map_slug": i.map_slug,
                                "map_name": i.map_name,
                                "is_rollback": i.is_rollback,
                                "full_match_available": i.full_match_available,
                            }
                            for i in items if i.match_id
                        ])

                        match_ids = [i.match_id for i in items if i.match_id]

                        queue = []
                        for mid in match_ids:
                            if store.should_skip_poison(mid, max_attempts=self.cfg.max_attempts, force_retry=self.cfg.force_retry):
                                self.log.warn("Skipping poison match (cooldown active)", match_id=mid)
                                continue
                            if self.cfg.force_retry or not store.v2_done(mid):
                                queue.append(mid)

                        for mid in queue:
                            try:
                                await asyncio.wait_for(
                                    self._process_match(store, session.page, mid),
                                    timeout=self.cfg.match_detail_timeout_s,
                                )
                                consecutive_match_detail_timeouts = 0
                            except asyncio.TimeoutError:
                                err = f"TimeoutError: match processing exceeded {self.cfg.match_detail_timeout_s}s"
                                consecutive_match_detail_timeouts += 1
                                self.log.warn(
                                    "Match timed out",
                                    match_id=mid,
                                    timeout_s=self.cfg.match_detail_timeout_s,
                                    consecutive_match_detail_timeouts=consecutive_match_detail_timeouts,
                                )
                                store.mark_match_failure(
                                    mid,
                                    error=err,
                                    max_attempts=self.cfg.max_attempts,
                                    cooldown_days=self.cfg.poison_cooldown_days,
                                )
                                if consecutive_match_detail_timeouts > 5:
                                    self.log.warn(
                                        "Too many consecutive match detail timeouts - restarting session",
                                        player=self.cfg.player,
                                        cursor=cursor,
                                    )
                                    restart_reason = "too many consecutive match detail timeouts"
                                    break
                            await asyncio.sleep(self.cfg.sleep_between_matches_s)

                        if restart_reason is not None:
                            break

                        pages += 1

                        if not page_result.items:
                            self.log.info(
                                "End of match history (empty page)",
                                player=self.cfg.player,
                                cursor=cursor,
                            )
                            break

                        fully_known = store.page_matches_all_v2_done(match_ids)
                        if fully_known and not self.cfg.full_sync:
                            self.log.info(
                                "Stopping: page fully known (incremental boundary)",
                                player=self.cfg.player,
                                cursor=cursor,
                            )
                            break

                        cursor = page_result.next_cursor
                        if cursor is not None and self.cfg.page_sleep_s > 0:
                            await asyncio.sleep(self.cfg.page_sleep_s)

                finally:
                    self.log.info(
                        "Closing browser session",
                        player=self.cfg.player,
                        cursor=cursor,
                        pages_completed=pages,
                    )
                    await session.close(skip_state_save=restart_reason is not None)
                    self.log.info(
                        "Browser session closed",
                        player=self.cfg.player,
                        cursor=cursor,
                        pages_completed=pages,
                    )

                if restart_reason is None:
                    break

                restart_attempts += 1
                if restart_attempts >= self.cfg.max_session_restarts:
                    self.log.warn(
                        "Restart budget exhausted; continuing with fresh headless session",
                        player=self.cfg.player,
                        cursor=cursor,
                        pages=pages,
                        restart_attempts=restart_attempts,
                        reason=restart_reason,
                    )
                    restart_attempts = 0
                    force_headless_fresh = True

                self.log.warn(
                    "Restarting browser session",
                    player=self.cfg.player,
                    restart_attempt=restart_attempts,
                    max_session_restarts=self.cfg.max_session_restarts,
                    resume_cursor=cursor,
                    pages_completed=pages,
                    backoff_s=self.cfg.restart_backoff_s,
                    reason=restart_reason,
                )
                if self.cfg.restart_backoff_s > 0:
                    await asyncio.sleep(self.cfg.restart_backoff_s)

        finally:
            store.close()

    async def _process_match(self, store: SQLiteStore, page: Page, match_id: str) -> None:
        self.log.info("Processing match", match_id=match_id)
        try:
            payloads = await fetch_match_detail(
                page,
                match_id,
                allow_v1=True,
                timeout_ms=int(self.cfg.match_timeout_s * 1000),
                log=self.log,
            )

            v2_parsed = parse_v2_match(payloads.v2)
            v1_parsed = parse_v1_ingest(payloads.v1) if payloads.v1 else None
            merged = merge_v1_v2(v2_parsed, v1_parsed)

            store.upsert_match(merged.match)
            store.upsert_match_players(merged.match["match_id"], merged.match_players)
            if merged.rounds:
                store.upsert_rounds(merged.match["match_id"], merged.rounds)
            if merged.player_rounds:
                store.upsert_player_rounds(merged.match["match_id"], merged.player_rounds)
            if merged.kill_events:
                store.upsert_kill_events(merged.match["match_id"], merged.kill_events)

            store.mark_match_success(match_id, v2_done=True, v1_done=bool(merged.v1_used))
            self.log.info(
                "Match committed",
                match_id=match_id,
                v1_used=merged.v1_used,
                pr=len(merged.player_rounds),
                rounds=len(merged.rounds),
            )

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            tb = traceback.format_exc(limit=10)
            self.log.error("Match failed", match_id=match_id, error=err)
            row = store.mark_match_failure(
                match_id,
                error=err + "\n" + tb,
                max_attempts=self.cfg.max_attempts,
                cooldown_days=self.cfg.poison_cooldown_days,
            )
            self.log.warn(
                "Updated match status after failure",
                match_id=match_id,
                attempts=int(row["attempts"]),
                next_retry_after=row["next_retry_after"],
            )
