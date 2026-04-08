from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass(frozen=True)
class ScrapeConfig:
    db_path: Path
    player: str
    headed: bool = False
    storage_state_path: Optional[Path] = None
    save_storage_state_path: Optional[Path] = None
    max_pages: Optional[int] = None
    full_sync: bool = False
    page_sleep_s: float = 0.75
    sleep_between_matches_s: float = 1.5
    match_timeout_s: float = 30.0
    match_detail_timeout_s: float = 15.0
    include_rollbacks: bool = False
    force_retry: bool = False
    max_attempts: int = 3
    poison_cooldown_days: int = 7
    max_session_restarts: int = 3
    restart_backoff_s: float = 5.0
    user_agent: Optional[str] = None
