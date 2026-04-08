from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from playwright.async_api import Page


@dataclass(frozen=True)
class MatchListItem:
    match_id: str
    timestamp: Optional[str]
    session_type_name: Optional[str]
    gamemode: Optional[str]
    map_slug: Optional[str]
    map_name: Optional[str]
    is_rollback: Optional[bool]
    full_match_available: Optional[bool]


@dataclass(frozen=True)
class MatchListPage:
    items: List[MatchListItem]
    next_cursor: Optional[int]
    raw: Dict[str, Any]


def _extract_item(m: Dict[str, Any]) -> MatchListItem:
    attrs = m.get("attributes", {})
    meta = m.get("metadata", {})
    return MatchListItem(
        match_id=attrs.get("id"),
        timestamp=meta.get("timestamp"),
        session_type_name=meta.get("sessionTypeName"),
        gamemode=attrs.get("gamemode") or meta.get("gamemodeName"),
        map_slug=attrs.get("sessionMap"),
        map_name=meta.get("sessionMapName"),
        is_rollback=meta.get("isRollback"),
        full_match_available=meta.get("fullMatchAvailable"),
    )


def _is_match_list_response(response, username: str, cursor: int) -> bool:
    """Return True only for the specific paginated match-list API call we care about."""
    if response.request.resource_type not in ("fetch", "xhr"):
        return False
    target = f"/matches/ubi/{username}?next={cursor}".lower()
    return target in response.url.lower()


async def fetch_match_list_page(
    page: Page,
    username: str,
    cursor: Optional[int],
    *,
    timeout_ms: int = 30_000,
) -> MatchListPage:
    """
    Navigate to (or paginate) the R6 Tracker match history page and intercept
    the match-list API response using page.expect_response().

    For cursor=0  → full page navigation (fires ?next=0 on load automatically).
    For cursor>0  → scroll to page bottom; infinite-scroll triggers ?next=N organically.

    Both paths use page.expect_response() so the waiter is registered *before*
    the navigation/click fires, eliminating any race condition.
    """
    request_cursor = 0 if cursor is None else cursor

    async with page.expect_response(
        lambda r: _is_match_list_response(r, username, request_cursor),
        timeout=timeout_ms,
    ) as response_info:
        if request_cursor == 0:
            await page.goto(
                f"https://r6.tracker.network/r6siege/profile/ubi/{username}/matches",
                wait_until="domcontentloaded",
            )
        else:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

    response = await response_info.value
    raw = await response.json()

    data = raw.get("data", {})
    items = [_extract_item(m) for m in data.get("matches", [])]
    metadata = raw.get("metadata") or {}
    raw_next = metadata.get("next")

    if not items:
        next_cursor = None
    else:
        try:
            next_cursor = int(raw_next)
        except (TypeError, ValueError):
            next_cursor = request_cursor + 1

    return MatchListPage(items=items, next_cursor=next_cursor, raw=raw)
