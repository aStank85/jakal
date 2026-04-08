from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import asyncio

from playwright.async_api import Page


@dataclass(frozen=True)
class MatchDetailPayloads:
    v2: Dict[str, Any]
    v1: Optional[Dict[str, Any]]


async def fetch_match_detail(
    page: Page,
    match_id: str,
    *,
    allow_v1: bool = True,
    timeout_ms: int = 30_000,
    log=None,
) -> MatchDetailPayloads:
    v2_fragment = f"/v2/r6siege/standard/matches/{match_id}".lower()
    v1_fragment = f"/v1/r6siege/ow-ingest/match/get/{match_id}/".lower()
    captured_v1: Dict[str, Any] = {}
    tasks: list[asyncio.Task[Any]] = []

    if log:
        log.info("Match detail fetch started", match_id=match_id, allow_v1=allow_v1)

    async def _capture_v1(response: Any) -> None:
        if "raw" in captured_v1:
            return
        if response.request.resource_type not in ("fetch", "xhr"):
            return
        if v1_fragment not in response.url.lower():
            return
        try:
            captured_v1["raw"] = await response.json()
        except Exception as e:
            if log:
                log.warn("Match detail v1 parse failed", match_id=match_id, error=f"{type(e).__name__}: {e}")

    def _on_response(response: Any) -> None:
        tasks.append(asyncio.create_task(_capture_v1(response)))

    page.on("response", _on_response)
    try:
        async with page.expect_response(
            lambda r: r.request.resource_type in ("fetch", "xhr") and v2_fragment in r.url.lower(),
            timeout=timeout_ms,
        ) as v2_info:
            await page.goto(
                f"https://r6.tracker.network/r6siege/matches/{match_id}",
                wait_until="domcontentloaded",
            )

        v2_resp = await v2_info.value
        v2_raw = await v2_resp.json()
        v2_data = v2_raw.get("data") or v2_raw

        if log:
            metadata = (v2_data.get("metadata") or {}) if isinstance(v2_data, dict) else {}
            log.info(
                "Match detail v2 parsed",
                match_id=match_id,
                has_overwolf_roster=bool(metadata.get("hasOverwolfRoster")),
                extended_data_available=bool(metadata.get("extendedDataAvailable")),
                full_match_available=metadata.get("fullMatchAvailable"),
            )

        if allow_v1:
            await asyncio.sleep(2.5)
        v1_raw = captured_v1.get("raw")
    finally:
        page.remove_listener("response", _on_response)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    if log:
        log.info("Match detail fetch completed", match_id=match_id, has_v1=bool(v1_raw))

    return MatchDetailPayloads(v2=v2_data, v1=v1_raw)
