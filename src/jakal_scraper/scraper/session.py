from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth
from ..log import Logger

@dataclass
class BrowserSession:
    browser: Browser
    context: BrowserContext
    page: Page
    _close_timeout_s: float = 10.0
    log: Optional[Logger] = None

    @classmethod
    async def start(
        cls,
        headed: bool,
        storage_state_path: Optional[Path] = None,
        save_storage_state_path: Optional[Path] = None,
        user_agent: Optional[str] = None,
    ) -> "BrowserSession":
        pw = await async_playwright().start()
        stealth = Stealth()
        browser = await pw.chromium.launch(headless=not headed)
        context_kwargs = {
            "viewport": {"width": 1280, "height": 900},
            "locale": "en-US",
            "timezone_id": "America/New_York",
        }
        if storage_state_path:
            context_kwargs["storage_state"] = str(storage_state_path)
        if user_agent:
            context_kwargs["user_agent"] = user_agent
        context = await browser.new_context(**context_kwargs)
        await stealth.apply_stealth_async(context)
        page = await context.new_page()

        # Wire into close to also close playwright driver.
        session = cls(browser=browser, context=context, page=page)
        session._pw = pw  # type: ignore[attr-defined]
        session._save_storage_state_path = save_storage_state_path  # type: ignore[attr-defined]
        return session

    async def close(self, *, skip_state_save: bool = False) -> None:
        # Save cookies/state if requested
        save_path = getattr(self, "_save_storage_state_path", None)
        if save_path and not skip_state_save:
            if self.log:
                self.log.info(
                    "Saving browser storage state",
                    path=str(Path(save_path).resolve()),
                    timeout_s=self._close_timeout_s,
                )
            try:
                await asyncio.wait_for(
                    self.context.storage_state(path=str(save_path)),
                    timeout=self._close_timeout_s,
                )
            except Exception as e:
                if self.log:
                    self.log.warn(
                        "Saving browser storage state failed",
                        path=str(Path(save_path).resolve()),
                        timeout_s=self._close_timeout_s,
                        error=f"{type(e).__name__}: {e}",
                    )
            else:
                if self.log:
                    self.log.info(
                        "Saved browser storage state",
                        path=str(Path(save_path).resolve()),
                    )
        try:
            await asyncio.wait_for(self.context.close(), timeout=self._close_timeout_s)
        except Exception:
            pass
        try:
            await asyncio.wait_for(self.browser.close(), timeout=self._close_timeout_s)
        except Exception:
            pass
        try:
            await asyncio.wait_for(getattr(self, "_pw").stop(), timeout=self._close_timeout_s)  # type: ignore[attr-defined]
        except Exception:
            pass
