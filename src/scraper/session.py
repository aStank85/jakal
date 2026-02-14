# src/scraper/session.py
"""
Browser session management for Playwright-based scraping.

Handles browser lifecycle, storage state persistence for cookies.
"""

import os
from typing import Optional, Tuple

try:
    from playwright.sync_api import sync_playwright, Browser, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    # Type stubs for when Playwright isn't installed
    Browser = None
    BrowserContext = None


def create_browser_context(
    headed: bool = True,
    storage_state_path: Optional[str] = None
) -> Tuple['Browser', 'BrowserContext']:
    """
    Create a Playwright browser and context.

    Args:
        headed: If True, run browser in headed mode (visible window)
        storage_state_path: Path to storage state JSON file for cookie persistence.
                           If file exists, cookies will be loaded.

    Returns:
        (browser, context) tuple

    Raises:
        ImportError: If Playwright is not installed
        RuntimeError: If browser launch fails
    """
    if not PLAYWRIGHT_AVAILABLE:
        raise ImportError(
            "Playwright is not installed.\n"
            "Install with: pip install playwright\n"
            "Then run: python -m playwright install"
        )

    try:
        playwright = sync_playwright().start()

        # Launch browser
        browser = playwright.chromium.launch(
            headless=not headed
        )

        # Create context with optional storage state
        context_kwargs = {}
        if storage_state_path and os.path.exists(storage_state_path):
            context_kwargs['storage_state'] = storage_state_path

        context = browser.new_context(**context_kwargs)

        return browser, context

    except Exception as e:
        raise RuntimeError(f"Failed to create browser context: {e}")


def save_storage_state(context: 'BrowserContext', path: str) -> None:
    """
    Save browser storage state (cookies, localStorage, etc.) to JSON file.

    Args:
        context: Playwright browser context
        path: Path to save storage state JSON

    Raises:
        RuntimeError: If save fails
    """
    try:
        context.storage_state(path=path)
    except Exception as e:
        raise RuntimeError(f"Failed to save storage state to {path}: {e}")


def close_browser(browser: 'Browser') -> None:
    """
    Close browser and cleanup.

    Args:
        browser: Playwright browser instance
    """
    try:
        browser.close()
    except Exception:
        pass  # Best effort cleanup
