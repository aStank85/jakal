# src/scraper/__init__.py
"""
Web scraping module for R6 Tracker stats.

v0.5: Playwright-based drawer scraper for automated stats fetching.
"""

from .drawer import scrape_profile_drawer, scrape_drawer_text
from .session import create_browser_context, save_storage_state, close_browser
from .validation import is_valid_snapshot
from .core import R6Scraper, ScraperBlockedError, PlayerNotFoundError

__all__ = [
    'scrape_profile_drawer',
    'scrape_drawer_text',
    'create_browser_context',
    'save_storage_state',
    'close_browser',
    'is_valid_snapshot',
    'R6Scraper',
    'ScraperBlockedError',
    'PlayerNotFoundError',
]
