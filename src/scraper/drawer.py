# src/scraper/drawer.py
"""
Core scraping logic for R6 Tracker season drawer.

Extracts stats from R6 Tracker profile pages using Playwright.
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

from .session import create_browser_context, save_storage_state, close_browser
from .validation import is_valid_snapshot

try:
    from playwright.sync_api import Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    Page = None


def normalize_drawer_text(raw_text: str) -> str:
    """
    Normalize raw drawer text for parser compatibility.

    Args:
        raw_text: Raw text extracted from drawer

    Returns:
        Normalized text (empty lines removed, whitespace cleaned)
    """
    lines = raw_text.split('\n')
    cleaned = [line.strip() for line in lines if line.strip()]
    return '\n'.join(cleaned)


def slice_from_game_section(full_text: str) -> str:
    """
    Extract drawer portion from full page text (fallback strategy).

    Slices from "Game" section header to known footer boundaries.

    Args:
        full_text: Full page inner_text

    Returns:
        Sliced drawer text

    Raises:
        ValueError: If "Game" section not found
    """
    lines = full_text.split('\n')

    # Find "Game" section start
    start_idx = None
    for i, line in enumerate(lines):
        if line.strip() == 'Game':
            start_idx = i
            break

    if start_idx is None:
        raise ValueError(
            "Could not find 'Game' section in page text. "
            "Page may have loaded incorrectly or R6 Tracker changed their layout."
        )

    # Collect until we hit a non-stat section or end
    # Stop at known boundaries like footer text
    stop_markers = {
        'Privacy Policy',
        'Terms of Service',
        'About',
        'Tracker Network',
        'Contact',
        'Advertise',
    }

    drawer_lines = []
    for line in lines[start_idx:]:
        if line.strip() in stop_markers:
            break
        drawer_lines.append(line.strip())

    return '\n'.join([l for l in drawer_lines if l])


def scrape_drawer_text(
    page: 'Page',
    season: Optional[str] = None,
    dump_raw: bool = False
) -> str:
    """
    Extract and normalize drawer text from current R6 Tracker page.

    Args:
        page: Playwright Page object (already navigated to profile)
        season: Season identifier (future use for dropdown selection)
        dump_raw: If True, write debug files (main text + drawer slice)

    Returns:
        Normalized drawer text ready for parser

    Raises:
        RuntimeError: If extraction fails
    """
    try:
        # Click "View All Stats" button using robust selector.
        view_all_btn = page.get_by_role('button', name=re.compile(r'View All Stats', re.I))
        view_all_btn.first.click()

        # R6 Tracker keeps background requests alive; use fixed wait, not networkidle.
        page.wait_for_timeout(2000)

        # Wait for drawer to be visible.
        page.wait_for_selector('text="Game"', state='visible', timeout=30000)

        # Click "Game" header to ensure drawer is expanded/focused
        game_header = page.locator('text="Game"').first
        game_header.click()

        # Brief wait for any animations
        page.wait_for_timeout(500)

        # Primary extraction strategy: Get container via Game header parent
        try:
            # Try to find parent container with stats
            # Walk up DOM to find a reasonable ancestor
            container = game_header.locator('..').locator('..')
            drawer_text = container.inner_text()

            # Sanity check: if too small, fall back
            if len(drawer_text) < 100:
                raise ValueError("Container text too small, using fallback")

        except Exception:
            # Fallback strategy: Get full page text and slice
            full_text = page.locator('body').inner_text()
            drawer_text = slice_from_game_section(full_text)

            if dump_raw:
                # Write debug files for troubleshooting
                with open('debug_raw.txt', 'w', encoding='utf-8') as f:
                    f.write(full_text)
                with open('debug_drawer.txt', 'w', encoding='utf-8') as f:
                    f.write(drawer_text)

        # Normalize text
        normalized = normalize_drawer_text(drawer_text)

        return normalized

    except Exception as e:
        raise RuntimeError(f"Failed to extract drawer text: {e}")


def scrape_profile_drawer(
    username: str,
    platform: str = "ubi",
    season: str = "Y10S4",
    storage_state_path: Optional[str] = "storage_state.json",
    headed: bool = True,
    pause: bool = False,
    dump_raw: bool = False,
    screenshot_path: Optional[str] = None,
    min_rounds: int = 10
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Scrape R6 Tracker profile drawer and return parsed stats.

    Complete pipeline: Navigate → Extract → Parse → Validate → Return

    Args:
        username: R6 username
        platform: Platform (ubi, psn, xbl)
        season: Season identifier
        storage_state_path: Path to storage state JSON for cookies
        headed: Run browser in headed mode (visible window)
        pause: Call page.pause() for debugging with Playwright Inspector
        dump_raw: Write debug files (raw page text + drawer slice)
        screenshot_path: Optional path to save screenshot
        min_rounds: Minimum rounds threshold for validation

    Returns:
        (parsed_stats, meta) tuple:
        - parsed_stats: Dict compatible with Database.add_stats_snapshot()
        - meta: Dict with {url, raw_excerpt, warnings, timestamp}

    Raises:
        ImportError: If Playwright not installed
        ValueError: If validation fails
        RuntimeError: If scraping/parsing fails
    """
    # Import parser here to avoid circular dependency
    from ..parser import R6TrackerParser

    browser = None
    meta = {
        'url': '',
        'raw_excerpt': '',
        'warnings': [],
        'timestamp': datetime.now().isoformat()
    }

    try:
        # Create browser context
        browser, context = create_browser_context(
            headed=headed,
            storage_state_path=storage_state_path
        )

        # Create page and navigate
        page = context.new_page()
        url = f"https://r6.tracker.network/r6siege/profile/{platform}/{username}/overview"
        meta['url'] = url

        page.goto(url)
        page.wait_for_load_state("networkidle")

        # Optional: Pause for debugging
        if pause:
            page.pause()

        # Extract drawer text
        drawer_text = scrape_drawer_text(page, season=season, dump_raw=dump_raw)

        # Store raw excerpt for debugging
        meta['raw_excerpt'] = drawer_text[:500]

        # Optional: Take screenshot
        if screenshot_path:
            page.screenshot(path=screenshot_path)

        # Save storage state (cookies) for future runs
        if storage_state_path:
            save_storage_state(context, storage_state_path)

        # Close browser
        close_browser(browser)
        browser = None  # Mark as closed

        # Parse drawer text
        parser = R6TrackerParser()
        stats = parser.parse(drawer_text)

        # Validate snapshot
        is_valid, validation_warnings = is_valid_snapshot(stats, min_rounds=min_rounds)

        if not is_valid:
            meta['warnings'].extend(validation_warnings)
            raise ValueError(
                f"Snapshot validation failed: {'; '.join(validation_warnings)}"
            )

        # Add non-blocking warnings to meta
        meta['warnings'].extend([w for w in validation_warnings if w.startswith('WARNING')])

        return (stats, meta)

    except Exception as e:
        # Ensure browser cleanup on error
        if browser:
            try:
                close_browser(browser)
            except Exception:
                pass

        raise

    finally:
        # Final cleanup
        if browser:
            try:
                close_browser(browser)
            except Exception:
                pass
