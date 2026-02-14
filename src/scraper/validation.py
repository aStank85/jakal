# src/scraper/validation.py
"""
Validation logic to prevent junk snapshot inserts.

Catches blocked pages, consent walls, partial loads that would insert zeros.
"""

from typing import Dict, Any, List, Tuple


def is_valid_snapshot(stats: Dict[str, Any], min_rounds: int = 10) -> Tuple[bool, List[str]]:
    """
    Validate that scraped stats represent real data, not a blocked/partial page.

    Validation rules (strengthened):
    1. Hard-fail: rounds_played == 0 → Reject immediately
    2. Hard-fail: Both matches == 0 AND time_played_hours == 0 → Reject immediately
    3. Soft validation: If rounds_played < min_rounds AND at least one of
       (kills, matches, time_played_hours) is missing/0 → Reject

    Args:
        stats: Parsed stats dictionary from R6TrackerParser
        min_rounds: Minimum rounds threshold (default: 10)

    Returns:
        (is_valid: bool, warnings: List[str])
        - is_valid: True if snapshot should be inserted
        - warnings: List of validation messages (both failures and warnings)

    Examples:
        >>> stats = {'rounds': {'rounds_played': 0}, ...}
        >>> is_valid, warnings = is_valid_snapshot(stats)
        >>> is_valid
        False
        >>> 'rounds_played is 0' in warnings[0]
        True
    """
    warnings = []

    # Extract critical fields with safe fallbacks
    rounds_played = stats.get('rounds', {}).get('rounds_played', 0) or 0
    kills = stats.get('combat', {}).get('kills', 0) or 0
    matches = stats.get('game', {}).get('matches', 0) or 0
    time_played_hours = stats.get('game', {}).get('time_played_hours', 0) or 0

    # Hard-fail #1: rounds_played == 0
    if rounds_played == 0:
        warnings.append("REJECTED: rounds_played is 0 (likely blocked/partial page)")
        return (False, warnings)

    # Hard-fail #2: Both matches AND time_played_hours are 0
    if matches == 0 and time_played_hours == 0:
        warnings.append(
            "REJECTED: Both matches and time_played_hours are 0 (likely blocked page)"
        )
        return (False, warnings)

    # Soft validation: Low rounds + other missing fields
    if rounds_played < min_rounds:
        warnings.append(f"rounds_played ({rounds_played}) < minimum threshold ({min_rounds})")

        missing_count = 0
        if kills == 0:
            missing_count += 1
            warnings.append("kills is 0 or missing")
        if matches == 0:
            missing_count += 1
            warnings.append("matches is 0 or missing")
        if time_played_hours == 0:
            missing_count += 1
            warnings.append("time_played_hours is 0 or missing")

        # If low rounds AND at least one other field missing, reject
        if missing_count >= 1:
            warnings.append("REJECTED: Likely blocked/partial page load")
            return (False, warnings)

    # Additional sanity checks (non-blocking warnings)
    if rounds_played > 0 and kills == 0:
        warnings.append("WARNING: rounds_played > 0 but kills = 0 (unusual but allowed)")

    # If we got here, validation passed
    return (True, warnings)
