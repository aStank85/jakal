import math
import re
from datetime import datetime, timezone


def _parse_iso_datetime(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _wilson_ci(successes: int, trials: int) -> tuple[float, float]:
    if trials <= 0:
        return 0.0, 0.0
    p = successes / trials
    z = 1.96
    z2 = z * z
    denom = 1.0 + (z2 / trials)
    center = (p + (z2 / (2.0 * trials))) / denom
    half = (z / denom) * math.sqrt((p * (1.0 - p) / trials) + (z2 / (4.0 * trials * trials)))
    return max(0.0, center - half), min(1.0, center + half)


def _pctile_abs_bound(values: list[float], fallback: float = 15.0) -> float:
    vals = sorted(abs(float(v)) for v in values if isinstance(v, (float, int)) and math.isfinite(float(v)))
    if len(vals) < 8:
        return fallback
    lo = vals[int(max(0, math.floor(0.05 * (len(vals) - 1))))]
    hi = vals[int(max(0, math.floor(0.95 * (len(vals) - 1))))]
    hi = max(hi, lo, 0.000001)
    return hi


def _is_unknown_operator_name(value: object) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return True
    bad = {
        "unknown",
        "unk",
        "n/a",
        "na",
        "none",
        "null",
        "-",
        "?",
        "operator",
        "undefined",
    }
    return text in bad


def _normalize_asset_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _normalize_mode_key(raw_mode: object) -> str:
    tokens = set(re.findall(r"[a-z0-9]+", str(raw_mode or "").strip().lower()))
    if not tokens:
        return "other"
    if "unranked" in tokens:
        return "unranked"
    if "ranked" in tokens:
        return "ranked"
    if "standard" in tokens:
        return "standard"
    if "quick" in tokens or "quickmatch" in tokens:
        return "quick"
    if "event" in tokens:
        return "event"
    if "arcade" in tokens:
        return "arcade"
    return "other"
