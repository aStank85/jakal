import json
import time
from typing import Callable


_db = None
_get_db_cursor: Callable[[], object] | None = None

workspace_scope_cache_mem: dict[str, tuple[float, dict]] = {}
workspace_team_cache_mem: dict[str, tuple[float, dict]] = {}
workspace_insights_cache_mem: dict[str, tuple[float, dict]] = {}
WORKSPACE_SCOPE_CACHE_TTL_SECONDS = 180
WORKSPACE_TEAM_CACHE_TTL_SECONDS = 300
WORKSPACE_INSIGHTS_CACHE_TTL_SECONDS = 300


def configure_workspace_cache(db, get_db_cursor: Callable[[], object]) -> None:
    global _db, _get_db_cursor
    _db = db
    _get_db_cursor = get_db_cursor


def _ensure_workspace_cache_tables() -> None:
    cur = _get_db_cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace_scope_cache (
            scope_key    TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            created_at   REAL NOT NULL,
            expires_at   REAL NOT NULL,
            db_rev       TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace_team_cache (
            team_key     TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            created_at   REAL NOT NULL,
            expires_at   REAL NOT NULL,
            db_rev       TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace_insights_cache (
            insights_key  TEXT PRIMARY KEY,
            payload_json  TEXT NOT NULL,
            created_at    REAL NOT NULL,
            expires_at    REAL NOT NULL,
            db_rev        TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workspace_scope_cache_expires
        ON workspace_scope_cache (expires_at)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workspace_team_cache_expires
        ON workspace_team_cache (expires_at)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workspace_insights_cache_expires
        ON workspace_insights_cache (expires_at)
        """
    )
    _db.conn.commit()


def _workspace_sql_cache_get(table: str, key_col: str, key: str, db_rev: str) -> dict | None:
    cur = _get_db_cursor()
    now = time.time()
    cur.execute(f"DELETE FROM {table} WHERE expires_at <= ?", (now,))
    cur.execute(
        f"SELECT payload_json, db_rev FROM {table} WHERE {key_col} = ? AND expires_at > ? LIMIT 1",
        (key, now),
    )
    row = cur.fetchone()
    if not row:
        return None
    if str(row["db_rev"] or "") != str(db_rev or ""):
        return None
    try:
        payload = json.loads(row["payload_json"] or "{}")
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _workspace_sql_cache_set(table: str, key_col: str, key: str, payload: dict, ttl_seconds: int, db_rev: str) -> None:
    now = time.time()
    expires = now + max(1, int(ttl_seconds))
    cur = _get_db_cursor()
    cur.execute(
        f"""
        INSERT INTO {table} ({key_col}, payload_json, created_at, expires_at, db_rev)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT({key_col}) DO UPDATE SET
            payload_json = excluded.payload_json,
            created_at = excluded.created_at,
            expires_at = excluded.expires_at,
            db_rev = excluded.db_rev
        """,
        (key, json.dumps(payload, separators=(",", ":"), sort_keys=True), now, expires, str(db_rev or "")),
    )
    _db.conn.commit()


def _workspace_scope_cache_get(scope_key: str, db_rev: str) -> dict | None:
    now = time.time()
    item = workspace_scope_cache_mem.get(scope_key)
    if item:
        ts, payload = item
        if now - ts <= WORKSPACE_SCOPE_CACHE_TTL_SECONDS:
            if str(payload.get("db_rev") or "") == str(db_rev or ""):
                return payload
        else:
            workspace_scope_cache_mem.pop(scope_key, None)
    payload = _workspace_sql_cache_get("workspace_scope_cache", "scope_key", scope_key, db_rev)
    if payload is not None:
        workspace_scope_cache_mem[scope_key] = (now, payload)
    return payload


def _workspace_scope_cache_set(scope_key: str, payload: dict, db_rev: str) -> None:
    cache_payload = dict(payload or {})
    cache_payload["db_rev"] = str(db_rev or "")
    workspace_scope_cache_mem[scope_key] = (time.time(), cache_payload)
    _workspace_sql_cache_set(
        "workspace_scope_cache",
        "scope_key",
        scope_key,
        cache_payload,
        WORKSPACE_SCOPE_CACHE_TTL_SECONDS,
        db_rev,
    )


def _workspace_team_cache_get(team_key: str, db_rev: str) -> dict | None:
    now = time.time()
    item = workspace_team_cache_mem.get(team_key)
    if item:
        ts, payload = item
        if now - ts <= WORKSPACE_TEAM_CACHE_TTL_SECONDS:
            if str(payload.get("db_rev") or "") == str(db_rev or ""):
                return payload
        else:
            workspace_team_cache_mem.pop(team_key, None)
    payload = _workspace_sql_cache_get("workspace_team_cache", "team_key", team_key, db_rev)
    if payload is not None:
        workspace_team_cache_mem[team_key] = (now, payload)
    return payload


def _workspace_team_cache_set(team_key: str, payload: dict, db_rev: str) -> None:
    cache_payload = dict(payload or {})
    cache_payload["db_rev"] = str(db_rev or "")
    workspace_team_cache_mem[team_key] = (time.time(), cache_payload)
    _workspace_sql_cache_set(
        "workspace_team_cache",
        "team_key",
        team_key,
        cache_payload,
        WORKSPACE_TEAM_CACHE_TTL_SECONDS,
        db_rev,
    )


def _workspace_insights_cache_get(insights_key: str, db_rev: str) -> dict | None:
    now = time.time()
    item = workspace_insights_cache_mem.get(insights_key)
    if item:
        ts, payload = item
        if now - ts <= WORKSPACE_INSIGHTS_CACHE_TTL_SECONDS:
            if str(payload.get("db_rev") or "") == str(db_rev or ""):
                return payload
        else:
            workspace_insights_cache_mem.pop(insights_key, None)
    payload = _workspace_sql_cache_get("workspace_insights_cache", "insights_key", insights_key, db_rev)
    if payload is not None:
        workspace_insights_cache_mem[insights_key] = (now, payload)
    return payload


def _workspace_insights_cache_set(insights_key: str, payload: dict, db_rev: str) -> None:
    cache_payload = dict(payload or {})
    cache_payload["db_rev"] = str(db_rev or "")
    workspace_insights_cache_mem[insights_key] = (time.time(), cache_payload)
    _workspace_sql_cache_set(
        "workspace_insights_cache",
        "insights_key",
        insights_key,
        cache_payload,
        WORKSPACE_INSIGHTS_CACHE_TTL_SECONDS,
        db_rev,
    )
