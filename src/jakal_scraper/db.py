from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import datetime as _dt
import json

def utc_now_iso() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat()

@dataclass
class SQLiteStore:
    path: Path
    conn: sqlite3.Connection

    @classmethod
    def open(cls, path: Path) -> "SQLiteStore":
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return cls(path=path, conn=conn)

    def close(self) -> None:
        self.conn.close()

    # -----------------------
    # Migration runner
    # -----------------------
    def apply_migrations(self, migrations_dir: Path) -> None:
        self.conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)")
        applied = {r["version"] for r in self.conn.execute("SELECT version FROM schema_migrations").fetchall()}
        migration_files = sorted(migrations_dir.glob("*.sql"))
        for mf in migration_files:
            version = mf.name.split("_", 1)[0]
            if version in applied:
                continue
            sql = mf.read_text(encoding="utf-8")
            self.conn.executescript(sql)
            self.conn.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (version, utc_now_iso()),
            )
            self.conn.commit()

    # -----------------------
    # Helpers: player + index
    # -----------------------
    def ensure_player(self, handle: str, platform: str = "ubi") -> int:
        now = utc_now_iso()
        cur = self.conn.execute(
            "INSERT INTO players(platform, handle, created_at, updated_at) VALUES (?,?,?,?) "
            "ON CONFLICT(platform, handle) DO UPDATE SET updated_at=excluded.updated_at "
            "RETURNING player_id",
            (platform, handle, now, now),
        )
        row = cur.fetchone()
        self.conn.commit()
        return int(row["player_id"])

    def upsert_player_match_index(self, handle: str, match_rows: List[Dict[str, Any]], platform: str = "ubi") -> None:
        sql = (
            "INSERT INTO player_match_index(platform, handle, match_id, match_timestamp, session_type_name, gamemode, map_slug, map_name, is_rollback, full_match_available) "
            "VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(platform, handle, match_id) DO UPDATE SET "
            "match_timestamp=excluded.match_timestamp, session_type_name=excluded.session_type_name, gamemode=excluded.gamemode, "
            "map_slug=excluded.map_slug, map_name=excluded.map_name, is_rollback=excluded.is_rollback, full_match_available=excluded.full_match_available"
        )
        vals = []
        for r in match_rows:
            vals.append((
                platform, handle, r["match_id"], r.get("timestamp"), r.get("session_type_name"),
                r.get("gamemode"), r.get("map_slug"), r.get("map_name"),
                int(bool(r.get("is_rollback"))) if r.get("is_rollback") is not None else None,
                int(bool(r.get("full_match_available"))) if r.get("full_match_available") is not None else None,
            ))
        self.conn.executemany(sql, vals)
        self.conn.commit()

    def page_matches_all_v2_done(self, match_ids: List[str]) -> bool:
        if not match_ids:
            return True
        qmarks = ",".join("?" for _ in match_ids)
        row = self.conn.execute(
            f"SELECT COUNT(*) AS cnt FROM scrape_match_status WHERE match_id IN ({qmarks}) AND v2_done=1",
            match_ids,
        ).fetchone()
        return int(row["cnt"]) == len(match_ids)

    # -----------------------
    # Upserts: match + players + rounds
    # -----------------------
    def upsert_match(self, match: Dict[str, Any]) -> None:
        now = utc_now_iso()
        sql = (
            "INSERT INTO matches(match_id, timestamp, duration_ms, datacenter, session_type, session_game_mode, session_mode, gamemode, "
            "map_slug, map_name, is_surrender, is_forfeit, is_rollback, is_cancelled_by_ac, full_match_available, has_overwolf_roster, extended_data_available, inserted_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(match_id) DO UPDATE SET "
            "timestamp=excluded.timestamp, duration_ms=excluded.duration_ms, datacenter=excluded.datacenter, session_type=excluded.session_type, "
            "session_game_mode=excluded.session_game_mode, session_mode=excluded.session_mode, gamemode=excluded.gamemode, map_slug=excluded.map_slug, "
            "map_name=excluded.map_name, is_surrender=excluded.is_surrender, is_forfeit=excluded.is_forfeit, is_rollback=excluded.is_rollback, "
            "is_cancelled_by_ac=excluded.is_cancelled_by_ac, full_match_available=excluded.full_match_available, has_overwolf_roster=excluded.has_overwolf_roster, "
            "extended_data_available=excluded.extended_data_available, updated_at=excluded.updated_at"
        )
        self.conn.execute(sql, (
            match["match_id"],
            match.get("timestamp"),
            match.get("duration_ms"),
            match.get("datacenter"),
            match.get("session_type"),
            match.get("session_game_mode"),
            match.get("session_mode"),
            match.get("gamemode"),
            match.get("map_slug"),
            match.get("map_name"),
            int(bool(match.get("is_surrender"))) if match.get("is_surrender") is not None else None,
            int(bool(match.get("is_forfeit"))) if match.get("is_forfeit") is not None else None,
            int(bool(match.get("is_rollback"))) if match.get("is_rollback") is not None else None,
            int(bool(match.get("is_cancelled_by_ac"))) if match.get("is_cancelled_by_ac") is not None else None,
            int(bool(match.get("full_match_available"))) if match.get("full_match_available") is not None else None,
            int(bool(match.get("has_overwolf_roster"))) if match.get("has_overwolf_roster") is not None else None,
            int(bool(match.get("extended_data_available"))) if match.get("extended_data_available") is not None else None,
            now,
            now,
        ))
        self.conn.commit()

    def upsert_match_players(self, match_id: str, rows: List[Dict[str, Any]]) -> None:
        sql = (
            "INSERT INTO match_players(match_id, player_uuid, handle, team_id, result, has_won, kills, deaths, assists, headshots, team_kills, "
            "first_bloods, first_deaths, clutches, clutches_lost, rounds_played, rounds_won, rounds_lost, rank_points, rank_name, rank_points_delta, "
            "kd_ratio, hs_pct, esr, raw_stats_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(match_id, player_uuid) DO UPDATE SET "
            "handle=excluded.handle, team_id=excluded.team_id, result=excluded.result, has_won=excluded.has_won, "
            "kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists, headshots=excluded.headshots, team_kills=excluded.team_kills, "
            "first_bloods=excluded.first_bloods, first_deaths=excluded.first_deaths, clutches=excluded.clutches, clutches_lost=excluded.clutches_lost, "
            "rounds_played=excluded.rounds_played, rounds_won=excluded.rounds_won, rounds_lost=excluded.rounds_lost, "
            "rank_points=excluded.rank_points, rank_name=excluded.rank_name, rank_points_delta=excluded.rank_points_delta, "
            "kd_ratio=excluded.kd_ratio, hs_pct=excluded.hs_pct, esr=excluded.esr, raw_stats_json=excluded.raw_stats_json"
        )
        vals = []
        for r in rows:
            vals.append((
                match_id, r["player_uuid"], r.get("handle"), r.get("team_id"), r.get("result"), int(bool(r.get("has_won"))) if r.get("has_won") is not None else None,
                r.get("kills"), r.get("deaths"), r.get("assists"), r.get("headshots"), r.get("team_kills"),
                r.get("first_bloods"), r.get("first_deaths"), r.get("clutches"), r.get("clutches_lost"),
                r.get("rounds_played"), r.get("rounds_won"), r.get("rounds_lost"),
                r.get("rank_points"), r.get("rank_name"), r.get("rank_points_delta"),
                r.get("kd_ratio"), r.get("hs_pct"), r.get("esr"),
                json.dumps(r.get("raw_stats", {}), separators=(",", ":"), ensure_ascii=False),
            ))
        self.conn.executemany(sql, vals)
        self.conn.commit()

    def upsert_rounds(self, match_id: str, rows: List[Dict[str, Any]]) -> None:
        sql = (
            "INSERT INTO rounds(match_id, round_id, winner_team_color, winner_team_id, win_condition, bomb_site_id, attacking_team_color, attacking_team_id, "
            "v2_round_end_reason_id, v2_round_end_reason_name, v2_winner_side_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(match_id, round_id) DO UPDATE SET "
            "winner_team_color=excluded.winner_team_color, winner_team_id=excluded.winner_team_id, win_condition=excluded.win_condition, bomb_site_id=excluded.bomb_site_id, "
            "attacking_team_color=excluded.attacking_team_color, attacking_team_id=excluded.attacking_team_id, "
            "v2_round_end_reason_id=excluded.v2_round_end_reason_id, v2_round_end_reason_name=excluded.v2_round_end_reason_name, v2_winner_side_id=excluded.v2_winner_side_id"
        )
        vals = []
        for r in rows:
            vals.append((
                match_id, r["round_id"], r.get("winner_team_color"), r.get("winner_team_id"),
                r.get("win_condition"), r.get("bomb_site_id"),
                r.get("attacking_team_color"), r.get("attacking_team_id"),
                r.get("v2_round_end_reason_id"), r.get("v2_round_end_reason_name"), r.get("v2_winner_side_id"),
            ))
        self.conn.executemany(sql, vals)
        self.conn.commit()

    def upsert_player_rounds(self, match_id: str, rows: List[Dict[str, Any]]) -> None:
        sql = (
            "INSERT INTO player_rounds(match_id, round_id, player_uuid, handle, team_id, side_id, operator_id, kills, deaths, assists, headshots, score, plants, trades, "
            "is_disconnected, first_blood, first_death, clutch_won, clutch_lost, killed_players_json, killed_by_player_uuid) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(match_id, round_id, player_uuid) DO UPDATE SET "
            "handle=excluded.handle, team_id=excluded.team_id, side_id=excluded.side_id, operator_id=excluded.operator_id, "
            "kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists, headshots=excluded.headshots, score=excluded.score, plants=excluded.plants, trades=excluded.trades, "
            "is_disconnected=excluded.is_disconnected, first_blood=excluded.first_blood, first_death=excluded.first_death, clutch_won=excluded.clutch_won, clutch_lost=excluded.clutch_lost, "
            "killed_players_json=excluded.killed_players_json, killed_by_player_uuid=excluded.killed_by_player_uuid"
        )
        vals = []
        for r in rows:
            vals.append((
                match_id, r["round_id"], r["player_uuid"], r.get("handle"), r.get("team_id"), r.get("side_id"), r.get("operator_id"),
                r.get("kills"), r.get("deaths"), r.get("assists"), r.get("headshots"),
                r.get("score"), r.get("plants"), r.get("trades"),
                int(bool(r.get("is_disconnected"))) if r.get("is_disconnected") is not None else None,
                int(bool(r.get("first_blood"))) if r.get("first_blood") is not None else None,
                int(bool(r.get("first_death"))) if r.get("first_death") is not None else None,
                int(bool(r.get("clutch_won"))) if r.get("clutch_won") is not None else None,
                int(bool(r.get("clutch_lost"))) if r.get("clutch_lost") is not None else None,
                json.dumps(r.get("killed_players", []), separators=(",", ":"), ensure_ascii=False) if r.get("killed_players") is not None else None,
                r.get("killed_by_player_uuid"),
            ))
        self.conn.executemany(sql, vals)
        self.conn.commit()

    def upsert_kill_events(self, match_id: str, rows: List[Dict[str, Any]]) -> None:
        sql = (
            "INSERT OR IGNORE INTO kill_events(match_id, round_id, timestamp_ms, attacker_uuid, victim_uuid) VALUES (?,?,?,?,?)"
        )
        vals = []
        for r in rows:
            vals.append((match_id, r["round_id"], r["timestamp_ms"], r.get("attacker_uuid"), r["victim_uuid"]))
        self.conn.executemany(sql, vals)
        self.conn.commit()

    def upsert_raw_payload(self, match_id: str, source: str, payload: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO raw_payloads(match_id, source, fetched_at, payload_json) VALUES (?,?,?,?) "
            "ON CONFLICT(match_id, source) DO UPDATE SET fetched_at=excluded.fetched_at, payload_json=excluded.payload_json",
            (match_id, source, utc_now_iso(), json.dumps(payload, ensure_ascii=False)),
        )
        self.conn.commit()

    # -----------------------
    # Poison-match status
    # -----------------------
    def get_match_status(self, match_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM scrape_match_status WHERE match_id=?", (match_id,)).fetchone()

    def mark_match_success(self, match_id: str, v2_done: bool, v1_done: bool) -> None:
        now = utc_now_iso()
        self.conn.execute(
            "INSERT INTO scrape_match_status(match_id, v2_done, v1_done, attempts, last_error, next_retry_after, updated_at) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(match_id) DO UPDATE SET "
            "v2_done=excluded.v2_done, v1_done=excluded.v1_done, attempts=0, last_error=NULL, next_retry_after=NULL, updated_at=excluded.updated_at",
            (match_id, int(v2_done), int(v1_done), 0, None, None, now),
        )
        self.conn.commit()

    def mark_match_failure(self, match_id: str, error: str, max_attempts: int, cooldown_days: int) -> sqlite3.Row:
        now = utc_now_iso()
        row = self.get_match_status(match_id)
        attempts = int(row["attempts"]) if row else 0
        attempts += 1
        next_retry_after = None
        if attempts >= max_attempts:
            next_retry_after = (_dt.datetime.now(tz=_dt.timezone.utc) + _dt.timedelta(days=cooldown_days)).isoformat()
        self.conn.execute(
            "INSERT INTO scrape_match_status(match_id, v2_done, v1_done, attempts, last_error, next_retry_after, updated_at) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(match_id) DO UPDATE SET "
            "attempts=excluded.attempts, last_error=excluded.last_error, next_retry_after=excluded.next_retry_after, updated_at=excluded.updated_at",
            (match_id, 0, 0, attempts, error[:4000], next_retry_after, now),
        )
        self.conn.commit()
        return self.get_match_status(match_id)

    def should_skip_poison(self, match_id: str, max_attempts: int, force_retry: bool) -> bool:
        if force_retry:
            return False
        row = self.get_match_status(match_id)
        if not row:
            return False
        attempts = int(row["attempts"])
        if attempts < max_attempts:
            return False
        nra = row["next_retry_after"]
        if not nra:
            return False
        try:
            dt = _dt.datetime.fromisoformat(nra)
        except Exception:
            return False
        return dt > _dt.datetime.now(tz=_dt.timezone.utc)

    def v2_done(self, match_id: str) -> bool:
        row = self.get_match_status(match_id)
        return bool(row and int(row["v2_done"]) == 1)

    def v1_done(self, match_id: str) -> bool:
        row = self.get_match_status(match_id)
        return bool(row and int(row["v1_done"]) == 1)
