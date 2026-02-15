import json
import os
import tempfile
from io import BytesIO
from urllib.error import HTTPError

import pytest

from src.api_client import TrackerAPIClient
from src.database import Database


def _fixture_path(filename: str) -> str:
    candidates = [
        os.path.join(os.path.dirname(__file__), "fixtures", filename),
        os.path.join(os.path.dirname(__file__), "..", "..", "tests", "fixtures", filename),
        os.path.join(os.path.dirname(__file__), "..", "..", "json", filename),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError(filename)


def _load_json(filename: str):
    with open(_fixture_path(filename), "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def client():
    return TrackerAPIClient(sleep_seconds=0)


@pytest.fixture
def temp_db():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(db_path)
    try:
        yield database
    finally:
        database.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def test_parse_match_list(client):
    parsed = client.parse_match_list(_load_json("saucedzyn_matchtab.json"))
    assert len(parsed["matches"]) == 20
    assert parsed["next"] == 1
    assert parsed["matches"][0]["match_id"]
    assert "overview_segment" in parsed["matches"][0]


def test_parse_match_detail_segments(client):
    grouped = client.parse_match_detail_segments(_load_json("match1.json"))
    assert len(grouped["overview"]) == 10
    assert len(grouped["player-operator"]) == 10
    assert len(grouped["round-overview"]) > 0
    assert len(grouped["player-round"]) > 0


def test_parse_player_overview(client):
    grouped = client.parse_match_detail_segments(_load_json("match1.json"))
    player = client.parse_player_overview(grouped["overview"][0])
    required = {
        "player_id_tracker",
        "username",
        "team_id",
        "result",
        "kills",
        "deaths",
        "assists",
        "headshots",
        "first_bloods",
        "first_deaths",
        "clutches_won",
        "clutches_lost",
        "clutches_1v1",
        "clutches_1v2",
        "clutches_1v3",
        "clutches_1v4",
        "clutches_1v5",
        "kills_1k",
        "kills_2k",
        "kills_3k",
        "kills_4k",
        "kills_5k",
        "rounds_won",
        "rounds_lost",
        "rank_points",
        "rank_points_delta",
        "rank_points_previous",
        "kd_ratio",
        "hs_pct",
        "esr",
        "kills_per_round",
        "time_played_ms",
    }
    assert required.issubset(set(player.keys()))


def test_parse_player_round(client):
    grouped = client.parse_match_detail_segments(_load_json("match1.json"))
    segment = grouped["player-round"][0]
    parsed = client.parse_player_round(segment)
    assert parsed["operator"]
    assert parsed["side"] in {"attacker", "defender", "unknown"}
    assert parsed["result"] in {"victory", "defeat", None}


def test_parse_round_outcome(client):
    grouped = client.parse_match_detail_segments(_load_json("match1.json"))
    parsed = client.parse_round_outcome(grouped["round-overview"][0])
    assert "end_reason" in parsed
    assert parsed["winner_side"] in {"attacker", "defender", "unknown"}


def test_pagination_next_token(client, monkeypatch):
    pages = [
        _load_json("saucedzyn_matchtab.json"),
        _load_json("saucedzyn_matchtab_next.json"),
    ]
    called_urls = []

    def fake_get(url, retry_429=True):
        called_urls.append(url)
        return pages.pop(0)

    monkeypatch.setattr(client, "_get_json", fake_get)
    matches = client.get_all_matches("SaucedZyn", max_pages=2)

    assert len(matches) == 40
    assert "matches/ubi/SaucedZyn" in called_urls[0]
    assert "next=1" in called_urls[1]


def test_since_date_cutoff_stops_pagination(client, monkeypatch):
    page1 = {
        "matches": [
            {"match_id": "m1", "timestamp": "2026-02-14T06:35:25.404+00:00"},
            {"match_id": "m2", "timestamp": "2026-01-10T00:00:00+00:00"},
        ],
        "next": 1,
    }
    page2 = {
        "matches": [
            {"match_id": "m3", "timestamp": "2025-10-01T00:00:00+00:00"},
            {"match_id": "m4", "timestamp": "2025-09-15T00:00:00+00:00"},
        ],
        "next": 2,
    }
    pages = [page1, page2]
    calls = {"count": 0}

    def fake_get_match_list(username, next_token=None):
        calls["count"] += 1
        return pages.pop(0)

    monkeypatch.setattr(client, "get_match_list", fake_get_match_list)
    matches = client.get_all_matches("SaucedZyn", since_date="2025-11-01T00:00:00+00:00")

    assert [m["match_id"] for m in matches] == ["m1", "m2"]
    assert calls["count"] == 2


def test_rate_limit_retry(monkeypatch):
    import src.api_client as api_module

    client = TrackerAPIClient()
    calls = {"count": 0}

    def fake_urlopen(req, timeout=20):
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(req.full_url, 429, "Too Many Requests", hdrs=None, fp=BytesIO(b"{}"))
        return BytesIO(b'{"ok": true}')

    monkeypatch.setattr(api_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(api_module.time, "sleep", lambda *_: None)

    data = client._get_json("https://example.com/test")
    assert data["ok"] is True
    assert calls["count"] == 2


def test_save_match_detail_players(client, temp_db):
    temp_db.add_player("SaucedZyn")
    player_id = temp_db.get_player("SaucedZyn")["player_id"]

    grouped = client.parse_match_detail_segments(_load_json("match1.json"))
    players = [client.parse_player_overview(s) for s in grouped["overview"]]
    temp_db.save_match_detail_players(player_id, "match-1", players)

    rows = temp_db.get_match_detail_players(player_id, "match-1")
    assert len(rows) == 10
    assert rows[0]["player_id_tracker"]


def test_save_player_rounds(client, temp_db):
    temp_db.add_player("SaucedZyn")
    player_id = temp_db.get_player("SaucedZyn")["player_id"]

    grouped = client.parse_match_detail_segments(_load_json("match1.json"))
    players = [client.parse_player_overview(s) for s in grouped["overview"]]
    usernames_by_id = {
        p["player_id_tracker"]: p.get("username")
        for p in players
        if p.get("player_id_tracker")
    }
    rounds = [client.parse_player_round(s) for s in grouped["player-round"]]
    temp_db.save_player_rounds(player_id, "match-1", rounds, usernames_by_tracker_id=usernames_by_id)

    rows = temp_db.get_player_rounds(player_id, "match-1")
    assert len(rows) > 0
    assert "operator" in rows[0]
    assert rows[0]["side"] in {"attacker", "defender", "unknown"}
