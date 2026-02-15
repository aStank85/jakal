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


def test_skip_existing_match_ids(monkeypatch):
    client = TrackerAPIClient(sleep_seconds=0)
    monkeypatch.setattr(
        client,
        "get_all_matches",
        lambda username, max_pages=None, since_date=None, show_progress=False: [
            {"match_id": "m1", "map": "Oregon", "timestamp": "2026-02-01T00:00:00+00:00"},
            {"match_id": "m2", "map": "Bank", "timestamp": "2026-02-02T00:00:00+00:00"},
        ],
    )
    called_ids = []
    monkeypatch.setattr(client, "get_match_detail", lambda match_id: called_ids.append(match_id) or {"match_id": match_id, "players": [], "round_outcomes": [], "player_rounds": []})
    monkeypatch.setattr("src.api_client.time.sleep", lambda *_: None)
    monkeypatch.setattr("src.api_client.random.uniform", lambda a, b: 0.0)

    rows = client.scrape_full_match_history("SaucedZyn", skip_match_ids={"m1"})
    assert [r["match_id"] for r in rows] == ["m2"]
    assert called_ids == ["m2"]


def test_batch_pause_every_10_details(monkeypatch):
    client = TrackerAPIClient(sleep_seconds=0, detail_batch_size=10, detail_batch_pause_seconds=15.0)
    monkeypatch.setattr(
        client,
        "get_all_matches",
        lambda username, max_pages=None, since_date=None, show_progress=False: [
            {"match_id": f"m{i}", "map": "Oregon", "timestamp": "2026-02-01T00:00:00+00:00"}
            for i in range(1, 12)
        ],
    )
    monkeypatch.setattr(
        client,
        "get_match_detail",
        lambda match_id: {"match_id": match_id, "players": [], "round_outcomes": [], "player_rounds": []},
    )
    sleep_calls = []
    monkeypatch.setattr("src.api_client.time.sleep", lambda s: sleep_calls.append(s))
    monkeypatch.setattr("src.api_client.random.uniform", lambda a, b: 3.0)

    rows = client.scrape_full_match_history("SaucedZyn")
    assert len(rows) == 11
    assert 15.0 in sleep_calls


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


def test_parse_profile_season_stats(client):
    parsed = client.parse_profile(_load_json("saucedzyn_home.json"))
    season = parsed["season_stats"]
    assert season["matchesPlayed"] > 0
    required = {
        "matchesPlayed",
        "matchesWon",
        "matchesLost",
        "matchesAbandoned",
        "kills",
        "deaths",
        "assists",
        "headshots",
        "firstBloods",
        "firstDeaths",
        "clutches",
        "clutchesLost",
        "clutches1v1",
        "clutches1v2",
        "clutches1v3",
        "clutches1v4",
        "clutches1v5",
        "clutchesLost1v1",
        "clutchesLost1v2",
        "clutchesLost1v3",
        "clutchesLost1v4",
        "clutchesLost1v5",
        "kills1K",
        "kills2K",
        "kills3K",
        "kills4K",
        "kills5K",
        "rankPoints",
        "maxRankPoints",
        "rank",
        "kdRatio",
        "headshotPct",
        "esr",
        "killsPerRound",
        "deathsPerRound",
        "assistsPerRound",
        "roundsPlayed",
        "roundsWon",
        "roundsLost",
        "winPercentage",
        "elo",
    }
    assert required.issubset(set(season.keys()))


def test_parse_profile_uuid(client):
    parsed = client.parse_profile(_load_json("saucedzyn_home.json"))
    assert parsed["uuid"] == "68cfcc8f-c91d-4d55-aa51-ddd6478932c9"
    assert parsed["username"] == "SaucedZyn"


def test_parse_profile_career_stats(client):
    parsed = client.parse_profile(_load_json("saucedzyn_home.json"))
    career = parsed["career_stats"]
    assert career["matchesPlayed"] > 0
    assert "kills" in career
    assert "deaths" in career


def test_parse_operator_stats(client):
    parsed = client.parse_operator_segments(_load_json("saucedzyn_operators.json"))
    assert len(parsed) == 2
    assert parsed[0]["operator_slug"] == "gridlock"
    assert parsed[0]["operator_name"] == "Gridlock"
    assert parsed[0]["rounds"] == 121
    assert parsed[1]["operator_name"] == "Kaid"
    assert parsed[1]["kd"] == pytest.approx(1.29)


def test_parse_map_stats(client):
    parsed = client.parse_map_segments(_load_json("saucedzyn_maps.json"))
    assert len(parsed) == 2
    assert parsed[0]["map_slug"] == "emerald-plains"
    assert parsed[0]["map_name"] == "Emerald Plains"
    assert parsed[0]["matches"] == 42
    assert parsed[1]["map_name"] == "Clubhouse"
    assert parsed[1]["win_pct"] == pytest.approx(51.9)


def test_get_map_stats_merges_side_payloads(client, monkeypatch):
    payloads = {
        "base": _load_json("saucedzyn_map.json"),
        "attacker": _load_json("saucedzyn_map_atk.json"),
        "defender": _load_json("saucedzyn_map_def.json"),
    }

    def fake_get(url, retry_429=True):
        if "side=attacker" in url:
            return payloads["attacker"]
        if "side=defender" in url:
            return payloads["defender"]
        return payloads["base"]

    monkeypatch.setattr(client, "_get_json", fake_get)
    maps = client.get_map_stats("SaucedZyn")
    assert len(maps) == 17
    assert all("atk_win_pct" in m and "def_win_pct" in m for m in maps)
    assert any((m.get("atk_win_pct") or 0) > 0 for m in maps)
    assert any((m.get("def_win_pct") or 0) > 0 for m in maps)
