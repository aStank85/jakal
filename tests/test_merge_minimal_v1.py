import json
from pathlib import Path
from src.jakal_scraper.scraper.parse_v2 import V2Parsed
from src.jakal_scraper.scraper.parse_v1 import parse_v1_ingest
from src.jakal_scraper.scraper.merge import merge_v1_v2

def test_merge_v1_first_blood_rules():
    # Minimal fake v2 parsed (only need match_players + player_rounds for merge)
    v2 = V2Parsed(
        match={"match_id":"m1"},
        match_players=[
            {"player_uuid":"p1","handle":"Alpha","team_id":0},
            {"player_uuid":"p2","handle":"Bravo","team_id":1},
        ],
        player_rounds=[
            {"player_uuid":"p1","round_id":1,"team_id":0,"side_id":"attacker","operator_id":"ash","is_disconnected":False,"killed_players":["p2"],"killed_by_player_uuid":None,"first_bloods":1,"first_deaths":0,"clutches":0,"clutches_lost":0},
            {"player_uuid":"p2","round_id":1,"team_id":1,"side_id":"defender","operator_id":"jager","is_disconnected":False,"killed_players":[],"killed_by_player_uuid":"p1","first_bloods":0,"first_deaths":1,"clutches":0,"clutches_lost":0},
        ],
        round_overviews=[],
    )

    v1_path = Path(__file__).resolve().parent / "fixtures" / "v1_minimal.json"
    v1 = parse_v1_ingest(json.loads(v1_path.read_text(encoding="utf-8")))
    merged = merge_v1_v2(v2, v1)

    # First blood round 1: p1 first_blood, p2 first_death
    pr1 = [r for r in merged.player_rounds if r["round_id"] == 1]
    by = {r["player_uuid"]: r for r in pr1}
    assert by["p1"]["first_blood"] is True
    assert by["p2"]["first_death"] is True
