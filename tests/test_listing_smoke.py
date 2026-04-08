import json
from pathlib import Path
from src.jakal_scraper.scraper.listing import MatchListItem

def test_list_fixture_has_20():
    p = Path(__file__).resolve().parent / "fixtures" / "saucedzyn_matchtab.json"
    raw = json.loads(p.read_text(encoding="utf-8"))
    data = raw["data"]
    assert len(data["matches"]) == 20
    # sanity: first match has id
    assert data["matches"][0]["attributes"]["id"]
