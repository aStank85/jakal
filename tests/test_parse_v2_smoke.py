import json
from pathlib import Path
from src.jakal_scraper.scraper.parse_v2 import parse_v2_match

def test_parse_v2_match_smoke():
    p = Path(__file__).resolve().parent / "fixtures" / "match1.json"
    raw = json.loads(p.read_text(encoding="utf-8"))
    v2 = raw["data"]
    parsed = parse_v2_match(v2)
    assert parsed.match["match_id"]
    assert len(parsed.match_players) == 10
    assert len(parsed.player_rounds) > 0

    # Promoted convenience metrics should be present on overview rows.
    row0 = parsed.match_players[0]
    assert "kd_ratio" in row0
    assert "hs_pct" in row0
    assert "esr" in row0
