import os
import tempfile

from src.database import Database
from src.plugins.v3_enemy_operator_threat import EnemyOperatorThreatPlugin


def test_enemy_operator_threat_plugin_returns_rows():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = Database(db_path)
    try:
        player_id = db.add_player("ThreatUser")
        db.save_player_rounds(
            player_id,
            "match-a",
            [
                {
                    "round_id": 1,
                    "player_id_tracker": "me",
                    "operator": "Smoke",
                    "result": "defeat",
                    "deaths": 1,
                    "killed_by_player_id": "enemy-1",
                    "killed_by_operator": "Ash",
                },
                {
                    "round_id": 2,
                    "player_id_tracker": "me",
                    "operator": "Mute",
                    "result": "victory",
                    "deaths": 0,
                },
            ],
            usernames_by_tracker_id={"me": "ThreatUser"},
        )
        db.conn.execute(
            "INSERT INTO scraped_match_cards (username, match_id, mode) VALUES (?, ?, ?)",
            ("ThreatUser", "match-a", "Ranked"),
        )
        db.conn.commit()

        analysis = EnemyOperatorThreatPlugin(db, "ThreatUser").analyze()
        assert analysis.get("error") is None
        assert analysis["baseline_win_rate"] == 50.0
        assert analysis["threats"][0]["operator"] == "Ash"
        assert analysis["threats"][0]["times_killed_by"] == 1
        assert analysis["scatter"]["points"][0]["operator"] == "Ash"
    finally:
        db.close()
        if os.path.exists(db_path):
            os.remove(db_path)
