# tests/helpers.py

import os
import json
from src.database import Database


def create_test_db() -> Database:
    """Create a fresh test database."""
    test_path = 'data/test_jakal.db'
    if os.path.exists(test_path):
        os.remove(test_path)
    return Database(db_path=test_path)


def _make_snapshot(db: Database, username: str, kd: float, win_pct: float,
                   hs_pct: float, kpr: float, apr: float, dpr: float,
                   first_bloods: int, first_deaths: int,
                   kills: int, deaths: int, assists: int,
                   rounds_played: int, matches: int,
                   clutch_1v1_w: int, clutch_1v1_l: int,
                   clutch_1v2_w: int = 0, clutch_1v2_l: int = 0):
    """Insert a synthetic snapshot directly into the database."""
    player_id = db.add_player(username)

    clutches = {
        'total': clutch_1v1_w + clutch_1v2_w,
        'lost_total': clutch_1v1_l + clutch_1v2_l,
        '1v1': clutch_1v1_w,
        'lost_1v1': clutch_1v1_l,
        '1v2': clutch_1v2_w,
        'lost_1v2': clutch_1v2_l,
        '1v3': 0, 'lost_1v3': 0,
        '1v4': 0, 'lost_1v4': 0,
        '1v5': 0, 'lost_1v5': 0,
    }

    wins = int(matches * win_pct / 100)
    losses = matches - wins
    headshots = int(kills * hs_pct / 100)

    cursor = db.conn.cursor()
    cursor.execute("""
        INSERT INTO stats_snapshots (
            player_id, snapshot_date, snapshot_time, season,
            abandons, matches, wins, losses, match_win_pct, time_played_hours,
            rounds_played, rounds_wins, rounds_losses, rounds_win_pct, disconnected,
            kills, deaths, assists, kd, kills_per_round, deaths_per_round,
            assists_per_round, headshots, hs_pct, first_bloods, first_deaths,
            teamkills, esr,
            clutches_data,
            aces, kills_3k, kills_4k, kills_2k, kills_1k,
            current_rank, max_rank, rank_points, max_rank_points, trn_elo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        player_id, '2025-01-15', '12:00', 'Y10S4',
        0, matches, wins, losses, win_pct, 200.0,
        rounds_played, int(rounds_played * 0.52), int(rounds_played * 0.48), 52.0, 0,
        kills, deaths, assists, kd, kpr, dpr,
        apr, headshots, hs_pct, first_bloods, first_deaths,
        5, 0.0,
        json.dumps(clutches),
        1, 10, 5, 30, 80,
        20, 22, 3200, 3500, 2800
    ))
    db.conn.commit()


def add_sample_players(db: Database):
    """Add 5 sample players (Stack A - your team) with varied stat profiles."""
    # PlayerA - Fragger type: high K/D, high kills
    _make_snapshot(db, "PlayerA",
                   kd=1.38, win_pct=52.1, hs_pct=54.0, kpr=0.85, apr=0.15, dpr=0.62,
                   first_bloods=180, first_deaths=120, kills=3200, deaths=2320, assists=560,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=45, clutch_1v1_l=25)

    # PlayerB - Support type: high assists, lower K/D
    _make_snapshot(db, "PlayerB",
                   kd=0.95, win_pct=51.2, hs_pct=42.0, kpr=0.58, apr=0.38, dpr=0.61,
                   first_bloods=90, first_deaths=130, kills=2180, deaths=2295, assists=1430,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=30, clutch_1v1_l=40)

    # PlayerC - Entry type: high aggression, good entry efficiency
    _make_snapshot(db, "PlayerC",
                   kd=1.05, win_pct=49.8, hs_pct=51.0, kpr=0.72, apr=0.20, dpr=0.69,
                   first_bloods=220, first_deaths=160, kills=2710, deaths=2580, assists=750,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=35, clutch_1v1_l=30)

    # PlayerD - Anchor type: good clutch stats, low deaths
    _make_snapshot(db, "PlayerD",
                   kd=1.12, win_pct=53.4, hs_pct=48.0, kpr=0.68, apr=0.22, dpr=0.55,
                   first_bloods=130, first_deaths=110, kills=2560, deaths=2286, assists=830,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=55, clutch_1v1_l=20, clutch_1v2_w=8, clutch_1v2_l=12)

    # PlayerE - Carry type: high overall impact
    _make_snapshot(db, "PlayerE",
                   kd=1.28, win_pct=55.0, hs_pct=56.0, kpr=0.80, apr=0.18, dpr=0.63,
                   first_bloods=200, first_deaths=140, kills=3010, deaths=2352, assists=680,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=50, clutch_1v1_l=22, clutch_1v2_w=5, clutch_1v2_l=10)


def add_opponent_players(db: Database):
    """Add 5 opponent players (Stack B) with slightly different profiles."""
    # EnemyA - Strong fragger, high HS%
    _make_snapshot(db, "EnemyA",
                   kd=1.42, win_pct=50.5, hs_pct=61.0, kpr=0.88, apr=0.12, dpr=0.62,
                   first_bloods=190, first_deaths=130, kills=3310, deaths=2330, assists=450,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=40, clutch_1v1_l=30)

    # EnemyB - Support with better teamplay
    _make_snapshot(db, "EnemyB",
                   kd=0.92, win_pct=49.1, hs_pct=40.0, kpr=0.55, apr=0.42, dpr=0.60,
                   first_bloods=80, first_deaths=140, kills=2070, deaths=2250, assists=1580,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=25, clutch_1v1_l=45)

    # EnemyC - Aggressive entry
    _make_snapshot(db, "EnemyC",
                   kd=1.00, win_pct=48.3, hs_pct=49.0, kpr=0.70, apr=0.18, dpr=0.70,
                   first_bloods=200, first_deaths=180, kills=2630, deaths=2630, assists=680,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=28, clutch_1v1_l=35)

    # EnemyD - Anchor
    _make_snapshot(db, "EnemyD",
                   kd=1.08, win_pct=51.0, hs_pct=46.0, kpr=0.65, apr=0.25, dpr=0.60,
                   first_bloods=120, first_deaths=100, kills=2450, deaths=2268, assists=940,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=48, clutch_1v1_l=22, clutch_1v2_w=6, clutch_1v2_l=14)

    # EnemyE - Carry with high K/D but lower win rate
    _make_snapshot(db, "EnemyE",
                   kd=1.35, win_pct=49.5, hs_pct=58.0, kpr=0.82, apr=0.14, dpr=0.61,
                   first_bloods=210, first_deaths=150, kills=3085, deaths=2285, assists=530,
                   rounds_played=3760, matches=350,
                   clutch_1v1_w=42, clutch_1v1_l=28, clutch_1v2_w=4, clutch_1v2_l=11)
