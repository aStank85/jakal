PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL
);

-- Players tracked by handle (UBI)
CREATE TABLE IF NOT EXISTS players (
  player_id INTEGER PRIMARY KEY AUTOINCREMENT,
  platform TEXT NOT NULL DEFAULT 'ubi',
  handle TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(platform, handle)
);

-- Association of a tracked player to a match_id as seen in their match list.
-- This is what enables incremental "stop when page fully known".
CREATE TABLE IF NOT EXISTS player_match_index (
  platform TEXT NOT NULL DEFAULT 'ubi',
  handle TEXT NOT NULL,
  match_id TEXT NOT NULL,
  match_timestamp TEXT,
  session_type_name TEXT,
  gamemode TEXT,
  map_slug TEXT,
  map_name TEXT,
  is_rollback INTEGER,
  full_match_available INTEGER,
  PRIMARY KEY (platform, handle, match_id)
);

-- Global match metadata (one per match)
CREATE TABLE IF NOT EXISTS matches (
  match_id TEXT PRIMARY KEY,
  timestamp TEXT,
  duration_ms INTEGER,
  datacenter TEXT,
  session_type TEXT,
  session_game_mode TEXT,
  session_mode TEXT,
  gamemode TEXT,
  map_slug TEXT,
  map_name TEXT,
  is_surrender INTEGER,
  is_forfeit INTEGER,
  is_rollback INTEGER,
  is_cancelled_by_ac INTEGER,
  full_match_available INTEGER,
  has_overwolf_roster INTEGER,
  extended_data_available INTEGER,
  inserted_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- 10 players per match (v2 overview segments)
CREATE TABLE IF NOT EXISTS match_players (
  match_id TEXT NOT NULL,
  player_uuid TEXT NOT NULL,
  handle TEXT,
  team_id INTEGER,
  result TEXT,
  has_won INTEGER,

  kills INTEGER,
  deaths INTEGER,
  assists INTEGER,
  headshots INTEGER,
  team_kills INTEGER,
  first_bloods INTEGER,
  first_deaths INTEGER,
  clutches INTEGER,
  clutches_lost INTEGER,

  rounds_played INTEGER,
  rounds_won INTEGER,
  rounds_lost INTEGER,

  rank_points INTEGER,
  rank_name TEXT,
  rank_points_delta INTEGER,

  raw_stats_json TEXT,

  PRIMARY KEY (match_id, player_uuid),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Round outcomes (prefer v1), plus optional v2 round-overview fields
CREATE TABLE IF NOT EXISTS rounds (
  match_id TEXT NOT NULL,
  round_id INTEGER NOT NULL,

  winner_team_color TEXT,
  winner_team_id INTEGER,
  win_condition TEXT,
  bomb_site_id TEXT,
  attacking_team_color TEXT,
  attacking_team_id INTEGER,

  v2_round_end_reason_id TEXT,
  v2_round_end_reason_name TEXT,
  v2_winner_side_id TEXT,

  PRIMARY KEY (match_id, round_id),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Per-player per-round rows (merge v1+v2)
CREATE TABLE IF NOT EXISTS player_rounds (
  match_id TEXT NOT NULL,
  round_id INTEGER NOT NULL,
  player_uuid TEXT NOT NULL,

  handle TEXT,
  team_id INTEGER,
  side_id TEXT,
  operator_id TEXT,

  kills INTEGER,
  deaths INTEGER,
  assists INTEGER,
  headshots INTEGER,
  score INTEGER,
  plants INTEGER,
  trades INTEGER,

  is_disconnected INTEGER,

  first_blood INTEGER,
  first_death INTEGER,
  clutch_won INTEGER,
  clutch_lost INTEGER,

  killed_players_json TEXT,
  killed_by_player_uuid TEXT,

  PRIMARY KEY (match_id, round_id, player_uuid),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Kill events (v1), used for first blood derivation and deeper analytics.
CREATE TABLE IF NOT EXISTS kill_events (
  match_id TEXT NOT NULL,
  round_id INTEGER NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  attacker_uuid TEXT,
  victim_uuid TEXT NOT NULL,
  PRIMARY KEY (match_id, round_id, timestamp_ms, victim_uuid),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Optional: store raw payloads for forensic debugging.
CREATE TABLE IF NOT EXISTS raw_payloads (
  match_id TEXT NOT NULL,
  source TEXT NOT NULL, -- 'v2_summary' | 'v1_ingest' | 'v2_list_page'
  fetched_at TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (match_id, source),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);
