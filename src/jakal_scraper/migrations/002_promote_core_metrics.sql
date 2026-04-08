-- Promote frequently-queried metrics out of JSON blobs.
-- These are present in the v2 overview segment stats as Tracker-computed values.

ALTER TABLE match_players ADD COLUMN kd_ratio REAL;
ALTER TABLE match_players ADD COLUMN hs_pct REAL;
ALTER TABLE match_players ADD COLUMN esr REAL;

-- Backfill from raw_stats_json when available; otherwise compute from typed columns.
UPDATE match_players
SET kd_ratio = COALESCE(
  json_extract(raw_stats_json, '$.kdRatio.value'),
  CASE
    WHEN kills IS NULL OR deaths IS NULL THEN NULL
    WHEN deaths = 0 THEN CAST(kills AS REAL)
    ELSE CAST(kills AS REAL) / CAST(deaths AS REAL)
  END
)
WHERE kd_ratio IS NULL;

UPDATE match_players
SET hs_pct = COALESCE(
  json_extract(raw_stats_json, '$.headshotPct.value'),
  CASE
    WHEN kills IS NULL OR kills = 0 OR headshots IS NULL THEN NULL
    ELSE 100.0 * CAST(headshots AS REAL) / CAST(kills AS REAL)
  END
)
WHERE hs_pct IS NULL;

UPDATE match_players
SET esr = COALESCE(
  json_extract(raw_stats_json, '$.esr.value'),
  CASE
    WHEN first_bloods IS NULL OR first_deaths IS NULL THEN NULL
    WHEN (first_bloods + first_deaths) = 0 THEN NULL
    ELSE 100.0 * CAST(first_bloods AS REAL) / CAST((first_bloods + first_deaths) AS REAL)
  END
)
WHERE esr IS NULL;

-- Helpful indexes for plugin queries.
CREATE INDEX IF NOT EXISTS idx_match_players_player_uuid ON match_players(player_uuid);
CREATE INDEX IF NOT EXISTS idx_match_players_handle ON match_players(handle);
CREATE INDEX IF NOT EXISTS idx_match_players_kd_ratio ON match_players(kd_ratio);
CREATE INDEX IF NOT EXISTS idx_match_players_hs_pct ON match_players(hs_pct);
CREATE INDEX IF NOT EXISTS idx_match_players_esr ON match_players(esr);
