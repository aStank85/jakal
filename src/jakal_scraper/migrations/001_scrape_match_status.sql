CREATE TABLE IF NOT EXISTS scrape_match_status (
  match_id TEXT PRIMARY KEY,
  v2_done INTEGER NOT NULL DEFAULT 0,
  v1_done INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  next_retry_after TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);
