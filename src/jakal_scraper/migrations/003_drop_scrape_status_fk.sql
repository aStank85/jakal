DROP TABLE IF EXISTS scrape_match_status;

CREATE TABLE scrape_match_status (
    match_id TEXT PRIMARY KEY,
    v2_done INTEGER NOT NULL DEFAULT 0,
    v1_done INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_after TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_scrape_match_status_attempts
ON scrape_match_status(attempts);
