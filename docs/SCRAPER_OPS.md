# Scraper Ops

This scraper subsystem uses its own SQLite database and should be operated separately from the main app database.

## Apply Migrations

Run migrations against a dedicated scraper database file:

```powershell
python scripts/migrate.py --db jakal_scraper.sqlite
```

This creates the scraper schema and applies all migration files from `src/jakal_scraper/migrations/`.

## First Sync

Use a headed first run so you can manually handle consent, login friction, or captcha and save the browser storage state for later runs:

```powershell
python scripts/scrape.py --db jakal_scraper.sqlite --player SaucedZyn --headed --save-state storage_state.json --max-pages 1
```

What this does:

- Uses a dedicated scraper DB at `jakal_scraper.sqlite`.
- Opens a visible browser window.
- Fetches one page of match history with `--max-pages 1`.
- Saves cookies and local session state to `storage_state.json`.

After the run finishes, validate the data:

```powershell
python scripts/validate_sync.py --db jakal_scraper.sqlite --player SaucedZyn
```

## Incremental Sync

After the first headed run, use the saved storage state for normal incremental syncs:

```powershell
python scripts/scrape.py --db jakal_scraper.sqlite --player SaucedZyn --state storage_state.json
```

This reuses the saved cookies and runs headless by default.

## Force Retry

If a match has been quarantined by poison-match handling, force a retry with:

```powershell
python scripts/scrape.py --db jakal_scraper.sqlite --player SaucedZyn --state storage_state.json --force-retry
```

`--force-retry` bypasses the poison cooldown gate for the run and attempts failed matches again.

## Poison Match Cooldown

The scraper tracks repeated match-detail failures in `scrape_match_status`.

- `attempts` counts consecutive failures for a match.
- Once `attempts` reaches the configured max, the match is put on cooldown.
- `next_retry_after` stores the UTC timestamp after which the scraper may try that match again.
- While the cooldown is active, normal incremental sync skips that match instead of retrying it every run.
- A successful reprocess clears the failure state and resets attempts to `0`.

The default behavior from `scripts/scrape.py` is:

- `--max-attempts 3`
- `--poison-cooldown-days 7`

## Manual Inspection With sqlite3

If you have the `sqlite3` CLI installed, you can inspect scraper state directly.

Open the scraper DB:

```powershell
sqlite3 jakal_scraper.sqlite
```

Check tracked matches for a player:

```sql
SELECT handle, COUNT(*) AS match_rows
FROM player_match_index
WHERE handle = 'SaucedZyn'
GROUP BY handle;
```

Inspect poison-match status:

```sql
SELECT match_id, v2_done, v1_done, attempts, next_retry_after, last_error
FROM scrape_match_status
ORDER BY updated_at DESC
LIMIT 20;
```

Find matches currently on cooldown:

```sql
SELECT match_id, attempts, next_retry_after
FROM scrape_match_status
WHERE next_retry_after IS NOT NULL
ORDER BY next_retry_after DESC;
```

See how many matches have full v1 round detail:

```sql
SELECT v1_done, COUNT(*) AS matches
FROM scrape_match_status
WHERE v2_done = 1
GROUP BY v1_done
ORDER BY v1_done DESC;
```

Exit sqlite3:

```sql
.quit
```

## Recommended First-Run Flow

1. Run migrations.
2. Run the first headed sync with `--save-state`.
3. Run `scripts/validate_sync.py`.
4. Inspect `scrape_match_status` if any validation check fails.
5. Move to headless incremental syncs with `--state storage_state.json`.
