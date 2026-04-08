from __future__ import annotations
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jakal_scraper.db import SQLiteStore

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite DB")
    args = ap.parse_args()

    db_path = Path(args.db)
    store = SQLiteStore.open(db_path)
    try:
        migrations_dir = Path(__file__).resolve().parents[1] / "src" / "jakal_scraper" / "migrations"
        store.apply_migrations(migrations_dir)
        print(f"OK: migrated {db_path}")
    finally:
        store.close()

if __name__ == "__main__":
    main()
