"""Create a consistent SQLite backup using SQLite's online backup API."""
import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="bot.db", help="source database path")
    parser.add_argument("--out-dir", default="backups", help="backup output directory")
    args = parser.parse_args()

    source = Path(args.db).resolve()
    if not source.exists():
        raise SystemExit(f"Database not found: {source}")

    output_dir = Path(args.out_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = output_dir / f"bot-{stamp}.sqlite3"

    with sqlite3.connect(source) as src, sqlite3.connect(destination) as dst:
        src.backup(dst)
        result = dst.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            destination.unlink(missing_ok=True)
            raise SystemExit(f"Backup integrity check failed: {result}")

    print(f"Backup created and verified: {destination}")


if __name__ == "__main__":
    main()
