"""Download the TTC static GTFS schedule, extract it, and check it against live trip updates.

Run the full load:      python ingestion/static_loader/static_loader.py
Re-run the check only:  python ingestion/static_loader/static_loader.py --check-only

Needs TTC_GTFS_STATIC_URL in .env. Output goes to ./data/static/<YYYY-MM-DD>/ (git-ignored).
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import logging
import os
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("static_loader")

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
STATIC_URL = os.getenv("TTC_GTFS_STATIC_URL", "")
KEY_FILES = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar.txt"]


def download(url: str, dest: Path) -> str:
    """Stream the zip to disk and return its SHA-256 hash."""
    sha = hashlib.sha256()
    with requests.get(url, stream=True, timeout=60,
                      headers={"User-Agent": "transit-lakehouse/0.1"}) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
                sha.update(chunk)
    return sha.hexdigest()


def read_rows(path: Path):
    with open(path, newline="", encoding="utf-8-sig") as fh:
        yield from csv.DictReader(fh)


def count_rows(path: Path) -> int:
    with open(path, "rb") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def calendar_range(path: Path) -> tuple[str | None, str | None]:
    starts, ends = [], []
    for row in read_rows(path):
        starts.append(row["start_date"])
        ends.append(row["end_date"])
    return (min(starts) if starts else None, max(ends) if ends else None)


def latest_parsed(feed: str) -> Path | None:
    files = sorted(DATA_DIR.glob(f"parsed/{feed}/*/*.jsonl.gz"))
    return files[-1] if files else None


def check_join(gtfs_dir: Path) -> None:
    """Do live trip IDs and stop sequences line up with the static schedule?"""
    path = latest_parsed("trip_updates")
    if path is None:
        log.warning("No saved trip updates found. Run the poller (without --no-save) first.")
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        live = [json.loads(line) for line in fh]

    scheduled = [r for r in live if r["trip_schedule_relationship"] in (0, None)]
    live_trips = {r["trip_id"] for r in scheduled if r["trip_id"]}
    static_trips = {row["trip_id"] for row in read_rows(gtfs_dir / "trips.txt")}
    found = live_trips & static_trips

    print("\nJOIN CHECK (live trip updates vs static schedule)")
    print(f"  live file:                         {path}")
    print(f"  scheduled live trips:              {len(live_trips):,}")
    print(f"  of those found in static trips:    {len(found):,} "
          f"({len(found) / len(live_trips):.1%})" if live_trips else "  no scheduled live trips")

    keys: dict[tuple[str, int], str] = {}
    for row in read_rows(gtfs_dir / "stop_times.txt"):
        if row["trip_id"] in found:
            keys[(row["trip_id"], int(row["stop_sequence"]))] = row["stop_id"]

    with_seq = [r for r in scheduled if r["stop_sequence"] is not None]
    seq_match = stop_match = 0
    for r in with_seq:
        static_stop = keys.get((r["trip_id"], r["stop_sequence"]))
        if static_stop is not None:
            seq_match += 1
            if static_stop == r["stop_id"]:
                stop_match += 1
    if with_seq:
        print(f"  live stop rows with stop_sequence: {len(with_seq):,}")
        print(f"  matched on (trip_id, sequence):    {seq_match:,} ({seq_match / len(with_seq):.1%})")
        print(f"  ...and stop_id also agrees:        {stop_match:,} ({stop_match / len(with_seq):.1%})")


def main() -> None:
    parser = argparse.ArgumentParser(description="TTC static GTFS loader")
    parser.add_argument("--check-only", action="store_true", help="skip download, run join check")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    static_root = DATA_DIR / "static"
    if args.check_only:
        folders = sorted(p for p in static_root.glob("*/gtfs") if p.is_dir())
        if not folders:
            raise SystemExit("No extracted static data found. Run without --check-only first.")
        check_join(folders[-1])
        return

    if not STATIC_URL:
        raise SystemExit("Missing TTC_GTFS_STATIC_URL in .env")

    out_dir = static_root / date.today().isoformat()
    gtfs_dir = out_dir / "gtfs"
    gtfs_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "ttc_gtfs.zip"

    log.info("Downloading static GTFS ...")
    sha = download(STATIC_URL, zip_path)
    log.info("Downloaded %.1f MB, sha256=%s", zip_path.stat().st_size / 1e6, sha[:12])

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(gtfs_dir)

    manifest = {
        "url": STATIC_URL,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "sha256": sha,
        "files": {},
    }
    print("\nSTATIC GTFS")
    for name in KEY_FILES:
        path = gtfs_dir / name
        if path.exists():
            manifest["files"][name] = count_rows(path)
            print(f"  {name:<16} {manifest['files'][name]:>12,} rows")
        else:
            print(f"  {name:<16} missing")

    cal = gtfs_dir / "calendar.txt"
    if cal.exists():
        start, end = calendar_range(cal)
        manifest["calendar_start"], manifest["calendar_end"] = start, end
        print(f"  calendar.txt service dates: {start} to {end}")

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    check_join(gtfs_dir)


if __name__ == "__main__":
    main()