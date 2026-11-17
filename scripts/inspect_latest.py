"""Profile the most recent saved poll so we can see what the TTC feeds really contain.

Run from the repo root after the poller has saved at least one poll:
    python scripts/inspect_latest.py
"""
from __future__ import annotations

import glob
import gzip
import json
from collections import Counter


def latest_file(feed: str) -> str:
    files = sorted(glob.glob(f"data/parsed/{feed}/*/*.jsonl.gz"))
    if not files:
        raise SystemExit(f"No saved files for {feed}. Run the poller without --no-save first.")
    return files[-1]


def load(path: str) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def pct(part: int, whole: int) -> str:
    return f"{part:,} ({part / whole:.1%})" if whole else "0"


def main() -> None:
    veh_path, trip_path = latest_file("vehicle_positions"), latest_file("trip_updates")
    veh, trips = load(veh_path), load(trip_path)
    print(f"Vehicle file: {veh_path}")
    print(f"Trip file:    {trip_path}\n")

    print("VEHICLE POSITIONS")
    print(f"  records:                 {len(veh):,}")
    print(f"  with trip_id:            {pct(sum(r['trip_id'] is not None for r in veh), len(veh))}")
    print(f"  with route_id:           {pct(sum(r['route_id'] is not None for r in veh), len(veh))}")
    stale = sum(1 for r in veh if r["event_ts"] and r["feed_ts"] and r["feed_ts"] - r["event_ts"] > 120)
    print(f"  stale (>120s behind feed): {pct(stale, len(veh))}")
    print(f"  distinct vehicles:       {len({r['vehicle_id'] for r in veh}):,}\n")

    print("TRIP UPDATES")
    print(f"  stop rows:               {len(trips):,}")
    print(f"  distinct trips:          {len({r['trip_id'] for r in trips}):,}")
    print(f"  distinct routes:         {len({r['route_id'] for r in trips}):,}")
    rel = Counter(r["trip_schedule_relationship"] for r in trips)
    print(f"  trip schedule relationship (value: rows): {dict(rel)}")
    print(f"  with arrival_time:       {pct(sum(r['arrival_time'] is not None for r in trips), len(trips))}")
    print(f"  with departure_time:     {pct(sum(r['departure_time'] is not None for r in trips), len(trips))}")
    any_delay = sum(r["arrival_delay"] is not None or r["departure_delay"] is not None for r in trips)
    print(f"  with any delay field:    {pct(any_delay, len(trips))}")
    print(f"  with vehicle_id:         {pct(sum(r['vehicle_id'] is not None for r in trips), len(trips))}\n")

    veh_trips = {r["trip_id"] for r in veh if r["trip_id"]}
    upd_trips = {r["trip_id"] for r in trips if r["trip_id"]}
    print("JOIN CHECK")
    print(f"  trips in both feeds:     {len(veh_trips & upd_trips):,}")


if __name__ == "__main__":
    main()