"""TTC GTFS-Realtime poller (local-first version).

Fetches vehicle positions and trip updates, decodes the protobuf, flattens each
entity into a record, and writes raw data, parsed data, and a health log to ./data.

Run once and show sample records:  python ingestion/poller/poller.py --once
Run continuously:                  python ingestion/poller/poller.py
Run without saving files:          python ingestion/poller/poller.py --once --no-save
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

load_dotenv()
log = logging.getLogger("poller")

AGENCY = "ttc"
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
TIMEOUT_SECONDS = 15
FEEDS = {
    "vehicle_positions": os.getenv("TTC_GTFSRT_VEHICLE_URL", ""),
    "trip_updates": os.getenv("TTC_GTFSRT_TRIPUPDATE_URL", ""),
}


def opt(msg, field):
    """Return a field's value, or None when the feed did not set it."""
    try:
        return getattr(msg, field) if msg.HasField(field) else None
    except ValueError:
        return None


def parse_vehicle(entity, base: dict) -> dict:
    v = entity.vehicle
    pos = v.position
    return {
        **base,
        "entity_id": entity.id,
        "vehicle_id": opt(v.vehicle, "id"),
        "vehicle_label": opt(v.vehicle, "label"),
        "trip_id": opt(v.trip, "trip_id"),
        "route_id": opt(v.trip, "route_id"),
        "direction_id": opt(v.trip, "direction_id"),
        "start_date": opt(v.trip, "start_date"),
        "lat": opt(pos, "latitude"),
        "lon": opt(pos, "longitude"),
        "bearing": opt(pos, "bearing"),
        "speed": opt(pos, "speed"),
        "current_stop_sequence": opt(v, "current_stop_sequence"),
        "stop_id": opt(v, "stop_id"),
        "current_status": opt(v, "current_status"),
        "occupancy_status": opt(v, "occupancy_status"),
        "event_ts": opt(v, "timestamp"),
    }


def parse_trip_update(entity, base: dict) -> list[dict]:
    """One row per stop_time_update, so each row is a prediction for one stop."""
    tu = entity.trip_update
    common = {
        **base,
        "entity_id": entity.id,
        "trip_id": opt(tu.trip, "trip_id"),
        "route_id": opt(tu.trip, "route_id"),
        "direction_id": opt(tu.trip, "direction_id"),
        "start_date": opt(tu.trip, "start_date"),
        "trip_schedule_relationship": opt(tu.trip, "schedule_relationship"),
        "vehicle_id": opt(tu.vehicle, "id"),
        "trip_delay": opt(tu, "delay"),
        "event_ts": opt(tu, "timestamp"),
    }
    rows = []
    for stu in tu.stop_time_update:
        rows.append(
            {
                **common,
                "stop_id": opt(stu, "stop_id"),
                "stop_sequence": opt(stu, "stop_sequence"),
                "arrival_time": opt(stu.arrival, "time"),
                "arrival_delay": opt(stu.arrival, "delay"),
                "departure_time": opt(stu.departure, "time"),
                "departure_delay": opt(stu.departure, "delay"),
                "stop_schedule_relationship": opt(stu, "schedule_relationship"),
            }
        )
    return rows or [common]


def flatten(feed_name: str, feed, base: dict) -> list[dict]:
    rows: list[dict] = []
    for entity in feed.entity:
        if feed_name == "vehicle_positions" and entity.HasField("vehicle"):
            rows.append(parse_vehicle(entity, base))
        elif feed_name == "trip_updates" and entity.HasField("trip_update"):
            rows.extend(parse_trip_update(entity, base))
    return rows


def save_files(feed_name: str, raw: bytes, rows: list[dict], now: datetime) -> None:
    day = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%H%M%S")

    raw_dir = DATA_DIR / "raw" / feed_name / day
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / f"{stamp}.pb.gz").write_bytes(gzip.compress(raw))

    parsed_dir = DATA_DIR / "parsed" / feed_name / day
    parsed_dir.mkdir(parents=True, exist_ok=True)
    with gzip.open(parsed_dir / f"{stamp}.jsonl.gz", "wt", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def poll_once(feed_name: str, url: str, save: bool) -> tuple[dict, list[dict]]:
    """Fetch and decode one feed. Returns (health record, parsed rows)."""
    started = time.time()
    now = datetime.now(timezone.utc)
    health = {
        "ts": now.isoformat(),
        "feed": feed_name,
        "http_status": None,
        "bytes": 0,
        "latency_ms": None,
        "header_ts": None,
        "entities": 0,
        "records": 0,
        "decode_error": False,
        "error": None,
    }
    rows: list[dict] = []
    try:
        resp = requests.get(
            url, timeout=TIMEOUT_SECONDS, headers={"User-Agent": "transit-lakehouse/0.1"}
        )
        health["http_status"] = resp.status_code
        health["bytes"] = len(resp.content)
        health["latency_ms"] = int((time.time() - started) * 1000)
        resp.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(resp.content)

        base = {
            "agency": AGENCY,
            "ingest_ts": now.isoformat(),
            "feed_ts": feed.header.timestamp or None,
        }
        rows = flatten(feed_name, feed, base)
        health.update(
            header_ts=feed.header.timestamp or None,
            entities=len(feed.entity),
            records=len(rows),
        )
        if save:
            save_files(feed_name, resp.content, rows, now)
    except DecodeError as exc:
        health.update(decode_error=True, error=f"decode error: {exc}")
    except requests.RequestException as exc:
        health["error"] = str(exc)
    return health, rows


def log_health(health: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / "poll_log.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(health) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="TTC GTFS-Realtime poller")
    parser.add_argument("--once", action="store_true", help="poll once, print samples, exit")
    parser.add_argument("--no-save", action="store_true", help="do not write files to ./data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    missing = [name for name, url in FEEDS.items() if not url]
    if missing:
        raise SystemExit(f"Missing feed URL in .env for: {', '.join(missing)}")

    next_run = time.monotonic()
    try:
        while True:
            for name, url in FEEDS.items():
                health, rows = poll_once(name, url, save=not args.no_save)
                log_health(health)
                log.info(
                    "%s | http=%s | %s bytes | entities=%s | records=%s | %s ms%s",
                    name,
                    health["http_status"],
                    health["bytes"],
                    health["entities"],
                    health["records"],
                    health["latency_ms"],
                    f" | ERROR: {health['error']}" if health["error"] else "",
                )
                if args.once:
                    for row in rows[:2]:
                        print(json.dumps(row, indent=2))
            if args.once:
                break
            next_run += INTERVAL
            time.sleep(max(0.0, next_run - time.monotonic()))
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()