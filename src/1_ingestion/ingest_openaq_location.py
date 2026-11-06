import os
import json
import time
import hashlib
import argparse
import re
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
BASE_URL = "https://api.openaq.org/v3"
ROOT = Path("datas/raw/openaq")
LOCATIONS_FILE = ROOT / "india_pm25_locations.json"
PAGE_SIZE = 1000
DELAY = 0.3


def request_json(session, url, params=None):
    for attempt in range(6):
        try:
            response = session.get(url, params=params, timeout=(15, 90))
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 5:
                    wait = int(response.headers.get("Retry-After", 0))
                    time.sleep(wait if wait > 0 else min(2 ** attempt, 60))
                    continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == 5:
                raise
            time.sleep(min(2 ** attempt, 60))
    raise RuntimeError("OpenAQ request failed after retries")


def load_locations():
    with LOCATIONS_FILE.open(encoding="utf-8") as f:
        return json.load(f)


def select_location(locations, location_id=None, name=None):
    if location_id is not None:
        matches = [x for x in locations if str(x.get("id")) == str(location_id)]
    elif name:
        matches = [
            x for x in locations
            if name.lower() in (x.get("name") or "").lower()
        ]
    else:
        raise ValueError("Provide --location-id or --name")

    if len(matches) != 1:
        print("Matching locations:")
        for loc in matches[:50]:
            print(
                f"ID={loc.get('id')} | {loc.get('name')} | "
                f"{loc.get('locality')} | {loc.get('timezone')}"
            )
        if not matches:
            raise ValueError("No matching location. Try another name or ID.")
        raise ValueError(
            "Name matched multiple locations. Use --location-id to select one."
        )
    return matches[0]


def get_sensors(location):
    result = []
    for sensor in location.get("sensors", []):
        p = sensor.get("parameter") or {}
        if (p.get("name") or "").lower() in {"pm25", "pm2.5"}:
            result.append({
                "sensor_id": sensor["id"],
                "location_id": location["id"],
                "station_name": location.get("name"),
                "city_name": location.get("locality"),
                "state_name": None,
                "unit": p.get("units") or "µg/m³",
                "timezone": location.get("timezone"),
            })

    unique = {s["sensor_id"]: s for s in result}
    return list(unique.values())


def normalize(m, sensor):
    period = m.get("period") or {}
    dt = period.get("datetimeFrom") or {}
    timestamp = dt.get("local") or dt.get("utc")
    if not timestamp:
        return None

    # Match the current Bronze convention: keep the local clock time,
    # but store a timezone-naive timestamp.
    timestamp = timestamp.replace("Z", "")
    if len(timestamp) >= 25 and timestamp[19] in "+-":
        timestamp = timestamp[:19]

    try:
        value = float(m["value"]) if m.get("value") is not None else None
    except (TypeError, ValueError):
        value = None

    station_id = f"openaq_{sensor['location_id']}_{sensor['sensor_id']}"
    row = {
        "station_id": station_id,
        "station_name": sensor["station_name"],
        "state_name": sensor["state_name"],
        "city_name": sensor["city_name"],
        "parameter_name": "PM2.5",
        "unit": sensor["unit"],
        "collected_at": timestamp,
        "value": value,
        "source": "openaq",
        "qc_missing_value": value is None,
        "qc_negative_value": value is not None and value < 0,
        "qc_high_value_ge_500": value is not None and value >= 500,
        "qc_missing_timestamp": False,
        "qc_eligible": value is not None and 0 <= value < 500,
        "event_date": timestamp[:10],
    }
    identity = "|".join([
        row["source"], row["station_id"],
        row["parameter_name"], row["collected_at"]
    ])
    row["record_id"] = hashlib.sha256(identity.encode()).hexdigest()
    return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true",
                        help="List available locations")
    parser.add_argument("--location-id", help="Exact OpenAQ location ID")
    parser.add_argument("--name", help="Unique part of a location name")
    parser.add_argument("--from", dest="date_from",
                        help="Start datetime, e.g. 2025-01-01T00:00:00Z")
    parser.add_argument("--to", dest="date_to",
                        help="End datetime, e.g. 2026-10-27T00:00:00Z")
    args = parser.parse_args()

    ROOT.mkdir(parents=True, exist_ok=True)
    locations = load_locations()

    if args.list:
        for loc in locations:
            print(
                f"ID={loc.get('id')} | {loc.get('name')} | "
                f"{loc.get('locality')} | {loc.get('timezone')}"
            )
        return

    if not API_KEY:
        raise RuntimeError("OPENAQ_API_KEY is missing from .env")
    if bool(args.date_from) != bool(args.date_to):
        raise ValueError("Use both --from and --to, or neither.")

    location = select_location(locations, args.location_id, args.name)
    sensors = get_sensors(location)
    if not sensors:
        raise RuntimeError("No PM2.5 sensors found at this location.")

    loc_id = location["id"]
    stem = re.sub(r"[^A-Za-z0-9_-]+", "_", str(loc_id))
    output = ROOT / f"location_{stem}_pm25.jsonl"
    state_file = ROOT / f"location_{stem}_checkpoint.json"

    state = {"completed": [], "next_page": {}, "errors": {}}
    if state_file.exists():
        with state_file.open(encoding="utf-8") as f:
            state = json.load(f)

    session = requests.Session()
    session.headers.update({"X-API-Key": API_KEY})

    print("Location:", location.get("name"))
    print("Location ID:", loc_id)
    print("PM2.5 sensors:", len(sensors))
    print("Date range:", args.date_from or "all available", "to",
          args.date_to or "all available")
    print("Output:", output)
    print("Checkpoint:", state_file)

    for i, sensor in enumerate(sensors, 1):
        sid = str(sensor["sensor_id"])
        if sid in state["completed"]:
            print(f"[{i}/{len(sensors)}] sensor={sid}: already complete")
            continue

        page = int(state["next_page"].get(sid, 1))
        count = 0
        print(f"[{i}/{len(sensors)}] sensor={sid}, starting page={page}")

        try:
            while True:
                params = {"limit": PAGE_SIZE, "page": page}
                if args.date_from:
                    params["datetime_from"] = args.date_from
                    params["datetime_to"] = args.date_to

                payload = request_json(
                    session,
                    f"{BASE_URL}/sensors/{sid}/measurements",
                    params=params,
                )
                measurements = payload.get("results", [])
                if not measurements:
                    state["completed"].append(sid)
                    state["next_page"].pop(sid, None)
                    state["errors"].pop(sid, None)
                    break

                # Append this page before checkpointing. If interrupted
                # between these operations, final record-ID dedup removes
                # any repeated records on the next run.
                with output.open("a", encoding="utf-8") as f:
                    for measurement in measurements:
                        row = normalize(measurement, sensor)
                        if row:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
                            count += 1

                page += 1
                state["next_page"][sid] = page
                with state_file.open("w", encoding="utf-8") as f:
                    json.dump(state, f, indent=2)

                print(f"  Page saved; measurements this run: {count}")
                time.sleep(DELAY)

                if len(measurements) < PAGE_SIZE:
                    state["completed"].append(sid)
                    state["next_page"].pop(sid, None)
                    state["errors"].pop(sid, None)
                    break

            with state_file.open("w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            print(f"  Sensor finished; rows this run: {count}")

        except Exception as exc:
            state["errors"][sid] = str(exc)[:500]
            with state_file.open("w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            print(f"  ERROR for sensor {sid}: {str(exc)[:200]}")
            print("  Progress saved. Rerun this command to resume.")
            continue

    # Remove duplicate record IDs, including any page repeated after a crash.
    if output.exists():
        temp = output.with_suffix(".dedup.tmp")
        seen = set()
        total = 0
        with output.open(encoding="utf-8") as src, temp.open("w", encoding="utf-8") as dst:
            for line in src:
                try:
                    row = json.loads(line)
                    rid = row.get("record_id")
                    if rid and rid not in seen:
                        seen.add(rid)
                        dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                        total += 1
                except json.JSONDecodeError:
                    continue
        temp.replace(output)
    else:
        total = 0

    print("\n--- LOCATION INGESTION SUMMARY ---")
    print("Location:", location.get("name"))
    print("Sensors completed:", len(state["completed"]), "/", len(sensors))
    print("Unique records in output:", total)
    print("Sensors with errors:", len(state["errors"]))
    print("Measurements file:", output)
    print("Checkpoint file:", state_file)
    print("Hive modified: NO")


if __name__ == "__main__":
    main()
