
import os
import json
import hashlib
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://airquality.xkdr.org/v1/measurements"
API_KEY = os.getenv("XKDR_API_KEY")

OUTPUT_DIR = Path("datas/raw/xkdr")
STATIONS_FILE = OUTPUT_DIR / "stations.json"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "2026-03-26"
END_DATE = "2026-03-27"
PARAMETER = "PM2.5"
LIMIT = 1000


def load_stations():
    if not STATIONS_FILE.exists():
        raise FileNotFoundError(
            f"Station metadata not found: {STATIONS_FILE}"
        )

    with STATIONS_FILE.open(encoding="utf-8") as file:
        records = json.load(file)

    return {
        str(station["station_id"]): station
        for station in records
        if station.get("station_id")
    }


def fetch_measurements():
    if not API_KEY:
        raise RuntimeError("XKDR_API_KEY is missing from .env")

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)

    response = session.get(
        API_URL,
        headers={"Authorization": f"Bearer {API_KEY}"},
        params={
            "parameter": PARAMETER,
            "start": START_DATE,
            "end": END_DATE,
            "agg": "raw",
            "limit": LIMIT,
            "format": "json",
        },
        timeout=(10, 60),
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("meta", {}).get("truncated"):
        raise RuntimeError(
            "API response was truncated. Narrow the date range."
        )

    return payload.get("data", []), payload.get("meta", {})


def normalize(record, stations):
    timestamp = record.get("collected_at")
    station_id = record.get("station_id")
    parameter = record.get("parameter_name")

    if not timestamp or not station_id or not parameter:
        return None

    station_id = str(station_id)
    station = stations.get(station_id, {})
    value = record.get("value")

    try:
        numeric_value = float(value) if value is not None else None
    except (TypeError, ValueError):
        numeric_value = None

    # Preserve the original monitoring-network source when available.
    source = station.get("source") or "xkdr"

    normalized = {
        "station_id": station_id,
        "station_name": station.get("station_name"),
        "state_name": station.get("state_name"),
        "city_name": station.get("city_name"),
        "parameter_name": str(parameter),
        "unit": record.get("unit"),
        "collected_at": timestamp,
        "value": numeric_value,
        "source": source,
        "qc_missing_value": numeric_value is None,
        "qc_negative_value": (
            numeric_value is not None and numeric_value < 0
        ),
        "qc_high_value_ge_500": (
            numeric_value is not None and numeric_value >= 500
        ),
        "qc_missing_timestamp": False,
        "qc_eligible": (
            numeric_value is not None
            and 0 <= numeric_value < 500
        ),
        "event_date": timestamp[:10],
    }

    identity = "|".join([
        normalized["source"],
        normalized["station_id"],
        normalized["parameter_name"],
        normalized["collected_at"],
    ])

    normalized["record_id"] = hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()

    return normalized


def main():
    stations = load_stations()
    records, metadata = fetch_measurements()

    unique = {}
    unmatched_stations = set()

    for record in records:
        item = normalize(record, stations)

        if item is None:
            continue

        if item["station_name"] is None:
            unmatched_stations.add(item["station_id"])

        unique[item["record_id"]] = item

    output = OUTPUT_DIR / (
        f"xkdr_pm25_{START_DATE}_{END_DATE}.jsonl"
    )

    with output.open("w", encoding="utf-8") as file:
        for item in unique.values():
            file.write(json.dumps(item, ensure_ascii=False) + "\n")

    print("Stations loaded:", len(stations))
    print("API rows:", metadata.get("rows", len(records)))
    print("Records received:", len(records))
    print("Unique rows saved:", len(unique))
    print("Unmatched station IDs:", len(unmatched_stations))

    if unmatched_stations:
        print("Unmatched examples:", sorted(unmatched_stations)[:10])

    print("Output:", output)
    print("Hive modified: NO")


if __name__ == "__main__":
    main()

