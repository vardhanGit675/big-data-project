
import os
import json
import hashlib
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
BASE_URL = "https://api.openaq.org/v3"

OUTPUT_DIR = Path("datas/raw/openaq")
LOCATIONS_FILE = OUTPUT_DIR / "india_pm25_locations.json"

# Start small. Increase after validating the output.
MAX_SENSORS = 10
PAGE_SIZE = 100
MAX_PAGES_PER_SENSOR = 5
REQUEST_DELAY_SECONDS = 0.25


def make_session():
    if not API_KEY:
        raise RuntimeError("OPENAQ_API_KEY is missing from .env")

    session = requests.Session()
    session.headers.update({"X-API-Key": API_KEY})
    return session


def request_json(session, url, params=None):
    for attempt in range(4):
        try:
            response = session.get(
                url, params=params, timeout=(10, 45)
            )

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 3:
                    time.sleep(2 ** attempt)
                    continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)

    raise RuntimeError("OpenAQ request failed after retries")


def load_sensors():
    if not LOCATIONS_FILE.exists():
        raise FileNotFoundError(
            f"Missing location metadata: {LOCATIONS_FILE}"
        )

    with LOCATIONS_FILE.open(encoding="utf-8") as file:
        locations = json.load(file)

    sensors = []

    for location in locations:
        coords = location.get("coordinates") or {}
        country = location.get("country") or {}

        for sensor in location.get("sensors", []):
            parameter = sensor.get("parameter") or {}
            if parameter.get("name", "").lower() not in {"pm25", "pm2.5"}:
                continue

            sensors.append({
                "sensor_id": sensor.get("id"),
                "location_id": location.get("id"),
                "station_name": location.get("name"),
                "city_name": location.get("locality"),
                "state_name": None,
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
                "country_code": country.get("code"),
                "parameter_name": "PM2.5",
                "unit": parameter.get("units") or "µg/m³",
            })

    # Each sensor is processed once.
    unique = {}
    for sensor in sensors:
        if sensor["sensor_id"] is not None:
            unique[sensor["sensor_id"]] = sensor

    return list(unique.values())


def normalize(measurement, sensor):
    value = measurement.get("value")
    period = measurement.get("period") or {}
    date_info = period.get("datetimeFrom") or {}
    timestamp = date_info.get("local") or date_info.get("utc")

    if not timestamp:
        return None

    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1]

    # Preserve the local clock time and remove the timezone offset
    # to match the existing Bronze timestamp convention.
    if len(timestamp) >= 25 and timestamp[19] in "+-":
        timestamp = timestamp[:19]

    try:
        numeric_value = float(value) if value is not None else None
    except (TypeError, ValueError):
        numeric_value = None

    station_id = f"openaq_{sensor['location_id']}_{sensor['sensor_id']}"

    result = {
        "station_id": station_id,
        "station_name": sensor["station_name"],
        "state_name": sensor["state_name"],
        "city_name": sensor["city_name"],
        "parameter_name": "PM2.5",
        "unit": sensor["unit"],
        "collected_at": timestamp,
        "value": numeric_value,
        "source": "openaq",
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
        result["source"],
        result["station_id"],
        result["parameter_name"],
        result["collected_at"],
    ])

    result["record_id"] = hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()

    return result


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session = make_session()
    sensors = load_sensors()[:MAX_SENSORS]

    unique_records = {}
    sensors_with_data = 0
    empty_sensors = 0
    errors = []

    print("Sensors selected:", len(sensors))

    for index, sensor in enumerate(sensors, start=1):
        sensor_id = sensor["sensor_id"]
        sensor_rows = 0

        print(
            f"[{index}/{len(sensors)}] "
            f"{sensor['station_name']} | sensor={sensor_id}"
        )

        try:
            for page in range(1, MAX_PAGES_PER_SENSOR + 1):
                payload = request_json(
                    session,
                    f"{BASE_URL}/sensors/{sensor_id}/measurements",
                    params={"limit": PAGE_SIZE, "page": page},
                )

                measurements = payload.get("results", [])
                if not measurements:
                    break

                for measurement in measurements:
                    item = normalize(measurement, sensor)
                    if item:
                        unique_records[item["record_id"]] = item
                        sensor_rows += 1

                if len(measurements) < PAGE_SIZE:
                    break

                time.sleep(REQUEST_DELAY_SECONDS)

            if sensor_rows:
                sensors_with_data += 1
            else:
                empty_sensors += 1

            print("  Measurements:", sensor_rows)

        except Exception as exc:
            errors.append({
                "sensor_id": sensor_id,
                "error": str(exc)[:300],
            })
            print("  ERROR:", str(exc)[:150])

        time.sleep(REQUEST_DELAY_SECONDS)

    output = OUTPUT_DIR / "openaq_pm25_test.jsonl"

    with output.open("w", encoding="utf-8") as file:
        for record in unique_records.values():
            file.write(json.dumps(record, ensure_ascii=False) + "\n")

    error_file = OUTPUT_DIR / "openaq_ingestion_errors.json"
    with error_file.open("w", encoding="utf-8") as file:
        json.dump(errors, file, indent=2, ensure_ascii=False)

    print("\n--- INGESTION SUMMARY ---")
    print("Sensors selected:", len(sensors))
    print("Sensors with data:", sensors_with_data)
    print("Empty sensors:", empty_sensors)
    print("Errors:", len(errors))
    print("Unique records:", len(unique_records))
    print("Measurements file:", output)
    print("Error log:", error_file)
    print("Hive modified: NO")


if __name__ == "__main__":
    main()

