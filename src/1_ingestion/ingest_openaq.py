
import os
import json
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
BASE_URL = "https://api.openaq.org/v3"

OUTPUT_DIR = Path("datas/raw/openaq")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    if not API_KEY:
        raise RuntimeError("OPENAQ_API_KEY is missing from .env")

    session = requests.Session()
    session.headers.update({"X-API-Key": API_KEY})

    response = session.get(
        f"{BASE_URL}/locations",
        params={
            "iso": "IN",
            "parameters_id": 2,
            "limit": 1000,
            "page": 1,
        },
        timeout=(10, 60),
    )
    response.raise_for_status()

    payload = response.json()
    locations = payload.get("results", [])

    output = OUTPUT_DIR / "india_pm25_locations.json"

    with output.open("w", encoding="utf-8") as file:
        json.dump(locations, file, ensure_ascii=False, indent=2)

    sensor_count = sum(
        1
        for location in locations
        for sensor in location.get("sensors", [])
        if sensor.get("parameter", {}).get("name", "").lower()
        in {"pm25", "pm2.5"}
    )

    print("Locations fetched:", len(locations))
    print("PM2.5 sensors found:", sensor_count)
    print("Output:", output)
    print("Hive modified: NO")

    if locations:
        print("Sample location:", locations[0].get("name"))


if __name__ == "__main__":
    main()

