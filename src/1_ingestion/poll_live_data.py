import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

# OpenAQ v3 API endpoint for measurements
API_URL = "https://api.openaq.org/v3/locations"

def fetch_live_sensor_data():
    if not OPENAQ_API_KEY:
        print("ERROR: OPENAQ_API_KEY is missing from your .env file!")
        return

    headers = {
        "X-API-Key": OPENAQ_API_KEY
    }

    # Example: Fetching for a specific OpenAQ location ID mapping to your station
    # (You can loop through your database openaq_id values here)
    params = {
        "limit": 100
    }

    print("Fetching live data from OpenAQ API...")
    response = requests.get(API_URL, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])
        
        batch_records = []
        for loc in results:
            # Map OpenAQ fields to your pipeline schema
            record = {
                "station_id": f"site_{loc.get('id')}",
                "station_name": loc.get('name'),
                "state_name": "India",
                "city_name": loc.get('locality', 'Unknown'),
                "parameter_name": "pm25",
                "unit": "µg/m³",
                "timestamp_str": loc.get('datetime', {}).get('utc'),
                "pm25": loc.get('sensors', [{}])[0].get('latest', {}).get('value', 45.0), # fallback sample
                "temperature": 28.5,
                "humidity": 75.0,
                "wind_speed": 3.5,
                "pressure": 1012.0
            }
            batch_records.append(record)

        # Save to raw incoming folder where Spark Step 1 reads from
        raw_output_path = "data/raw_stream_incoming/batch_live.json"
        os.makedirs("data/raw_stream_incoming", exist_ok=True)
        
        with open(raw_output_path, "w") as f:
            json.dump(batch_records, f, indent=4)
            
        print(f"Successfully fetched and saved {len(batch_records)} records to {raw_output_path}")
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

if __name__ == "__main__":
    fetch_live_sensor_data()