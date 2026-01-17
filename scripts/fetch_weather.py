import requests
from datetime import datetime

WEATHER_BASE_URL = "https://api.weather.com/v2/pws/observations/current"

def fetch_weather(stations_df, api_key):
    rows = []
    now = datetime.utcnow().isoformat()

    for _, row in stations_df.iterrows():
        station_id = row["id"]
        station_name = row["station"]

        # Weather.com station → ID = huruf/angka pendek
        if station_id.isdigit():
            continue

        try:
            url = (
                f"{WEATHER_BASE_URL}"
                f"?stationId={station_id}"
                f"&format=json&units=m&apiKey={api_key}"
            )

            resp = requests.get(url, timeout=20)
            data = resp.json()

            obs = data["observations"][0]
            rainfall = obs["metric"].get("precipTotal")

            rows.append({
                "timestamp": now,
                "station": station_name,
                "rainfall_mm": rainfall,
                "source": "weather"
            })

        except Exception as e:
            print(f"[WEATHER ERROR] {station_name}: {e}")

    return rows
