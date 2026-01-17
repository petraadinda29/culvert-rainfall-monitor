import requests
from datetime import datetime, timedelta

METEOBOT_BASE_URL = "https://export.meteobot.com/v2/Generic/IndexFull"

def fetch_meteobot(stations_df):
    rows = []
    now = datetime.utcnow()

    start_date = (now - timedelta(minutes=1)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    for _, row in stations_df.iterrows():
        station_id = row["id"]
        station_name = row["station"]

        # Meteobot → ID numerik panjang
        if not station_id.isdigit():
            continue

        try:
            url = (
                f"{METEOBOT_BASE_URL}"
                f"?id={station_id}"
                f"&startTime={start_date}%2000:00"
                f"&endTime={end_date}%2023:59"
            )

            resp = requests.get(url, timeout=30)
            data = resp.json()

            if not data:
                continue

            last = data[-1]
            rainfall = last.get("rainfall")

            rows.append({
                "timestamp": now.isoformat(),
                "station": station_name,
                "rainfall_mm": rainfall,
                "source": "meteobot"
            })

        except Exception as e:
            print(f"[METEOBOT ERROR] {station_name}: {e}")

    return rows
