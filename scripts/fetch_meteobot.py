import requests
from datetime import datetime, timedelta

METEOBOT_BASE_URL = "https://export.meteobot.com/v2/Generic/IndexFull"

def fetch_meteobot(stations_df):
    rows = []
    now = datetime.utcnow()

    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    for _, row in stations_df.iterrows():
        station_id = str(row["id"]).strip().lstrip("'")
        station_name = row["station"]

        print(f"[METEOBOT DEBUG] {station_name} → {station_id}")

        # Meteobot pakai ID numerik
        if not station_id or station_id.lower() == "nan":
            continue

        try:
            url = (
                f"{METEOBOT_BASE_URL}"
                f"?id={station_id}"
                f"&startTime={start_date}%2000:00"
                f"&endTime={end_date}%2023:59"
            )

            print(f"[METEOBOT] Fetching {station_name} ({station_id})")

            resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            json_resp = resp.json()
            records = json_resp.get("data", [])

            if not records:
                print(f"[METEOBOT] No data for {station_name}")
                continue

            last = records[-1]
            rainfall = last.get("rainfall", 0)

            rows.append({
                "timestamp": now.isoformat(),
                "station": station_name,
                "rainfall_mm": rainfall,
                "source": "meteobot"
            })

        except Exception as e:
            print(f"[METEOBOT ERROR] {station_name}: {e}")

    return rows
