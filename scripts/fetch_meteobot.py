import os
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
        
        print(
            f"[DEBUG] station_id='{station_id}' "
            f"env_key='METEOBOT_{station_id}_USER'"
        )

        user = os.environ.get(f"METEOBOT_{station_id}_USER")
        password = os.environ.get(f"METEOBOT_{station_id}_PASS")

        if not user or not password:
            print(f"[METEOBOT SKIP] No credential for {station_name}")
            continue

        print(f"[METEOBOT] Fetching {station_name}")

        try:
            url = (
                f"{METEOBOT_BASE_URL}"
                f"?id={station_id}"
                f"&startTime={start_date}%2000:00"
                f"&endTime={end_date}%2023:59"
            )

            resp = requests.get(
                url,
                auth=(user, password),
                timeout=30
            )
            resp.raise_for_status()

            json_resp = resp.json()
            records = json_resp.get("data", [])

            if not records:
                continue

            last = records[-1]

            rows.append({
                "timestamp": now.isoformat(),
                "station": station_name,
                "rainfall_mm": last.get("rainfall", 0),
                "source": "meteobot"
            })

        except Exception as e:
            print(f"[METEOBOT ERROR] {station_name}: {e}")

    return rows
