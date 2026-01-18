import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

METEOBOT_BASE_URL = "https://export.meteobot.com/v2/Generic/IndexFull"

def fetch_meteobot(stations_df):
    rows = []
    now = datetime.utcnow()

    # ambil data 1 hari terakhir (aman & stabil)
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    for _, row in stations_df.iterrows():
        station_id = str(row["id"]).strip().lstrip("'")
        station_name = row["station"]

        # Ambil credential dari GitHub Secrets
        user = os.environ.get(f"METEOBOT_{station_id}_USER")
        password = os.environ.get(f"METEOBOT_{station_id}_PASS")

        if not user or not password:
            print(f"[METEOBOT SKIP] No credential for {station_name}")
            continue

        print(f"[METEOBOT] Fetching {station_name}")

        try:
            resp = requests.get(
                METEOBOT_BASE_URL,
                params={
                    "id": station_id,
                    "startTime": f"{start_date} 00:00",
                    "endTime": f"{end_date} 23:59"
                },
                auth=(user, password),
                timeout=30
            )
            resp.raise_for_status()

            # =========================
            # METEOBOT = CSV (;)
            # =========================
            df = pd.read_csv(StringIO(resp.text), sep=";")

            if df.empty:
                print(f"[METEOBOT EMPTY] {station_name}")
                continue

            # ambil data terakhir
            last = df.iloc[-1]

            rows.append({
                "timestamp": f"{last['date']}T{last['time']}",
                "station": station_name,
                "rainfall_mm": float(last.get("precipitation", 0)),
                "source": "meteobot"
            })

        except Exception as e:
            print(f"[METEOBOT ERROR] {station_name}: {e}")

    return rows
