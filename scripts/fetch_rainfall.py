import os
import sys
import pandas as pd
from datetime import datetime

# =========================
# PATH FIX (PENTING)
# =========================
BASE_DIR = os.getcwd()
sys.path.append(os.path.join(BASE_DIR, "scripts"))

from fetch_weather import fetch_weather
from fetch_meteobot import fetch_meteobot

# =========================
# CONFIG
# =========================
STATIONS_FILE = os.path.join(BASE_DIR, "data", "seed", "loc_stations.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "rainfall")

WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# MAIN
# =========================
def main():
    stations = pd.read_csv(STATIONS_FILE, dtype={"id": str})
    stations = stations[stations["active"] == 1]

    all_rows = []

    # Weather.com
    all_rows.extend(fetch_weather(stations, WEATHER_API_KEY))

    # Meteobot
    all_rows.extend(fetch_meteobot(stations))

    if not all_rows:
        print("No rainfall data fetched.")
        return

    df_new = pd.DataFrame(all_rows)

    now = datetime.utcnow()
    filename = f"rainfall_{now.strftime('%Y_%m')}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    if os.path.exists(filepath):
        df_old = pd.read_csv(filepath)
        df_new = pd.concat([df_old, df_new], ignore_index=True)

    df_new.to_csv(filepath, index=False)
    print(f"Rainfall data updated → {filepath}")

if __name__ == "__main__":
    main()
