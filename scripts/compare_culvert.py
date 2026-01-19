import os
import pandas as pd
from datetime import datetime

BASE_DIR = os.getcwd()

RAIN_DIR = os.path.join(BASE_DIR, "data", "rainfall")
CULVERT_FILE = os.path.join(BASE_DIR, "data", "seed", "loc_culvert.csv")
MAP_FILE = os.path.join(BASE_DIR, "data", "seed", "station_culvert.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "culvert_status")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def latest_rainfall_file():
    files = [f for f in os.listdir(RAIN_DIR) if f.startswith("rainfall_")]
    if not files:
        raise FileNotFoundError("No rainfall file found")
    return os.path.join(RAIN_DIR, sorted(files)[-1])

def main():
    # =========================
    # LOAD DATA
    # =========================
    rain_file = latest_rainfall_file()
    rain = pd.read_csv(rain_file)
    culvert = pd.read_csv(CULVERT_FILE)
    mapping = pd.read_csv(MAP_FILE)

    rain["timestamp"] = pd.to_datetime(
        rain["timestamp"],
        format="mixed",
        errors="coerce"
    )

    culvert = culvert[culvert["active"] == 1]
    mapping = mapping[mapping["active"] == 1]

    # =========================
    # AMBIL DATA HUJAN TERAKHIR PER STATION
    # =========================
    rain_latest = (
        rain.sort_values("timestamp")
        .groupby("station")
        .tail(1)
    )

    # =========================
    # JOIN: culvert → station → rainfall
    # =========================
    df = (
        mapping
        .merge(culvert, on="id", how="left")
        .merge(
            rain_latest,
            on="station",
            how="left"
        )
    )

    # =========================
    # LOGIC OVER / SAFE
    # =========================
    df["status"] = df.apply(
        lambda r: "OVER"
        if r["rainfall_mm"] > r["capacity"]
        else "SAFE",
        axis=1
    )

    # =========================
    # OUTPUT
    # =========================
    out = df[[
        "timestamp",
        "id",
        "station",
        "rainfall_mm",
        "capacity",
        "status"
    ]].rename(columns={"id": "culvert_id"})

    now = datetime.utcnow()
    out_file = os.path.join(
        OUTPUT_DIR,
        f"culvert_status_{now.strftime('%Y_%m')}.csv"
    )

    if os.path.exists(out_file):
        old = pd.read_csv(out_file)
        out = pd.concat([old, out], ignore_index=True)

    out.to_csv(out_file, index=False)
    print(f"[OK] Culvert status updated → {out_file}")

if __name__ == "__main__":
    main()
