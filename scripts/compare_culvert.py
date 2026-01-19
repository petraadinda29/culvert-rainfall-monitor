import os
import pandas as pd
from datetime import datetime

# =========================
# PATH
# =========================
BASE_DIR = os.getcwd()

RAIN_DIR = os.path.join(BASE_DIR, "data", "rainfall")
CULVERT_FILE = os.path.join(BASE_DIR, "data", "seed", "loc_culvert.csv")
MAP_FILE = os.path.join(BASE_DIR, "data", "seed", "station_culvert.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "culvert_status")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# UTILS
# =========================
def latest_rainfall_file():
    files = [f for f in os.listdir(RAIN_DIR) if f.startswith("rainfall_")]
    if not files:
        raise FileNotFoundError("No rainfall file found")
    return os.path.join(RAIN_DIR, sorted(files)[-1])

# =========================
# MAIN
# =========================
def main():
    # =========================
    # LOAD DATA
    # =========================
    rain = pd.read_csv(latest_rainfall_file())
    culvert = pd.read_csv(CULVERT_FILE)
    mapping = pd.read_csv(MAP_FILE)

    # =========================
    # CLEAN & NORMALIZE
    # =========================
    rain["timestamp"] = pd.to_datetime(
        rain["timestamp"],
        format="mixed",
        errors="coerce"
    )

    rain["station"] = rain["station"].str.strip().str.upper()
    mapping["station"] = mapping["station"].str.strip().str.upper()

    culvert = culvert[culvert["active"] == 1]
    mapping = mapping[mapping["active"] == 1]

    # =========================
    # LATEST RAINFALL / STATION
    # =========================
    rain_latest = (
        rain.sort_values("timestamp")
        .groupby("station", as_index=False)
        .tail(1)
    )

    # =========================
    # JOIN
    # =========================
    df = (
        mapping
        .merge(culvert, on="id", how="left")
        .merge(rain_latest, on="station", how="left")
    )

    # =========================
    # FILL NA
    # =========================
    df["rainfall_mm"] = df["rainfall_mm"].fillna(0)
    df["capacity"] = df["capacity"].fillna(0)

    # =========================
    # OVER / SAFE LOGIC
    # =========================
    df["status"] = df.apply(
        lambda r: "OVER" if r["rainfall_mm"] > r["capacity"] else "SAFE",
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
    print("[INFO] Total rows:", len(out))

if __name__ == "__main__":
    main()
