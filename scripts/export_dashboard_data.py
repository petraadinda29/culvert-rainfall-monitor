import os
import json
import pandas as pd
from datetime import datetime, timezone, timedelta

# ==============================
# CONFIG
# ==============================

BASE_DIR = os.getcwd()

STATUS_DIR = os.path.join(BASE_DIR, "data", "culvert_status")
CULVERT_FILE = os.path.join(BASE_DIR, "data", "seed", "loc_culvert.csv")

DASHBOARD_DIR = os.path.join(BASE_DIR, "dashboard_data")
OUTPUT_FILE = os.path.join(DASHBOARD_DIR, "culvert_latest.json")

HISTORY_FILE = os.path.join(DASHBOARD_DIR, "culvert_history.json")

LOCAL_TZ = timezone(timedelta(hours=8))

os.makedirs(DASHBOARD_DIR, exist_ok=True)


# ==============================
# HELPER FUNCTIONS
# ==============================

def get_latest_status_file():
    files = [
        f for f in os.listdir(STATUS_DIR)
        if f.startswith("culvert_status_") and f.endswith(".csv")
    ]

    if not files:
        raise FileNotFoundError("No culvert status file found")

    return os.path.join(STATUS_DIR, sorted(files)[-1])

# ==============================
# MAIN PROCESS
# ==============================

def main():

    print("[INFO] Generating dashboard data...")

    # Load data
    status_file = get_latest_status_file()
    status_df = pd.read_csv(status_file)

    culvert_df = pd.read_csv(CULVERT_FILE)
    culvert_df = culvert_df[culvert_df["active"] == 1]
    culvert_df = culvert_df[["id", "lat", "lon", "capacity"]]

    # Clean ID
    status_df["culvert_id"] = status_df["culvert_id"].astype(str).str.strip()
    culvert_df["id"] = culvert_df["id"].astype(str).str.strip()

    # Parse timestamp
    status_df["timestamp"] = pd.to_datetime(
        status_df["timestamp"],
        errors="coerce"
    )

    status_df = status_df.dropna(subset=["timestamp"])

    print("MAX TIMESTAMP:", status_df["timestamp"].max())
    
    # Latest status per culvert
    latest_status = (
        status_df.sort_values("timestamp")
        .groupby("culvert_id")
        .tail(1)
    )
    
    # Remove duplicated capacity from status
    if "capacity" in latest_status.columns:
        latest_status = latest_status.drop(columns=["capacity"])

    # Merge location
    merged = latest_status.merge(
        culvert_df,
        left_on="culvert_id",
        right_on="id",
        how="left"
    )

    merged = merged.where(pd.notna(merged), None)
    
    features = []
    over_count = 0
    safe_count = 0

    for _, row in merged.iterrows():

        status_value = row.get("status", "UNKNOWN")

        if status_value == "OVER":
            over_count += 1
        elif status_value == "SAFE":
            safe_count += 1

        # Safe timezone handling
        ts_value = None
        if pd.notna(row["timestamp"]):
            ts = row["timestamp"]

            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")

            ts = ts.tz_convert(LOCAL_TZ)
            ts_value = ts.isoformat()

        station_value = row["station"]
        if pd.isna(station_value):
            station_value = None
        
        features.append({
            "culvert_id": row["culvert_id"],
            "station": station_value,
            "lat": float(row["lat"]) if pd.notna(row["lat"]) else None,
            "lon": float(row["lon"]) if pd.notna(row["lon"]) else None,
            "rainfall_mm": float(row["rainfall_mm"]) if pd.notna(row["rainfall_mm"]) else None,
            "capacity": float(row["capacity"]) if pd.notna(row["capacity"]) else None,
            "status": status_value,
            "timestamp": ts_value
        })

    output = {
        "timestamp": datetime.now(LOCAL_TZ).isoformat(),
        "timezone": "UTC+8",
        "total_culvert": len(features),
        "over": over_count,
        "safe": safe_count,
        "culverts": features
    }

    # Save JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, allow_nan=False)

    # ==============================
    # SAVE HISTORY 24 HOURS
    # ==============================

    # Load existing history
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            history = json.load(f)
    else:
        history = []

    # Append new snapshot
    history.append({
        "timestamp": datetime.now(LOCAL_TZ).isoformat(),
        "culverts": features
    })

    # Keep only last 24 hours
    cutoff = datetime.now(LOCAL_TZ) - timedelta(hours=24)

    new_history = []
    for h in history:
        try:
            ts = datetime.fromisoformat(h["timestamp"])
            if ts > cutoff:
                new_history.append(h)
        except:
            pass

    # Save back
    with open(HISTORY_FILE, "w") as f:
        json.dump(new_history, f, indent=2, allow_nan=False)

    print(f"[OK] Dashboard JSON exported → {OUTPUT_FILE}")

import sys

if __name__ == "__main__":

    force = False

    if len(sys.argv) > 1 and sys.argv[1] == "force":
        force = True
        print("[FORCE] Manual run")

    main()

