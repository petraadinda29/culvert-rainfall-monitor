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

LOCAL_TZ = timezone(timedelta(hours=8))
MIN_UPDATE_INTERVAL = timedelta(minutes=3)

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


def should_update():
    if not os.path.exists(OUTPUT_FILE):
        return True

    last_modified = datetime.fromtimestamp(
        os.path.getmtime(OUTPUT_FILE),
        tz=LOCAL_TZ
    )

    return datetime.now(LOCAL_TZ) - last_modified >= MIN_UPDATE_INTERVAL


# ==============================
# MAIN PROCESS
# ==============================

def main():

    if not should_update():
        print("[SKIP] Dashboard update < 3 minutes")
        return

    print("[INFO] Generating dashboard data...")

    # Load data
    status_file = get_latest_status_file()
    status_df = pd.read_csv(status_file)

    culvert_df = pd.read_csv(CULVERT_FILE)
    culvert_df = culvert_df[culvert_df["active"] == 1]
    culvert_df = culvert_df[["id", "lat", "lon", "capacity"]]

    # Clean ID
    status_df["culvert_id"] = status_df["culvert_id"].astype(str).str.strip()
