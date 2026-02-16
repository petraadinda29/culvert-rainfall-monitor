import pandas as pd
import os

SRC_DIR = "data/culvert_status"
DST_FILE = "docs/data/culvert_status_latest.csv"

files = sorted(
    f for f in os.listdir(SRC_DIR)
    if f.startswith("culvert_status_")
)

if not files:
    raise FileNotFoundError("No culvert status file")

latest = os.path.join(SRC_DIR, files[-1])

df = pd.read_csv(latest)
df.to_csv(DST_FILE, index=False)

print("[OK] Dashboard data exported")
