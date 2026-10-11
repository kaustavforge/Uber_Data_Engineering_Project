"""Add N new fake rides to data/bulk_rides.json (a JSON list) so you can test adding history later.

  uv run python tools/add_bulk_rides.py --n 100

Then push data/bulk_rides.json to GitHub and run the Job: notebook 01 downloads the file and the MERGE
adds only the new ride_ids to uber.bronze.bulk_rides. Existing rides are left untouched.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))

from data import generate_uber_ride_confirmation  # noqa: E402

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--n", type=int, default=100, help="how many new rides to add")
args = ap.parse_args()

path = ROOT / "data" / "bulk_rides.json"
rides = json.loads(path.read_text(encoding="utf-8"))
existing = {r["ride_id"] for r in rides}

added = 0
while added < args.n:
    ride = generate_uber_ride_confirmation()
    if ride["ride_id"] in existing:
        continue
    rides.append(ride)
    existing.add(ride["ride_id"])
    added += 1

path.write_text(json.dumps(rides, indent=2), encoding="utf-8")
print(f"added {added} rides -> {len(rides)} total in {path}")
