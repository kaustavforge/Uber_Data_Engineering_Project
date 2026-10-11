"""Make a rides_stream-style file of fake rides WITHOUT Docker, Kafka or MinIO.
Handy for the SCD2 test: force every ride into one city so the changed city row is guaranteed to flow through.

  uv run python tools/generate_rides_file.py --n 10 --city 7             # writes generated/rides_manual_*.json
  uv run python tools/generate_rides_file.py --n 10 --city 7 --upload    # also uploads it to the Databricks Volume

--upload needs DATABRICKS_HOST and DATABRICKS_TOKEN in .env (same as sync_to_databricks.py).
Without --upload, upload the file by hand: Catalog -> uber -> raw -> landing -> rides_stream -> Upload to this volume.
"""
import argparse
import io
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))

from data import generate_uber_ride_confirmation  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--n", type=int, default=10, help="how many rides")
ap.add_argument("--city", type=int, default=None, help="force pickup_city_id (1-10); default = random like the app")
ap.add_argument("--upload", action="store_true", help="upload the file to the Databricks Volume rides_stream/")
args = ap.parse_args()

lines = []
for _ in range(args.n):
    ride = generate_uber_ride_confirmation()
    if args.city is not None:
        ride["pickup_city_id"] = args.city
    lines.append(json.dumps(ride))  # one ride per line (NDJSON), same as the consumer writes

ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
name = f"rides_manual_{ts}_{uuid.uuid4().hex[:8]}.json"
body = ("\n".join(lines) + "\n").encode("utf-8")

out_dir = ROOT / "generated"
out_dir.mkdir(exist_ok=True)
(out_dir / name).write_bytes(body)
print(f"wrote {args.n} rides -> {out_dir / name}")

if args.upload:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(host=os.getenv("DATABRICKS_HOST"), token=os.getenv("DATABRICKS_TOKEN"))
    volume = "/Volumes/{c}/{s}/{v}".format(
        c=os.getenv("UC_CATALOG", "uber"), s=os.getenv("UC_SCHEMA", "raw"), v=os.getenv("UC_VOLUME", "landing")
    )
    w.files.upload(f"{volume}/rides_stream/{name}", io.BytesIO(body), overwrite=True)
    print(f"uploaded -> {volume}/rides_stream/{name}")
