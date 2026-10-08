"""MinIO -> Databricks Unity Catalog Volume.

  python sync_to_databricks.py --static            # one time: ../data/*.json -> <volume>/static/
  python sync_to_databricks.py                     # sync new rides_stream/ files once, then stop
  python sync_to_databricks.py --watch             # keep running, check MinIO every 30s
  python sync_to_databricks.py --watch --interval 10

Already-synced files are remembered in .synced.json so reruns only upload new files.
If your workspace doesn't allow API tokens, skip this script and upload the files by hand
(Catalog -> your volume -> Upload to this volume).
"""
import argparse
import io
import json
import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

BUCKET = os.getenv("MINIO_BUCKET", "uber-raw")
VOLUME_ROOT = "/Volumes/{c}/{s}/{v}".format(
    c=os.getenv("UC_CATALOG", "uber"),
    s=os.getenv("UC_SCHEMA", "raw"),
    v=os.getenv("UC_VOLUME", "landing"),
)
STATE = Path(__file__).parent / ".synced.json"
DATA_DIR = Path(__file__).parent.parent / "data"


def get_databricks() -> WorkspaceClient:
    return WorkspaceClient(host=os.getenv("DATABRICKS_HOST"), token=os.getenv("DATABRICKS_TOKEN"))


def get_minio() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def sync_static(w: WorkspaceClient):
    for f in sorted(DATA_DIR.glob("*.json")):
        with open(f, "rb") as fh:
            w.files.upload(f"{VOLUME_ROOT}/static/{f.name}", fh, overwrite=True)
        print(f"uploaded static/{f.name}")


def sync_stream_once(w: WorkspaceClient, minio: Minio) -> int:
    """Upload MinIO files we haven't sent yet. Returns how many were uploaded."""
    done = set(json.loads(STATE.read_text())) if STATE.exists() else set()
    new = 0
    for obj in minio.list_objects(BUCKET, prefix="rides_stream/", recursive=True):
        if obj.object_name in done:
            continue
        resp = minio.get_object(BUCKET, obj.object_name)
        try:
            data = resp.read()
        finally:
            resp.close()
            resp.release_conn()
        w.files.upload(f"{VOLUME_ROOT}/{obj.object_name}", io.BytesIO(data), overwrite=True)
        done.add(obj.object_name)
        STATE.write_text(json.dumps(sorted(done)))  # save after every file, so a crash loses nothing
        new += 1
        print(f"uploaded {obj.object_name}")
    return new


def watch(w: WorkspaceClient, minio: Minio, interval: int):
    print(f"Watching MinIO bucket '{BUCKET}' every {interval}s. Ctrl+C to stop.")
    try:
        while True:
            try:
                new = sync_stream_once(w, minio)
                if new:
                    print(f"{new} new file(s) synced")
            except Exception as e:  # keep running if MinIO/Databricks hiccups
                print(f"sync failed, will retry: {e}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("stopped")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--static", action="store_true", help="upload ../data/*.json once")
    ap.add_argument("--watch", action="store_true", help="keep running and sync new files repeatedly")
    ap.add_argument("--interval", type=int, default=30, help="seconds between checks in --watch mode")
    args = ap.parse_args()

    dbx = get_databricks()
    if args.static:
        sync_static(dbx)
    elif args.watch:
        watch(dbx, get_minio(), args.interval)
    else:
        print(f"{sync_stream_once(dbx, get_minio())} new file(s) synced")
