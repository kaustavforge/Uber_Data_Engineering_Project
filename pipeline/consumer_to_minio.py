"""Kafka -> NDJSON files in MinIO.

Reads ride events, buffers them, and writes one newline-delimited JSON file per batch to
s3://<bucket>/rides_stream/. Offsets are committed only after the upload succeeds.
"""
import io
import json
import os
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ubertopic")
BUCKET = os.getenv("MINIO_BUCKET", "uber-raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "20"))
PREFIX = "rides_stream"

minio = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False,
)
if not minio.bucket_exists(BUCKET):
    minio.make_bucket(BUCKET)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "uber-minio-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)
consumer.subscribe([TOPIC])


def flush(lines):
    if not lines:
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    name = f"{PREFIX}/rides_{ts}_{uuid.uuid4().hex[:8]}.json"
    body = ("\n".join(lines) + "\n").encode("utf-8")
    minio.put_object(BUCKET, name, io.BytesIO(body), len(body), content_type="application/json")
    consumer.commit(asynchronous=False)
    print(f"wrote {len(lines)} events -> {BUCKET}/{name}")


def main():
    buffer, last_flush = [], time.time()
    print(f"Consuming '{TOPIC}' -> MinIO bucket '{BUCKET}' (batch={BATCH_SIZE}, every {FLUSH_SECONDS}s)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print(f"consumer error: {msg.error()}")
                else:
                    # re-dump to guarantee exactly one line per event
                    buffer.append(json.dumps(json.loads(msg.value().decode("utf-8"))))

            due = time.time() - last_flush >= FLUSH_SECONDS
            if buffer and (len(buffer) >= BATCH_SIZE or due):
                flush(buffer)
                buffer, last_flush = [], time.time()
            elif due:
                last_flush = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        flush(buffer)
        consumer.close()


if __name__ == "__main__":
    main()
