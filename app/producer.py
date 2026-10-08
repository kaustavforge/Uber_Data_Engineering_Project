"""Replaces Ansh's connection.py: sends ride events to Kafka instead of Azure Event Hubs."""
import json
import os

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ubertopic")

_producer = Producer({"bootstrap.servers": BOOTSTRAP})


def _on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def send_to_kafka(ride_data: dict) -> bool:
    try:
        _producer.produce(
            TOPIC,
            key=ride_data["ride_id"],
            value=json.dumps(ride_data),  # single line -> safe for NDJSON files later
            on_delivery=_on_delivery,
        )
        _producer.flush(10)
        return True
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
        return False


if __name__ == "__main__":
    from data import generate_uber_ride_confirmation

    ride = generate_uber_ride_confirmation()
    print(json.dumps(ride, indent=2))
    print("sent:", send_to_kafka(ride))
