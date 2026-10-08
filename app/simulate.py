"""OPTIONAL helper: fire N fake rides into Kafka without clicking the web page.
The main way to book rides is the FastAPI app (api.py), same as Ansh's project.
Usage:  python simulate.py --n 200 --delay 0.2
"""
import argparse
import time

from data import generate_uber_ride_confirmation
from producer import send_to_kafka

parser = argparse.ArgumentParser()
parser.add_argument("--n", type=int, default=100, help="how many rides to send")
parser.add_argument("--delay", type=float, default=0.2, help="seconds to wait between rides (0 = as fast as possible)")
args = parser.parse_args()

for i in range(1, args.n + 1):
    send_to_kafka(generate_uber_ride_confirmation())
    if i % 20 == 0:
        print(f"sent {i}/{args.n}")
    time.sleep(args.delay)
print("done")
