#!/usr/bin/env python3
"""Data generator for Project 27: Severe Weather Alert Pipeline."""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta

from google.cloud import pubsub_v1
from faker import Faker

fake = Faker()

REGIONS = ["Gulf Coast", "Great Plains", "Pacific NW", "Northeast", "Southwest", "Midwest"]
STATES = {"Gulf Coast": "TX", "Great Plains": "KS", "Pacific NW": "WA", "Northeast": "NY", "Southwest": "AZ", "Midwest": "IL"}
COUNTIES = {"Gulf Coast": ["Harris", "Galveston"], "Great Plains": ["Sedgwick", "Riley"], "Pacific NW": ["King", "Pierce"],
            "Northeast": ["Suffolk", "Nassau"], "Southwest": ["Maricopa", "Pima"], "Midwest": ["Cook", "DuPage"]}
MEASUREMENT_TYPES = ["temperature", "wind_speed", "humidity", "precipitation", "pressure"]
UNITS = {"temperature": "C", "wind_speed": "km/h", "humidity": "%", "precipitation": "mm/hr", "pressure": "hPa"}


def generate_record():
    """Generate a single weather station measurement."""
    now = datetime.utcnow()
    if random.random() < 0.10:
        now -= timedelta(minutes=random.randint(1, 5))

    region = random.choice(REGIONS)
    mtype = random.choice(MEASUREMENT_TYPES)
    station = f"WS-{region[:2].upper()}-{random.randint(1,10):03d}"

    if region == "Southwest" and mtype == "temperature":
        value = round(random.uniform(36, 48), 1)
    elif region == "Southwest" and mtype == "wind_speed":
        value = round(random.uniform(40, 90), 1)
    elif region == "Gulf Coast" and mtype == "precipitation":
        value = round(random.uniform(20, 80), 1)
    elif mtype == "temperature":
        value = round(random.uniform(15, 38), 1)
    elif mtype == "wind_speed":
        value = round(random.uniform(5, 60), 1)
    elif mtype == "humidity":
        value = round(random.uniform(20, 98), 1)
    elif mtype == "precipitation":
        value = round(random.uniform(0, 30), 1)
    else:
        value = round(random.uniform(985, 1035), 1)

    alert = "none"
    if mtype == "temperature" and value > 40:
        alert = "warning"
    elif mtype == "wind_speed" and value > 60:
        alert = "watch"
    elif mtype == "precipitation" and value > 50:
        alert = "warning"

    return {
        "station_id": station,
        "measurement_type": mtype,
        "value": value,
        "unit": UNITS[mtype],
        "region": region,
        "county": random.choice(COUNTIES[region]),
        "state": STATES[region],
        "alert_level": alert,
        "event_time": now.isoformat()
    }


def main():
    parser = argparse.ArgumentParser(description="Severe Weather Alert data generator")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (default: 60)")
    parser.add_argument("--rate", type=int, default=5, help="Messages per second (default: 5)")
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, "spark-27-weather-alerts")

    print(f"Publishing to {topic_path} at {args.rate} msgs/sec for {args.duration}s")
    print("--- Preview (first 3 records) ---")

    sent = 0
    duplicates_pool = []
    start = time.time()

    while time.time() - start < args.duration:
        record = generate_record()

        if random.random() < 0.05 and duplicates_pool:
            record = random.choice(duplicates_pool).copy()
        else:
            duplicates_pool.append(record)
            if len(duplicates_pool) > 50:
                duplicates_pool.pop(0)

        data = json.dumps(record).encode("utf-8")
        publisher.publish(topic_path, data=data)
        sent += 1

        if sent <= 3:
            print(json.dumps(record, indent=2))

        time.sleep(1.0 / args.rate)

    print(f"\nDone. Published {sent} messages.")


if __name__ == "__main__":
    main()
