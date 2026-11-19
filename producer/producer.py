import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer


def produce(bootstrap: str, count: int, topic: str = "clicks") -> None:
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             value_serializer=lambda value: json.dumps(value).encode())
    for _ in range(count):
        event = {"event_id": str(uuid.uuid4()), "event_type": random.choice(["view", "click"]),
                 "event_time": datetime.now(timezone.utc).isoformat(), "user_id": str(uuid.uuid4())}
        producer.send(topic, key=event["event_id"].encode(), value=event)
        time.sleep(0.05)
    producer.flush()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--count", type=int, default=100)
    a = p.parse_args()
    produce(a.bootstrap, a.count)
