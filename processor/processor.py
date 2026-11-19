import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer, KafkaProducer
import psycopg

WINDOW = timedelta(minutes=5)
ALLOWED_LATENESS = timedelta(minutes=2)


def parse_event(raw: bytes) -> dict:
    event = json.loads(raw)
    required = {"event_id", "event_type", "event_time", "user_id"}
    if not required.issubset(event):
        raise ValueError(f"missing fields: {sorted(required - event.keys())}")
    event["event_time"] = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    return event


def window_start(value: datetime) -> datetime:
    return value.replace(minute=value.minute - value.minute % 5, second=0, microsecond=0)


def run() -> None:
    consumer = KafkaConsumer("clicks", bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
                             group_id="click-aggregator", enable_auto_commit=False)
    dlq = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    counts = Counter()
    max_event_time = datetime.min.replace(tzinfo=timezone.utc)
    with psycopg.connect(os.getenv("DATABASE_URL", "postgresql://stream:stream@localhost:5432/events")) as db:
        db.execute("""create table if not exists event_windows (
          window_start timestamptz, event_type text, event_count integer,
          primary key (window_start, event_type))""")
        for message in consumer:
            try:
                event = parse_event(message.value)
                max_event_time = max(max_event_time, event["event_time"])
                if event["event_time"] < max_event_time - ALLOWED_LATENESS:
                    continue
                counts[(window_start(event["event_time"]), event["event_type"])] += 1
                watermark = max_event_time - ALLOWED_LATENESS
                for key in list(counts):
                    if key[0] + WINDOW <= watermark:
                        db.execute("""insert into event_windows values (%s,%s,%s)
                          on conflict (window_start,event_type) do update
                          set event_count=excluded.event_count""", (*key, counts.pop(key)))
                        db.commit()
                consumer.commit()
            except (ValueError, json.JSONDecodeError) as error:
                dlq.send("clicks.dlq", value=json.dumps({"error": str(error), "payload": message.value.decode()}).encode())
                consumer.commit()


if __name__ == "__main__":
    run()
