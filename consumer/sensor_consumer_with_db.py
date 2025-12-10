from kafka import KafkaConsumer
import json
import time
from collections import defaultdict, deque
from datetime import datetime

import sys
import pathlib

project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root)) 

from sqlalchemy import text
from config.database import get_engine

def create_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        "sensor_readings",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="sensor-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    return consumer

def write_events_per_second(engine, ts: datetime, count: int):
    sql = text(
        """
        INSERT INTO events_per_second (ts, events_count)
        VALUES (:ts, :count)
        ON CONFLICT (ts) DO UPDATE
          SET events_count = EXCLUDED.events_count;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"ts": ts, "count": count})

def write_sensor_rolling_avg(engine, sensor_id: str, ts: datetime, rolling_avg: float):
    sql = text(
        """
        INSERT INTO sensor_rolling_avg (sensor_id, ts, rolling_avg)
        VALUES (:sensor_id, :ts, :rolling_avg)
        ON CONFLICT (sensor_id, ts) DO UPDATE
          SET rolling_avg = EXCLUDED.rolling_avg;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"sensor_id": sensor_id, "ts": ts, "rolling_avg": rolling_avg})

def main():
    consumer = create_consumer()
    engine = get_engine()  
    print("Listening to topic: sensor_readings. Press Ctrl+C to stop.\n")

    event_counter = defaultdict(int)
    last_reset = time.time()

    sensor_windows = defaultdict(lambda: deque(maxlen=20))

    try:
        for message in consumer:
            event = message.value
            sensor_id = event.get("sensor_id")
            value = event.get("value")

            print("Received:", event)

            current_timestamp = int(time.time())
            event_counter[current_timestamp] += 1

            if time.time() - last_reset >= 1:
                second_key = int(last_reset)
                count = event_counter.get(second_key, 0)

                ts = datetime.utcfromtimestamp(second_key).replace(microsecond=0)

                print(f"Events per second: {count} at {ts.isoformat()}")

                try:
                    write_events_per_second(engine, ts, count)
                except Exception as e:
                    print("Error writing events_per_second:", e)

                if second_key in event_counter:
                    del event_counter[second_key]
                last_reset = time.time()

            if sensor_id is not None and value is not None:
                sensor_windows[sensor_id].append(value)
                rolling_avg = sum(sensor_windows[sensor_id]) / len(sensor_windows[sensor_id])

                now_ts = datetime.utcnow().replace(microsecond=0)

                print(f"Sensor {sensor_id} | Value={value} | Rolling Avg={rolling_avg:.2f} at {now_ts.isoformat()}")

                try:
                    write_sensor_rolling_avg(engine, sensor_id, now_ts, rolling_avg)
                except Exception as e:
                    print("Error writing sensor_rolling_avg:", e)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
