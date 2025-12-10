from kafka import KafkaConsumer
import json
import time
from collections import defaultdict, deque


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


def main():
    consumer = create_consumer()
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
                print(f"Events per second: {count}")

                if second_key in event_counter:
                    del event_counter[second_key]

                last_reset = time.time()

            if sensor_id is not None and value is not None:
                sensor_windows[sensor_id].append(value)
                rolling_avg = sum(sensor_windows[sensor_id]) / len(sensor_windows[sensor_id])
                print(f"Sensor {sensor_id} | Value={value} | Rolling Avg={rolling_avg:.2f}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
