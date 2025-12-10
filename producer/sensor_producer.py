import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


def create_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    return producer


def generate_sensor_event() -> dict:
    sensor_id = f"sensor-{random.randint(1, 5)}"
    timestamp = datetime.utcnow().isoformat()
    value = round(random.uniform(15.0, 35.0), 2)

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "value": value,
    }


def main():
    topic_name = "sensor_readings"
    producer = create_producer()
    print(f"Producing messages to topic: {topic_name}. Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_sensor_event()
            producer.send(topic_name, value=event)
            print("Sent:", event)
            producer.flush()
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
