"""
Producer: simulates clickstream events and sends them to Kafka.
"""

import os, json, time, random, uuid
from confluent_kafka import Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clicks")

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

actions = ["page_view", "add_to_cart", "purchase"]

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Produced event to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

print(f"Starting producer to {KAFKA_BROKER}, topic {KAFKA_TOPIC}")

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "session_id": str(uuid.uuid4()),
        "action": random.choice(actions),
        "metadata": {"campaign": random.choice(["summer", "spring", "fall"])},
        "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    producer.produce(KAFKA_TOPIC, json.dumps(event).encode("utf-8"), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)
