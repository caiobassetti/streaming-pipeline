import json, time, random, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "clicks"
BOOTSTRAP = "localhost:9092"

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 50,
    "message.timeout.ms": 5000,
}

p = Producer(conf)

users = list(range(100, 110))
campaigns = [12, 13, 14]
actions = ["view", "search", "click", "purchase"]

def fake_event():
    return {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "user_id": random.choice(users),
        "campaign_id": random.choice(campaigns),
        "action": random.choices(actions, weights=[0.5, 0.3, 0.18, 0.02])[0],
        "page": random.choice(["/home", "/search", "/results", "/checkout"]),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

if __name__ == "__main__":
    print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP}")
    try:
        while True:
            evt = fake_event()
            payload = json.dumps(evt).encode("utf-8")
            p.produce(TOPIC, value=payload, on_delivery=delivery_report)
            p.poll(0)
            print("→", evt)
            time.sleep(random.uniform(0.1, 0.8))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        p.flush(5)
