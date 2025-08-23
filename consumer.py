import json, os, time
from typing import Optional
from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()  # load .env if present

TOPIC = "clicks"
BOOTSTRAP = "localhost:9092"
GROUP_ID = "demo-consumer"

KAFKA_CONF = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

PG_CONN_INFO = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5433")),
    dbname=os.getenv("PG_DB", "demo"),
    user=os.getenv("PG_USER", "demo"),
    password=os.getenv("PG_PASS", "demo"),
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id UUID PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    user_id INT NOT NULL,
    campaign_id INT NOT NULL,
    action TEXT NOT NULL,
    page TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO raw_events (event_id, ts, user_id, campaign_id, action, page)
VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""

def pg_connect(retries: int = 20, delay: float = 1.0) -> psycopg2.extensions.connection:
    last_err: Optional[Exception] = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(**PG_CONN_INFO)
            conn.autocommit = False
            return conn
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Postgres: {last_err}")

def main():
    # Connect to Postgres and ensure table exists
    conn = pg_connect()
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    c = Consumer(KAFKA_CONF)
    c.subscribe([TOPIC])
    print(f"Connected to Postgres at {PG_CONN_INFO['host']}:{PG_CONN_INFO['port']}, DB={PG_CONN_INFO['dbname']}")
    print(f"Subscribed to Kafka topic '{TOPIC}' (group={GROUP_ID})")
    print(f"\nConsuming from '{TOPIC}'")
    buffer = []
    BATCH = 10  # commit every 10 rows (demo-level batching)

    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                if buffer:
                    execute_values(cur, INSERT_SQL, buffer)
                    conn.commit()
                    buffer.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                e = json.loads(msg.value().decode("utf-8"))
                print(f"← consumed: {e}")
                row = (
                    e["event_id"],
                    e["ts"],
                    int(e["user_id"]),
                    int(e["campaign_id"]),
                    str(e["action"]),
                    str(e["page"]),
                )
                buffer.append(row)
                if len(buffer) >= BATCH:
                    execute_values(cur, INSERT_SQL, buffer)
                    conn.commit()
                    print(f"✓ inserted {len(buffer)} rows into Postgres")
                    buffer.clear()
            except Exception as ex:
                print(f"Skipping malformed message: {ex}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        if buffer:
            try:
                execute_values(cur, INSERT_SQL, buffer)
                conn.commit()
                print(f"✓ inserted {len(buffer)} rows into Postgres")
            except Exception as ex:
                print(f"Failed to flush final batch: {ex}")
        cur.close()
        conn.close()
        c.close()

if __name__ == "__main__":
    main()
