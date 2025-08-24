"""
Consumer: reads events from Kafka and persists them into Postgres (raw_events table).
"""

import os, json, logging
from confluent_kafka import Consumer, KafkaException
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clicks")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "demo")
PG_USER = os.getenv("PG_USER", "demo")
PG_PASS = os.getenv("PG_PASS", "demo")

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def ensure_table():
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_events (
            event_id TEXT PRIMARY KEY,
            user_id INT,
            session_id TEXT,
            action TEXT,
            metadata JSONB,
            event_time TIMESTAMP
        )
        """)
        conn.commit()
    logger.info("Ensured table public.raw_events exists.")

def run_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "clicks_ingestors",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info(f"Config -> Kafka: {KAFKA_BROKER} | topic: {KAFKA_TOPIC} | group: clicks_ingestors | PG: {PG_HOST}:{PG_PORT}/{PG_DB} user={PG_USER}")
    ensure_table()
    logger.info(f'Consuming from topic "{KAFKA_TOPIC}"')

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            event = json.loads(msg.value().decode("utf-8"))
            with pg_connect() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.raw_events (event_id, user_id, session_id, action, metadata, event_time)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (
                    event["event_id"], event["user_id"], event["session_id"],
                    event["action"], json.dumps(event["metadata"]), event["event_time"]
                ))
                conn.commit()
            logger.info(f"Inserted event {event['event_id']} action={event['action']}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    run_consumer()
