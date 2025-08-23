import os, time
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
from streamlit_autorefresh import st_autorefresh


PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB   = os.getenv("PG_DB", "demo")
PG_USER = os.getenv("PG_USER", "demo")
PG_PASS = os.getenv("PG_PASS", "demo")

@st.cache_data(ttl=5)
def fetch(sql):
    with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return pd.DataFrame(rows)

st.set_page_config(page_title="Clickstream Demo", layout="wide")
st.title("Kafka → Postgres Clickstream (Demo)")
st.caption(f"DB: {PG_HOST}:{PG_PORT}/{PG_DB}")

col1, col2, col3 = st.columns(3)
total = fetch("SELECT COUNT(*) AS n FROM raw_events;")
col1.metric("Total events", int(total['n'].iloc[0]) if not total.empty else 0)

by_action = fetch("""
  SELECT action, COUNT(*) AS n
  FROM raw_events
  GROUP BY action
  ORDER BY n DESC;
""")
st.subheader("Events by Action")
if by_action.empty:
    st.info("No data yet. Start the producer.")
else:
    st.bar_chart(by_action.set_index("action")["n"])

st.caption("Auto-refresh every 5 seconds while running")
time.sleep(5)
st_autorefresh(interval=5000, limit=None, key="refresh")
