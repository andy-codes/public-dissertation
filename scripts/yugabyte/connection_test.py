# yb_connect_test.py
from __future__ import annotations

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=False)

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or not str(v).strip():
        raise RuntimeError(f"Missing required env var: {name}")
    return str(v).strip()

YB_HOST = env("YB_HOST", "127.0.0.1")
YB_PORT = int(env("YB_PORT", "5433"))
YB_DB = env("YB_DB", "fanoutdb")
YB_USER = env("YB_USER", "yugabyte")
YB_PASSWORD = os.getenv("YB_PASSWORD", "")
YB_SSLMODE = env("YB_SSLMODE", "prefer")

conn = psycopg2.connect(
    host=YB_HOST,
    port=YB_PORT,
    dbname=YB_DB,
    user=YB_USER,
    password=YB_PASSWORD,
    sslmode=YB_SSLMODE,
    connect_timeout=5,
)

try:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(cur.fetchone()[0])

        cur.execute("SELECT current_database(), current_user")
        print(cur.fetchone())

        cur.execute("SELECT count(*) FROM yb_counters")
        print("row_count =", cur.fetchone()[0])
finally:
    conn.close()