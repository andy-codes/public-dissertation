# yb_background_locust.py
from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import itertools
import os
import random
import time

import psycopg2
from psycogreen.gevent import patch_psycopg
patch_psycopg()

from locust import User, task, between



YB_HOSTS = [h.strip() for h in os.getenv("YB_HOSTS", "127.0.0.1").split(",") if h.strip()]
YB_PORT = int(os.getenv("YB_PORT", "5433"))
YB_DB = os.getenv("YB_DB", "fanoutdb")
YB_USER = os.getenv("YB_USER", "yugabyte")
YB_PASSWORD = os.getenv("YB_PASSWORD", "")
YB_SSLMODE = os.getenv("YB_SSLMODE", "prefer")
YB_TABLE = os.getenv("YB_TABLE", "yb_counters")

INCREMENT_BY = int(os.getenv("YB_INCREMENT_BY", "1"))
KEY_MIN = int(os.getenv("YB_KEY_MIN", "0"))
KEY_MAX = int(os.getenv("YB_KEY_MAX", "99999"))

_node_cycle = itertools.cycle(YB_HOSTS)


class YugabyteUser(User):
    wait_time = between(0.01, 0.2)

    def on_start(self):
        self.yb_host = next(_node_cycle)
        self.conn = psycopg2.connect(
            host=self.yb_host,
            port=YB_PORT,
            dbname=YB_DB,
            user=YB_USER,
            password=YB_PASSWORD,
            sslmode=YB_SSLMODE,
            connect_timeout=5,
        )
        self.conn.autocommit = True

    def on_stop(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _fire_metric(self, name: str, start: float, exc: Exception | None = None):
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        self.environment.events.request.fire(
            request_type="ysql",
            name=name,
            response_time=elapsed_ms,
            response_length=0,
            exception=exc,
        )

    @task(1)
    def read_counter(self):
        key = random.randint(KEY_MIN, KEY_MAX)
        start = time.perf_counter()
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT v FROM {YB_TABLE} WHERE k = %s", (key,))
                _ = cur.fetchone()
            self._fire_metric("YSQL_GET_COUNTER", start, None)
        except Exception as e:
            self._fire_metric("YSQL_GET_COUNTER", start, e)

    @task(9)
    def increment_counter(self):
        key = random.randint(KEY_MIN, KEY_MAX)
        start = time.perf_counter()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {YB_TABLE} SET v = v + %s WHERE k = %s",
                    (INCREMENT_BY, key),
                )
            self._fire_metric("YSQL_UPDATE_INCREMENT", start, None)
        except Exception as e:
            self._fire_metric("YSQL_UPDATE_INCREMENT", start, e)