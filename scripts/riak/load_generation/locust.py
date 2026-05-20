from dotenv import load_dotenv
load_dotenv()

import itertools
import os
import random
import time

import requests
from locust import User, task, between

'''
Note: You have to create the bucket
sudo -u riak /opt/openriak/bin/riak-admin bucket-type create counters '{"props":{"datatype":"counter"}}'
sudo -u riak /opt/openriak/bin/riak-admin bucket-type activate counters

'''


RIAK_NODES = os.getenv("RIAK_NODES", "riak1,riak2,riak3").split(",")
RIAK_HTTP_PORT = int(os.getenv("RIAK_HTTP_PORT", "8098"))

# CRDT counter bucket type you created with riak-admin (datatype=counter)
BUCKET_TYPE = os.getenv("RIAK_BUCKET_TYPE", "counters")

# Bucket name inside that type (just namespacing; pick anything)
BUCKET = os.getenv("RIAK_BUCKET", "devices")

# Key prefix to help you separate workloads (e.g., bg:device or ryw:device)
KEY_PREFIX = os.getenv("RIAK_KEY_PREFIX", "bg:device")

# Increment amount per write
INCREMENT_BY = int(os.getenv("RIAK_INCREMENT_BY", "1"))

# Key range
KEY_MIN = int(os.getenv("RIAK_KEY_MIN", "1"))
KEY_MAX = int(os.getenv("RIAK_KEY_MAX", "1000000"))

# Round-robin node picker (process-local; each Locust worker has its own cycle)
_node_cycle = itertools.cycle([n.strip() for n in RIAK_NODES if n.strip()])


class RiakUser(User):
    wait_time = between(0.01, 0.2)

    def on_start(self):
        self.riak_host = next(_node_cycle)
        self.session = requests.Session()
        self.base_url = f"http://{self.riak_host}:{RIAK_HTTP_PORT}"

    def _fire_metric(self, name: str, start: float, exc: Exception | None = None, response_length: int = 0):
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        self.environment.events.request.fire(
            request_type="riak_http",
            name=name,
            response_time=elapsed_ms,
            response_length=response_length,
            exception=exc,
        )

    def _counter_url(self, key: str) -> str:
        # Riak HTTP datatypes endpoint (CRDTs)
        # /types/<bucket_type>/buckets/<bucket>/datatypes/<key>
        return f"{self.base_url}/types/{BUCKET_TYPE}/buckets/{BUCKET}/datatypes/{key}"

    @task(1)
    def read_counter(self):
        key = f"{KEY_PREFIX}:{random.randint(KEY_MIN, KEY_MAX)}"
        url = self._counter_url(key)

        start = time.perf_counter()
        try:
            r = self.session.get(url, headers={"Accept": "application/json"}, timeout=5)

            # 200 = found, 404 = not found (counter not created yet)
            if r.status_code in (200, 404):
                self._fire_metric("CRDT_GET_COUNTER", start, None, len(r.content or b""))
            else:
                self._fire_metric("CRDT_GET_COUNTER", start, Exception(f"HTTP {r.status_code}: {r.text[:200]}"), len(r.content or b""))
        except Exception as e:
            self._fire_metric("CRDT_GET_COUNTER", start, e)

    @task(9)
    def increment_counter(self):
        key = f"{KEY_PREFIX}:{random.randint(KEY_MIN, KEY_MAX)}"
        url = self._counter_url(key)

        # CRDT counter update: increment by N
        payload = {"increment": INCREMENT_BY}

        start = time.perf_counter()
        try:
            r = self.session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=5,
            )

            # Typical success for datatype update is 204 No Content; some setups may return 200
            if r.status_code in (200, 201, 204):
                self._fire_metric("CRDT_POST_INCREMENT", start, None, len(r.content or b""))
            else:
                self._fire_metric("CRDT_POST_INCREMENT", start, Exception(f"HTTP {r.status_code}: {r.text[:200]}"), len(r.content or b""))
        except Exception as e:
            self._fire_metric("CRDT_POST_INCREMENT", start, e)
