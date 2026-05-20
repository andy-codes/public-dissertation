from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import gevent
from gevent.pool import Pool
import requests
from locust import User, task, between

# pip install python-dotenv
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

# Load .env from current working directory (does not override already-set env vars)
load_dotenv(override=False)


# ----------------------------
# Config + helpers
# ----------------------------

@dataclass
class FanoutConfig:
    nodes: List[str]              # base URLs like http://ip:8098
    dtype_type: str               # datatype bucket-type name, e.g. "counters"
    bucket: str                   # bucket name within the type, e.g. "bg" or "fanout"
    key_prefix: str               # e.g. "bg:"
    keyspace_size: int            # e.g. 100000
    hotset_size: int              # e.g. 10000
    hotset_prob: float            # e.g. 0.8
    fanout_k: int                 # K keys per logical request
    concurrency_cap: int          # max in-flight subreads per logical request
    subread_timeout_s: float      # per GET timeout
    logical_deadline_ms: int      # deadline for the entire logical request (SLO)
    rng_seed: int                 # reproducibility


def dt_url(node: str, dtype_type: str, bucket: str, key: str) -> str:
    node = node.rstrip("/")
    return f"{node}/types/{dtype_type}/buckets/{bucket}/datatypes/{key}"


def get_counter(session: requests.Session, url: str, timeout_s: float) -> int:
    r = session.get(url, timeout=timeout_s, headers={"Accept": "application/json"})
    if r.status_code != 200:
        raise RuntimeError(f"GET {r.status_code} {r.text[:200]}")
    data = r.json()
    if "value" not in data:
        raise RuntimeError(f"Unexpected JSON: {data}")
    return int(data["value"])


def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _get_env(name: str, default: str) -> str:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return v.strip()


# ----------------------------
# Locust User: Fan-out logical reads
# ----------------------------

class RiakFanoutUser(User):
    """
    Executes ONE *logical* fan-out request per task execution:

      - choose K distinct keys (hotset-biased)
      - issue K sub-reads concurrently, bounded by concurrency_cap
      - enforce a logical deadline (logical_deadline_ms)
      - report ONE Locust metric event per logical request:
            request_type="FANOUT", name=f"K{K}"
        and mark failure if not all K sub-reads succeed within the deadline.

    Additionally writes a JSONL record per logical request with:
      ok | partial | error
      completed_count, errors_count, timed_out flag, latency_ms, etc.

    Enhancements added:
      - Measures per-subread latency and records min/mean/max subread latency per logical request.
      - These stats support tail-amplification analysis:
            logical latency ~= max(subread latency) as K increases.
    """

    wait_time = between(0.2, 0.5)

    def on_start(self):
        # Required
        nodes = _require_env("RIAK_NODES")
        dtype_type = _require_env("RIAK_TYPE")
        bucket = _require_env("RIAK_BUCKET")

        self.cfg = FanoutConfig(
            nodes=[n.strip() for n in nodes.split(",") if n.strip()],
            dtype_type=dtype_type,
            bucket=bucket,
            # Optional with defaults
            key_prefix=_get_env("RIAK_KEY_PREFIX", "bg:"),
            keyspace_size=int(_get_env("RIAK_KEYSPACE_SIZE", "100000")),
            hotset_size=int(_get_env("RIAK_HOTSET_SIZE", "10000")),
            hotset_prob=float(_get_env("RIAK_HOTSET_PROB", "0.8")),
            fanout_k=int(_get_env("FANOUT_K", "10")),
            concurrency_cap=int(_get_env("FANOUT_CONCURRENCY_CAP", "20")),
            subread_timeout_s=float(_get_env("FANOUT_SUBREAD_TIMEOUT_S", "3.0")),
            logical_deadline_ms=int(_get_env("FANOUT_DEADLINE_MS", "500")),
            rng_seed=int(_get_env("FANOUT_SEED", "12345")),
        )

        if not self.cfg.nodes:
            raise RuntimeError("RIAK_NODES must contain at least one node URL")
        if self.cfg.hotset_size > self.cfg.keyspace_size:
            raise RuntimeError("RIAK_HOTSET_SIZE cannot exceed RIAK_KEYSPACE_SIZE")
        if not (0.0 <= self.cfg.hotset_prob <= 1.0):
            raise RuntimeError("RIAK_HOTSET_PROB must be between 0.0 and 1.0")
        if self.cfg.fanout_k < 1:
            raise RuntimeError("FANOUT_K must be >= 1")
        if self.cfg.concurrency_cap < 1:
            raise RuntimeError("FANOUT_CONCURRENCY_CAP must be >= 1")

        self.session = requests.Session()

        adapter = HTTPAdapter(
            pool_connections=200,  # number of host pools
            pool_maxsize=200,      # max connections per host
            max_retries=0,
            pool_block=True,       # IMPORTANT: block instead of creating/discarding
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Run metadata
        self.run_id = _get_env("RUN_ID", "run_id_not_set")
        self.stream = _get_env("STREAM_NAME", "fanout")

        # Per-user RNG (stable-ish)
        base_seed = self.cfg.rng_seed + (id(self) % 10_000_000)
        self.rng = random.Random(base_seed)

        # JSONL output
        log_path = _get_env("FANOUT_JSONL_PATH", "fanout_requests.jsonl")
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        # Pre-create a greenlet pool per user for bounded concurrency
        self.pool = Pool(size=max(1, self.cfg.concurrency_cap))

    def _append_jsonl(self, record: dict) -> None:
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")

    def _pick_distinct_key_ids(self, k: int) -> List[int]:
        chosen = set()
        while len(chosen) < k:
            if self.rng.random() < self.cfg.hotset_prob:
                kid = self.rng.randrange(self.cfg.hotset_size)
            else:
                kid = self.rng.randrange(self.cfg.keyspace_size)
            chosen.add(kid)
        return list(chosen)

    def _subread(self, node: str, key: str, timeout_s: float) -> Tuple[int, float]:
        """
        Perform one subread and return (value, subread_latency_ms).
        """
        start = time.perf_counter()
        url = dt_url(node, self.cfg.dtype_type, self.cfg.bucket, key)
        value = get_counter(self.session, url, timeout_s=timeout_s)
        sub_latency_ms = (time.perf_counter() - start) * 1000.0
        return value, sub_latency_ms

    @task
    def fanout_request(self):
        K = self.cfg.fanout_k
        deadline_ms = self.cfg.logical_deadline_ms
        deadline_s = deadline_ms / 1000.0

        # choose keys
        key_ids = self._pick_distinct_key_ids(K)
        keys = [f"{self.cfg.key_prefix}{kid}" for kid in key_ids]

        # choose a target node per subread (random)
        targets = [self.cfg.nodes[self.rng.randrange(len(self.cfg.nodes))] for _ in range(K)]

        start = time.perf_counter()

        # spawn subreads (bounded by pool size)
        greenlets = []
        for node, key in zip(targets, keys):
            g = self.pool.spawn(self._subread, node, key, self.cfg.subread_timeout_s)
            greenlets.append(g)

        # wait up to deadline for completion
        gevent.joinall(greenlets, timeout=deadline_s, raise_error=False)

        elapsed_ms = (time.perf_counter() - start) * 1000.0

        completed = 0
        errors = 0
        timed_out = False

        # Enhancement: collect per-subread latencies for completed subreads
        subread_latencies: List[float] = []

        for g in greenlets:
            if not g.ready():
                timed_out = True
                continue
            try:
                _, sub_latency_ms = g.get()
                completed += 1
                subread_latencies.append(float(sub_latency_ms))
            except Exception:
                errors += 1

        if timed_out:
            for g in greenlets:
                if not g.ready():
                    g.kill(block=False)

        # Enhancement: compute subread latency summary stats
        max_subread_ms: Optional[float] = None
        min_subread_ms: Optional[float] = None
        mean_subread_ms: Optional[float] = None

        if subread_latencies:
            max_subread_ms = max(subread_latencies)
            min_subread_ms = min(subread_latencies)
            mean_subread_ms = sum(subread_latencies) / len(subread_latencies)

        ok = (completed == K) and (errors == 0) and (not timed_out) and (elapsed_ms <= deadline_ms)

        exception = None
        outcome = "ok"
        if not ok:
            if timed_out or elapsed_ms > deadline_ms:
                outcome = "timeout_or_slo_miss"
                exception = Exception(
                    f"{outcome}: completed={completed}/{K} errors={errors} elapsed_ms={elapsed_ms:.1f}"
                )
            elif errors > 0:
                outcome = "subread_error"
                exception = Exception(f"{outcome}: completed={completed}/{K} errors={errors}")
            else:
                outcome = "partial"
                exception = Exception(f"{outcome}: completed={completed}/{K} errors={errors}")

        self.environment.events.request.fire(
            request_type="FANOUT",
            name=f"K{K}",
            response_time=elapsed_ms,
            response_length=0,
            exception=exception,
        )

        self._append_jsonl({
            "ts_epoch": time.time(),
            "run_id": self.run_id,
            "stream": self.stream,
            "k": K,
            "concurrency_cap": self.cfg.concurrency_cap,
            "deadline_ms": deadline_ms,
            "latency_ms": elapsed_ms,
            "completed": completed,
            "errors": errors,
            "timed_out": timed_out,
            "outcome": outcome,
            # Enhancement fields
            "max_subread_ms": max_subread_ms,
            "min_subread_ms": min_subread_ms,
            "mean_subread_ms": mean_subread_ms,
            "subread_count_completed": len(subread_latencies),
            "bucket_type": self.cfg.dtype_type,
            "bucket": self.cfg.bucket,
            "key_prefix": self.cfg.key_prefix,
        })