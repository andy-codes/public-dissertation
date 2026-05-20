from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import gevent
from gevent.lock import Semaphore
from gevent.pool import Pool
from gevent.queue import Empty, Queue
from locust import User, task, between
import psycopg2
from psycopg2 import InterfaceError, OperationalError, sql
from psycogreen.gevent import patch_psycopg
from dotenv import load_dotenv

patch_psycopg()
load_dotenv(override=False)


@dataclass
class FanoutConfig:
    hosts: List[str]
    table: str
    keyspace_size: int
    hotset_size: int
    hotset_prob: float
    fanout_k: int
    concurrency_cap: int
    acquire_timeout_s: float
    statement_timeout_s: float
    logical_deadline_ms: int
    rng_seed: int
    dbname: str
    user: str
    password: str
    port: int
    sslmode: str
    per_host_pool_size: int


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


def _new_connection(cfg: FanoutConfig, host: str):
    conn = psycopg2.connect(
        host=host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        sslmode=cfg.sslmode,
        connect_timeout=5,
    )
    conn.autocommit = True
    return conn


class PerUserYBPoolManager:
    """
    Lazy per-user, per-host bounded connection pool.
    Connections are created on demand up to per_host_pool_size, then callers
    block waiting for a returned connection.
    """

    def __init__(self, cfg: FanoutConfig):
        self.cfg = cfg
        self._closed = False
        self.queues: Dict[str, Queue] = {}
        self.created_counts: Dict[str, int] = {}
        self.locks: Dict[str, Semaphore] = {}

        for host in cfg.hosts:
            self.queues[host] = Queue()
            self.created_counts[host] = 0
            self.locks[host] = Semaphore()

    def _make_conn(self, host: str):
        conn = _new_connection(self.cfg, host)
        with conn.cursor() as cur:
            cur.execute(
                "SET statement_timeout = %s",
                (int(self.cfg.statement_timeout_s * 1000),),
            )
        return conn

    @staticmethod
    def _is_usable(conn) -> bool:
        try:
            return conn is not None and conn.closed == 0
        except Exception:
            return False

    def acquire(self, host: str, timeout: Optional[float] = None):
        if self._closed:
            raise RuntimeError("per-user pool manager is closed")

        q = self.queues[host]
        start = time.perf_counter()

        try:
            conn = q.get_nowait()
            if self._is_usable(conn):
                return conn
            try:
                conn.close()
            except Exception:
                pass
            with self.locks[host]:
                self.created_counts[host] = max(0, self.created_counts[host] - 1)
        except Empty:
            pass

        with self.locks[host]:
            if self.created_counts[host] < self.cfg.per_host_pool_size:
                self.created_counts[host] += 1
                should_create = True
            else:
                should_create = False

        if should_create:
            try:
                return self._make_conn(host)
            except Exception:
                with self.locks[host]:
                    self.created_counts[host] = max(0, self.created_counts[host] - 1)
                raise

        remaining = timeout
        while True:
            if self._closed:
                raise RuntimeError("per-user pool manager is closed")

            try:
                conn = q.get(timeout=remaining)
            except Empty as e:
                raise RuntimeError(
                    f"timed out waiting for DB connection for host {host}"
                ) from e

            if self._is_usable(conn):
                return conn

            try:
                conn.close()
            except Exception:
                pass

            with self.locks[host]:
                self.created_counts[host] = max(0, self.created_counts[host] - 1)
                if self.created_counts[host] < self.cfg.per_host_pool_size:
                    self.created_counts[host] += 1
                    should_create = True
                else:
                    should_create = False

            if should_create:
                try:
                    return self._make_conn(host)
                except Exception:
                    with self.locks[host]:
                        self.created_counts[host] = max(0, self.created_counts[host] - 1)
                    raise

            if timeout is not None:
                elapsed = time.perf_counter() - start
                remaining = timeout - elapsed
                if remaining <= 0:
                    raise RuntimeError(
                        f"timed out waiting for DB connection for host {host}"
                    )

    def release(self, host: str, conn, *, discard: bool = False) -> None:
        if conn is None:
            return

        if self._closed or discard or not self._is_usable(conn):
            try:
                conn.close()
            except Exception:
                pass
            with self.locks[host]:
                self.created_counts[host] = max(0, self.created_counts[host] - 1)
            return

        self.queues[host].put(conn)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        for host, q in self.queues.items():
            while True:
                try:
                    conn = q.get_nowait()
                except Exception:
                    break
                try:
                    conn.close()
                except Exception:
                    pass
            with self.locks[host]:
                self.created_counts[host] = 0


class YugabyteFanoutUser(User):
    wait_time = between(0.2, 0.5)

    def on_start(self):
        hosts = _require_env("YB_HOSTS")
        self.cfg = FanoutConfig(
            hosts=[h.strip() for h in hosts.split(",") if h.strip()],
            table=_get_env("YB_TABLE", "yb_counters"),
            keyspace_size=int(_get_env("YB_KEYSPACE_SIZE", "100000")),
            hotset_size=int(_get_env("YB_HOTSET_SIZE", "10000")),
            hotset_prob=float(_get_env("YB_HOTSET_PROB", "0.8")),
            fanout_k=int(_get_env("FANOUT_K", "10")),
            concurrency_cap=int(_get_env("FANOUT_CONCURRENCY_CAP", "20")),
            acquire_timeout_s=float(_get_env("FANOUT_ACQUIRE_TIMEOUT_S", "0.5")),
            statement_timeout_s=float(_get_env("FANOUT_STATEMENT_TIMEOUT_S", "1.0")),
            logical_deadline_ms=int(_get_env("FANOUT_DEADLINE_MS", "1000")),
            rng_seed=int(_get_env("FANOUT_SEED", "12345")),
            dbname=_require_env("YB_DB"),
            user=_require_env("YB_USER"),
            password=os.getenv("YB_PASSWORD", ""),
            port=int(_get_env("YB_PORT", "5433")),
            sslmode=_get_env("YB_SSLMODE", "prefer"),
            per_host_pool_size=int(_get_env("YB_POOL_PER_HOST", "24")),
        )

        if not self.cfg.hosts:
            raise RuntimeError("YB_HOSTS must contain at least one host")
        if self.cfg.hotset_size > self.cfg.keyspace_size:
            raise RuntimeError("YB_HOTSET_SIZE cannot exceed YB_KEYSPACE_SIZE")
        if not (0.0 <= self.cfg.hotset_prob <= 1.0):
            raise RuntimeError("YB_HOTSET_PROB must be between 0.0 and 1.0")
        if self.cfg.fanout_k < 1:
            raise RuntimeError("FANOUT_K must be >= 1")
        if self.cfg.concurrency_cap < 1:
            raise RuntimeError("FANOUT_CONCURRENCY_CAP must be >= 1")
        if self.cfg.per_host_pool_size < 1:
            raise RuntimeError("YB_POOL_PER_HOST must be >= 1")

        self.run_id = _get_env("RUN_ID", "run_id_not_set")
        self.stream = _get_env("STREAM_NAME", "fanout")
        self.log_path = Path(_get_env("FANOUT_JSONL_PATH", "fanout_requests.jsonl"))
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        base_seed = self.cfg.rng_seed + (id(self) % 10_000_000)
        self.rng = random.Random(base_seed)

        self.pool_mgr = PerUserYBPoolManager(self.cfg)
        self._lingering_greenlets: List[gevent.Greenlet] = []

    def _drain_lingering(self) -> None:
        if not self._lingering_greenlets:
            return

        pending = [g for g in self._lingering_greenlets if not g.ready()]
        for g in pending:
            try:
                g.kill(block=False)
            except Exception:
                pass

        gevent.joinall(
            pending,
            timeout=self.cfg.statement_timeout_s + 0.1,
            raise_error=False,
        )
        self._lingering_greenlets = []

    def on_stop(self):
        self._drain_lingering()
        if hasattr(self, "pool_mgr") and self.pool_mgr is not None:
            self.pool_mgr.close()

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

    def _subread(self, host: str, key: int) -> dict:
        """
        Perform one subread and return a structured result dict instead of raising.
        This avoids noisy greenlet tracebacks and makes failure accounting explicit.
        """
        start = time.perf_counter()
        acquire_start = start
        conn = None
        discard_conn = False

        try:
            conn = self.pool_mgr.acquire(host, timeout=self.cfg.acquire_timeout_s)
            acquire_wait_ms = (time.perf_counter() - acquire_start) * 1000.0

            if conn.closed != 0:
                discard_conn = True
                return {
                    "ok": False,
                    "error_type": "ClosedConnection",
                    "error": f"acquired closed connection for host {host}",
                    "acquire_wait_ms": acquire_wait_ms,
                }

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT v FROM {} WHERE k = %s").format(
                        sql.Identifier(self.cfg.table)
                    ),
                    (key,),
                )
                row = cur.fetchone()
                if row is None:
                    return {
                        "ok": False,
                        "error_type": "MissingKey",
                        "error": f"missing key {key}",
                        "acquire_wait_ms": acquire_wait_ms,
                    }

                value = int(row[0])

            subread_latency_ms = (time.perf_counter() - start) * 1000.0
            return {
                "ok": True,
                "value": value,
                "subread_latency_ms": subread_latency_ms,
                "acquire_wait_ms": acquire_wait_ms,
            }

        except psycopg2.errors.QueryCanceled as e:
            discard_conn = True
            return {
                "ok": False,
                "error_type": "QueryCanceled",
                "error": str(e),
                "acquire_wait_ms": (time.perf_counter() - acquire_start) * 1000.0,
            }

        except (InterfaceError, OperationalError) as e:
            discard_conn = True
            return {
                "ok": False,
                "error_type": type(e).__name__,
                "error": repr(e),
                "acquire_wait_ms": (time.perf_counter() - acquire_start) * 1000.0,
            }

        except Exception as e:
            return {
                "ok": False,
                "error_type": type(e).__name__,
                "error": repr(e),
                "acquire_wait_ms": (time.perf_counter() - acquire_start) * 1000.0,
            }

        finally:
            if conn is not None:
                self.pool_mgr.release(host, conn, discard=discard_conn)

    @task
    def fanout_request(self):
        self._drain_lingering()

        K = self.cfg.fanout_k
        deadline_ms = self.cfg.logical_deadline_ms
        deadline_s = deadline_ms / 1000.0

        key_ids = self._pick_distinct_key_ids(K)
        targets = [self.cfg.hosts[self.rng.randrange(len(self.cfg.hosts))] for _ in range(K)]

        start = time.perf_counter()
        req_pool = Pool(size=max(1, self.cfg.concurrency_cap))

        greenlets = [
            req_pool.spawn(self._subread, host, key)
            for host, key in zip(targets, key_ids)
        ]

        gevent.joinall(greenlets, timeout=deadline_s, raise_error=False)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        completed = 0
        errors = 0
        timed_out = False
        acquire_timeouts = 0
        query_canceled = 0
        subread_latencies: List[float] = []
        acquire_waits_ms: List[float] = []

        last_error = None
        error_types = set()

        for g in greenlets:
            if not g.ready():
                timed_out = True
                continue

            result = g.get()

            if result.get("ok"):
                completed += 1
                subread_latencies.append(float(result["subread_latency_ms"]))
                acquire_waits_ms.append(float(result["acquire_wait_ms"]))
            else:
                errors += 1
                last_error = result.get("error")
                err_type = result.get("error_type", "UnknownError")
                error_types.add(err_type)

                if err_type == "QueryCanceled":
                    query_canceled += 1
                if "timed out waiting for DB connection" in str(result.get("error", "")):
                    acquire_timeouts += 1

        pending = [g for g in greenlets if not g.ready()]
        for g in pending:
            try:
                g.kill(block=False)
            except Exception:
                pass

        self._lingering_greenlets = pending

        max_subread_ms = max(subread_latencies) if subread_latencies else None
        min_subread_ms = min(subread_latencies) if subread_latencies else None
        mean_subread_ms = (
            sum(subread_latencies) / len(subread_latencies)
            if subread_latencies else None
        )
        max_acquire_wait_ms = max(acquire_waits_ms) if acquire_waits_ms else None
        mean_acquire_wait_ms = (
            sum(acquire_waits_ms) / len(acquire_waits_ms)
            if acquire_waits_ms else None
        )

        ok = (completed == K) and (errors == 0) and (not timed_out) and (elapsed_ms <= deadline_ms)

        outcome = "ok"
        exception = None
        if not ok:
            if timed_out or elapsed_ms > deadline_ms:
                outcome = "timeout_or_slo_miss"
                exception = Exception(
                    f"{outcome}: completed={completed}/{K} errors={errors} elapsed_ms={elapsed_ms:.1f}"
                )
            elif errors > 0:
                outcome = "subread_error"
                exception = Exception(
                    f"{outcome}: completed={completed}/{K} errors={errors}"
                )
            else:
                outcome = "partial"
                exception = Exception(
                    f"{outcome}: completed={completed}/{K} errors={errors}"
                )

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
            "acquire_timeouts": acquire_timeouts,
            "query_canceled": query_canceled,
            "outcome": outcome,
            "last_error": last_error,
            "error_types": list(error_types),
            "max_subread_ms": max_subread_ms,
            "min_subread_ms": min_subread_ms,
            "mean_subread_ms": mean_subread_ms,
            "subread_count_completed": len(subread_latencies),
            "max_acquire_wait_ms": max_acquire_wait_ms,
            "mean_acquire_wait_ms": mean_acquire_wait_ms,
            "table": self.cfg.table,
            "target_host_counts": {h: targets.count(h) for h in self.cfg.hosts},
            "per_host_pool_size": self.cfg.per_host_pool_size,
        })