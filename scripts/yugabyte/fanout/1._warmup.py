#!/usr/bin/env python3
# yb_prewarm.py
from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import List

import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)


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


def parse_hosts(hosts_csv: str) -> List[str]:
    hosts = [h.strip() for h in hosts_csv.split(",") if h.strip()]
    if not hosts:
        raise RuntimeError("YB_HOSTS must contain at least one host")
    return hosts


def connect_host(host: str):
    return psycopg2.connect(
        host=host,
        port=int(_get_env("YB_PORT", "5433")),
        dbname=_require_env("YB_DB"),
        user=_require_env("YB_USER"),
        password=os.getenv("YB_PASSWORD", ""),
        sslmode=_get_env("YB_SSLMODE", "prefer"),
        connect_timeout=int(_get_env("YB_CONNECT_TIMEOUT", "5")),
    )


def main() -> None:
    hosts = parse_hosts(_require_env("YB_HOSTS"))
    table = _get_env("YB_TABLE", "yb_counters")

    keyspace_size = int(_get_env("YB_KEYSPACE_SIZE", "100000"))
    prewarm_keys = int(_get_env("PREWARM_KEYS", str(keyspace_size)))
    prewarm_value = int(_get_env("PREWARM_VALUE", "1"))

    verify_sample = int(_get_env("PREWARM_VERIFY_SAMPLE", "200"))
    verify_timeout_s = int(_get_env("PREWARM_VERIFY_TIMEOUT_SECONDS", "30"))

    run_id = _get_env("RUN_ID", "run_id_not_set")
    out_dir = _get_env("PREWARM_OUT_DIR", f"runs/{run_id}/prewarm_yb")

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    meta = {
        "run_id": run_id,
        "ts_start_epoch": time.time(),
        "hosts": hosts,
        "table": table,
        "keyspace_size": keyspace_size,
        "prewarm_keys": prewarm_keys,
        "prewarm_value": prewarm_value,
        "verify_sample": verify_sample,
        "verify_timeout_s": verify_timeout_s,
    }

    print(f"Pre-warming {prewarm_keys} rows into {table}")

    errors = 0
    t0 = time.time()

    conns = [connect_host(h) for h in hosts]
    try:
        for c in conns:
            c.autocommit = True

        for i in range(prewarm_keys):
            conn = conns[i % len(conns)]
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {table} (k, v)
                        VALUES (%s, %s)
                        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
                        """,
                        (i, prewarm_value),
                    )
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"[ERROR] i={i} host={hosts[i % len(hosts)]} err={e}")

            if (i + 1) % 5000 == 0:
                print(f"  warmed {i+1}/{prewarm_keys}")

        meta["prewarm_seconds"] = time.time() - t0
        meta["prewarm_errors"] = errors

        if verify_sample > 0 and verify_timeout_s > 0:
            sample_n = min(verify_sample, prewarm_keys)
            sample_ids = random.sample(range(prewarm_keys), k=sample_n)

            print(f"Verifying {sample_n} sampled keys readable from all {len(hosts)} hosts...")
            deadline = time.time() + verify_timeout_s
            pending = set(sample_ids)
            attempts = 0

            while pending and time.time() < deadline:
                attempts += 1
                done_now = set()

                for key in list(pending):
                    ok_all = True
                    for conn in conns:
                        try:
                            with conn.cursor() as cur:
                                cur.execute(f"SELECT v FROM {table} WHERE k = %s", (key,))
                                row = cur.fetchone()
                                if row is None:
                                    ok_all = False
                                    break
                        except Exception:
                            ok_all = False
                            break
                    if ok_all:
                        done_now.add(key)

                pending -= done_now
                if pending:
                    time.sleep(0.5)

            meta["verify_attempts"] = attempts
            meta["verify_unreadable_remaining"] = len(pending)

            if pending:
                print(f"[WARN] {len(pending)} sampled keys not readable on all hosts within timeout.")
            else:
                print("Verification OK: sampled keys readable on all hosts.")
    finally:
        for c in conns:
            try:
                c.close()
            except Exception:
                pass

    meta["ts_end_epoch"] = time.time()
    (out_path / "prewarm_meta.json").write_text(json.dumps(meta, indent=2))
    print(f"Done. Metadata written to: {out_path / 'prewarm_meta.json'}")


if __name__ == "__main__":
    main()