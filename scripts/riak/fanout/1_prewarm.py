#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import List

import requests

# pip install python-dotenv
from dotenv import load_dotenv


# Load .env from current working directory (does not override already-set env vars)
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


def parse_nodes(nodes_csv: str) -> List[str]:
    nodes = [n.strip() for n in nodes_csv.split(",") if n.strip()]
    if not nodes:
        raise RuntimeError("RIAK_NODES must contain at least one node URL")
    return nodes


def dt_url(node: str, dtype_type: str, bucket: str, key: str) -> str:
    node = node.rstrip("/")
    return f"{node}/types/{dtype_type}/buckets/{bucket}/datatypes/{key}"


def inc_counter(session: requests.Session, url: str, delta: int, timeout_s: float) -> None:
    r = session.post(url, json={"increment": delta}, timeout=timeout_s)
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"POST {r.status_code} {r.text[:200]}")


def get_counter(session: requests.Session, url: str, timeout_s: float) -> int:
    r = session.get(url, headers={"Accept": "application/json"}, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"GET {r.status_code} {r.text[:200]}")
    data = r.json()
    if "value" not in data:
        raise RuntimeError(f"Unexpected JSON: {data}")
    return int(data["value"])


def main() -> None:
    # Required (shared with your fanout script)
    nodes = parse_nodes(_require_env("RIAK_NODES"))
    dtype_type = _require_env("RIAK_TYPE")
    bucket = _require_env("RIAK_BUCKET")

    # Shared optional keyspace config
    key_prefix = _get_env("RIAK_KEY_PREFIX", "fanout:")
    keyspace_size = int(_get_env("RIAK_KEYSPACE_SIZE", "100000"))

    # Prewarm-specific config (new env vars)
    prewarm_keys = int(_get_env("PREWARM_KEYS", str(keyspace_size)))
    prewarm_delta = int(_get_env("PREWARM_DELTA", "1"))

    http_timeout_s = float(_get_env("RIAK_HTTP_TIMEOUT", "3.0"))

    verify_sample = int(_get_env("PREWARM_VERIFY_SAMPLE", "200"))
    verify_timeout_s = int(_get_env("PREWARM_VERIFY_TIMEOUT_SECONDS", "30"))

    run_id = _get_env("RUN_ID", "run_id_not_set")
    out_dir = _get_env("PREWARM_OUT_DIR", f"runs/{run_id}/prewarm_fanout")

    if prewarm_keys < 1:
        raise RuntimeError("PREWARM_KEYS must be >= 1")

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    meta = {
        "run_id": run_id,
        "ts_start_epoch": time.time(),
        "nodes": nodes,
        "bucket_type": dtype_type,
        "bucket": bucket,
        "key_prefix": key_prefix,
        "keyspace_size": keyspace_size,
        "prewarm_keys": prewarm_keys,
        "prewarm_delta": prewarm_delta,
        "http_timeout_s": http_timeout_s,
        "verify_sample": verify_sample,
        "verify_timeout_s": verify_timeout_s,
    }

    print(f"Pre-warming {prewarm_keys} keys in bucket '{bucket}' (type '{dtype_type}') prefix '{key_prefix}'")
    session = requests.Session()

    errors = 0
    t0 = time.time()

    # Round-robin coordinator nodes to warm all nodes fairly
    for i in range(prewarm_keys):
        node = nodes[i % len(nodes)]
        key = f"{key_prefix}{i}"
        url = dt_url(node, dtype_type, bucket, key)
        try:
            inc_counter(session, url, delta=prewarm_delta, timeout_s=http_timeout_s)
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"[ERROR] i={i} node={node} key={key} err={e}")

        if (i + 1) % 5000 == 0:
            print(f"  warmed {i+1}/{prewarm_keys}")

    meta["prewarm_seconds"] = time.time() - t0
    meta["prewarm_errors"] = errors

    # Optional visibility check across nodes (lightweight sanity check)
    if verify_sample > 0 and verify_timeout_s > 0:
        import random

        sample_n = min(verify_sample, prewarm_keys)
        sample_ids = random.sample(range(prewarm_keys), k=sample_n)
        sample_keys = [f"{key_prefix}{i}" for i in sample_ids]

        print(f"Verifying {sample_n} sampled keys readable on all {len(nodes)} nodes (timeout {verify_timeout_s}s)...")
        deadline = time.time() + verify_timeout_s

        pending = set(sample_keys)
        attempts = 0
        while pending and time.time() < deadline:
            attempts += 1
            done_now = set()
            for key in list(pending):
                ok_all = True
                for node in nodes:
                    url = dt_url(node, dtype_type, bucket, key)
                    try:
                        _ = get_counter(session, url, timeout_s=http_timeout_s)
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
            print(f"[WARN] {len(pending)} sampled keys not readable on all nodes within verify timeout.")
        else:
            print("Verification OK: sampled keys readable on all nodes.")

    meta["ts_end_epoch"] = time.time()
    (out_path / "prewarm_meta.json").write_text(json.dumps(meta, indent=2))
    print(f"Done. Metadata written to: {out_path / 'prewarm_meta.json'}")


if __name__ == "__main__":
    main()
