
## Run Pre-warm
```
pip install requests python-dotenv
python3 riak_prewarm_fanout_from_env.py

```

## Run it (Riak fan-out workload)
Set env vars (example)
```
export RIAK_NODES="http://<n1>:8098,http://<n2>:8098,http://<n3>:8098"
export RIAK_TYPE="counters"          # bucket type (CRDT counter type)
export RIAK_BUCKET="fanout"          # bucket name (separate from bg/ryw is recommended)
export RIAK_KEY_PREFIX="bg:"         # or "fan:" if you prefer
export RIAK_KEYSPACE_SIZE="100000"
export RIAK_HOTSET_SIZE="10000"
export RIAK_HOTSET_PROB="0.8"

export FANOUT_K="50"                 # the K for THIS run
export FANOUT_CONCURRENCY_CAP="20"   # max concurrent sub-reads per logical request
export FANOUT_SUBREAD_TIMEOUT_S="3.0"
export FANOUT_DEADLINE_MS="500"      # SLO deadline D

export RUN_ID="riak_K50_nofault_r1_2026-02-08T1430Z"
export FANOUT_JSONL_PATH="runs/$RUN_ID/fanout_requests.jsonl"
export STREAM_NAME="fanout"
```


## Run Locust headless
```
locust -f 2_locust.py --headless \
  --users 10 --spawn-rate 10 \
  --run-time 10m \
  --csv runs/locust_fanout
```