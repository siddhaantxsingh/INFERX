#!/bin/sh
# InferX routing and autoscaling benchmark with llm-inference-engine replicas
# serving the pretrained SmolLM2-135M-Instruct GGUF.
#   bench/run_smollm2.sh [DATE] [REPS]     (env: MODEL, DUR)
set -eu
D=${1:-$(date +%Y-%m-%d)}
REPS=${2:-3}
DUR=${DUR:-120s}
CLIENTS=${CLIENTS:-8}
MODEL=${MODEL:-/home/user/models/smollm2-135m-instruct-f16.gguf}
OUT=bench/results/$D-smollm2
mkdir -p "$OUT"
export PATH=$PATH:/usr/local/go/bin
go build -o /tmp/inferx-bin ./cmd/inferx && go build -o /tmp/inferx-loadgen ./cmd/loadgen
ENGINE=$(cd ../llm-inference-engine && pwd)
test -x "$ENGINE/target/release/llm" || (cd "$ENGINE" && cargo build --release -q)
test -f "$OUT/prefixes.txt" || python3 bench/make_prefixes.py "$OUT/prefixes.txt"
sha256sum "$MODEL" > "$OUT/model.sha256"
cfg() { # policy min max
  cat > /tmp/inferx-bench.json <<JSON
{"listen": "127.0.0.1:18300", "policy": "$1", "prefix_bytes": 256,
 "replica_command": ["env", "RAYON_NUM_THREADS=1", "$ENGINE/target/release/llm", "serve", "--listen", "127.0.0.1:{port}",
   "--model", "$MODEL", "--ctx", "2048", "--blocks", "160", "--max-seqs", "16"],
 "min_replicas": $2, "max_replicas": $3, "target_inflight_per_replica": 4, "max_inflight_per_replica": 16,
 "max_queue": 512, "max_wait_ms": 120000, "scale_down_after_s": 10, "tenants": {"bench": "bench"}}
JSON
}
run_gw() { # name policy min max loadgen-args...
  name=$1; shift; MIN=$2; cfg "$1" "$2" "$3"; shift 3
  /tmp/inferx-bin -config /tmp/inferx-bench.json 2> "$OUT/$name.log" &
  GW=$!
  until curl -sf 127.0.0.1:18300/healthz >/dev/null; do sleep 0.5; done
  # wait until the minimum replicas have loaded the model and joined
  until [ "$(curl -sf 127.0.0.1:18300/v1/replicas | python3 -c 'import json,sys; print(sum(1 for r in (json.load(sys.stdin)["replicas"] or []) if r.get("healthy")))')" -ge "$MIN" ]; do sleep 1; done
  uptime > "$OUT/$name.uptime"
  /tmp/inferx-loadgen -url http://127.0.0.1:18300 -key bench -prefixes "$OUT/prefixes.txt" -max-tokens 16 -stream -out "$OUT/$name.json" "$@" > /dev/null
  uptime >> "$OUT/$name.uptime"
  curl -sf 127.0.0.1:18300/v1/replicas > "$OUT/$name.replicas.json"
  kill -INT $GW; wait $GW 2>/dev/null || true
}
for r in $(seq 1 "$REPS"); do
  for pol in round_robin least_loaded prefix_affinity; do
    run_gw "policy_${pol}_r$r" $pol 4 4 -c "$CLIENTS" -d "$DUR" -seed "$r"
  done
done
for r in $(seq 1 "$REPS"); do
  run_gw "autoscale_r$r" prefix_affinity 1 4 -schedule "20s:1,90s:12,60s:1" -seed "$r"
done
echo done > "$OUT/DONE"
