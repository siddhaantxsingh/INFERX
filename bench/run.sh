#!/bin/sh
# InferX in front of real llm-inference-engine replicas. bench/run.sh [DATE]
set -eu
D=${1:-$(date +%Y-%m-%d)}
OUT=bench/results/$D
mkdir -p "$OUT"
export PATH=$PATH:/usr/local/go/bin
go build -o /tmp/inferx-bin ./cmd/inferx && go build -o /tmp/inferx-loadgen ./cmd/loadgen
(cd ../llm-inference-engine && cargo build --release -q)
python3 bench/make_prefixes.py "$OUT/prefixes.txt"
ENGINE=$(cd ../llm-inference-engine && pwd)
cfg() { # policy min max
  cat > /tmp/inferx-bench.json <<JSON
{"listen": "127.0.0.1:18300", "policy": "$1", "prefix_bytes": 256,
 "replica_command": ["env", "RAYON_NUM_THREADS=1", "$ENGINE/target/release/llm", "serve", "--listen", "127.0.0.1:{port}",
   "--model", "$ENGINE/models/small.llmw", "--tokenizer", "$ENGINE/models/tokenizer.json", "--blocks", "160", "--max-seqs", "16"],
 "min_replicas": $2, "max_replicas": $3, "target_inflight_per_replica": 4, "max_inflight_per_replica": 16,
 "max_queue": 512, "max_wait_ms": 30000, "scale_down_after_s": 10, "tenants": {"bench": "bench"}}
JSON
}
run_gw() { # name policy min max loadgen-args...
  name=$1; shift; cfg "$1" "$2" "$3"; shift 3
  /tmp/inferx-bin -config /tmp/inferx-bench.json 2> "$OUT/$name.log" &
  GW=$!
  until curl -sf 127.0.0.1:18300/healthz >/dev/null; do sleep 0.2; done
  /tmp/inferx-loadgen -url http://127.0.0.1:18300 -key bench -prefixes "$OUT/prefixes.txt" -max-tokens 16 -stream -out "$OUT/$name.json" "$@" > /dev/null
  curl -sf 127.0.0.1:18300/v1/replicas > "$OUT/$name.replicas.json"
  kill -INT $GW; wait $GW 2>/dev/null || true
}
for pol in round_robin least_loaded prefix_affinity; do
  run_gw "policy_$pol" $pol 4 4 -c 16 -d 40s
done
run_gw autoscale prefix_affinity 1 4 -schedule "15s:2,40s:24,30s:2"
python3 bench/report.py "$D" > docs/benchmarks.md
