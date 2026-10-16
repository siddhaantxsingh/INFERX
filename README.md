# InferX (project 17)

An inference gateway and autoscaler in front of LLM engine replicas, written in Go with only the standard library. It provides:

* **KV-cache-aware routing:** prefix-affinity consistent hashing with bounded load;
* **admission control:** a bounded wait queue, load shedding and per-tenant caps;
* **health checks**, outlier ejection and retries;
* **an autoscaler** that launches and gracefully drains real `llm-inference-engine` processes;
* **streaming passthrough** and metrics.

**Status: done (CPU).** All tests pass. The benchmarks use real `llm-inference-engine` replicas, now also serving the pretrained SmolLM2-135M-Instruct model. GPU replicas are BLOCKED (no GPU here).

## Measured

**SmolLM2-135M-Instruct replicas** (2026-10-15; 4 replicas, 8 clients, 120 s, mean of 3 repetitions; shared 4-vCPU machine with load average 17–117):

| policy | requests/s | prefix-cache hit ratio | TTFT p50 |
|---|---|---|---|
| round_robin | 0.18 | 9 % | 23.4 s |
| least_loaded | 0.19 | 16 % | 26.5 s |
| **prefix_affinity** | **0.20** | **23 %** | **20.1 s** |

Affinity had the highest hit ratio in every repetition, but its throughput gain (1.07–1.16×) is within the noise: each run finished only ~25 requests, so the caches barely warmed up. The autoscaler went 1 → 3 → 1 replicas under a 1 → 12 → 1 client step, with 0 failed and 0 shed requests in 3 repetitions. Details and caveats: [docs/benchmarks.md](docs/benchmarks.md).

**Earlier, the 3M-parameter in-house model** (2026-10-14): 4 real engine replicas, 16 concurrent streaming clients, and 16 shared code prefixes.

| policy | requests/s | prefix-cache hit ratio | TTFT p50 |
|---|---|---|---|
| round_robin | 18.3 | 48 % | 256 ms |
| least_loaded | 22.0 | 47 % | 207 ms |
| **prefix_affinity** | **30.6** | **66 %** | **197 ms** |

Under a 2 → 24 → 2 client load step, the autoscaler went from 1 to 4 replicas and back, with 0 failed and 0 shed requests.

## Running

```sh
go build -o inferx ./cmd/inferx && ./inferx -config configs/example.json   # launches llm serve replicas
curl -N localhost:8080/v1/completions -H 'Authorization: Bearer change-me-team-a' -d '{"prompt":"import os\n","max_tokens":16,"stream":true}'
go test ./...                                    # uses the simulated engine; no binaries needed
bench/run.sh                                     # real-engine benchmark, 3M model (needs ../llm-inference-engine)
bench/run_smollm2.sh 2026-10-15 3                # the same with SmolLM2-135M replicas (model in /home/user/models)
```

## Documentation

* [Design](docs/design.md).
* [Testing](docs/testing.md).
* [Benchmarks](docs/benchmarks.md).
* [Limitations](docs/limitations.md).
* [Requirements matrix](docs/requirements.md).

## Scorecard

| Score (0–10) | Value | Why not higher |
|---|---|---|
| Fresher-portfolio strength | 7 | Real routing and autoscaling with real model processes, but the pretrained-model benchmark shows only a small, noisy gain. |
| Engineering maturity | 7 | Race-tested, fault-injection tests, real-process pool tests; single gateway instance and no TLS. |
| Deployment readiness | 4 | Local processes only: no container images, no cloud autoscaling, no multi-instance state, static API keys. |
| Completion of intended scope | 7 | CPU scope is done and re-run with a pretrained model; GPU replicas are blocked, and the SmolLM2 benchmark is too small to confirm the cache-affinity win. |
