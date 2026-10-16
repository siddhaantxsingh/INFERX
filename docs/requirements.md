# Requirements matrix — Project 17 (InferX)

| Requirement | Status | Evidence |
|---|---|---|
| Gateway in front of model replicas (OpenAI-style API, streaming) | VERIFIED | gateway tests; real-engine benchmark |
| Routing policies incl. KV-cache-aware prefix affinity with bounded load | VERIFIED | router tests; 1.68× throughput vs round robin with real 3M-model engines |
| Routing benchmark with pretrained SmolLM2-135M replicas | PARTIAL | `bench/results/2026-10-15-smollm2/`: affinity has the highest cache hit ratio (23 % vs 9 %) in all 3 repetitions, but the throughput gain (1.07–1.16×) is within noise; shared machine, ~25 requests per run |
| Admission control: global queue with deadline, load shedding, per-tenant caps | VERIFIED | admission test |
| Health checks, passive ejection, retries | VERIFIED | failure-handling test |
| Autoscaling of replica processes with graceful drain | VERIFIED | autoscale and pool tests; autoscaling benchmarks with the 3M model and with SmolLM2-135M (1 → 3 → 1 replicas, 0 failed, 0 shed, 3 repetitions) |
| Metrics | VERIFIED (basic) | `/metrics`, `/v1/replicas` |
| GPU replicas, GPU-aware placement | BLOCKED | no GPU |
| Multi-instance gateway, cloud autoscaling | NOT IMPLEMENTED | limitations |
| CI | VERIFIED | `.github/workflows/inferx.yml` (run 38587619872 green) |
