# Benchmarks

## SmolLM2-135M-Instruct replicas (2026-10-15)

Produced by `bench/run_smollm2.sh 2026-10-15 3`; summary rendered by `bench/report_smollm2.py`. Raw results: `bench/results/2026-10-15-smollm2/`.

**Setup.** InferX launches 4 `llm serve` replicas of the pretrained **SmolLM2-135M-Instruct** (F16 GGUF, sha256 `5157ca60…afb30`, Apache-2.0; see `llm-inference-engine/docs/pretrained.md`). Each replica has one rayon thread, a 160-block (2,560-token) KV pool and at most 16 sequences. The weights are held as f32 (651 MB per replica). Same workload as before: 16 shared prefixes of held-out Python code (~600 bytes each), one chosen at random per request, plus a unique 1-line suffix; streaming, `max_tokens` 16. **8 concurrent clients, 120 s per run, 3 repetitions per policy**, run in the order round_robin, least_loaded, prefix_affinity within each repetition.

**Load context.** The 4-vCPU machine was shared with other agents' builds and benchmarks. The 1-minute load average was between 17 and 117 during the policy runs (per run below). The numbers are therefore slow and noisy; treat them as a relative comparison under the same contention, not as engine throughput.

### Routing policy (4 replicas, 8 clients, 120 s; mean and range over 3 repetitions)

| policy | requests/s | prefix-cache hit ratio | TTFT p50 (ms) | TTFT p99 (ms) | latency p50 (ms) | failed / shed |
|---|---|---|---|---|---|---|
| round_robin | 0.18 (0.16–0.21) | 9 (3–16) % | 23406 (18034–34104) | 69203 (61193–84087) | 40294 (33458–49610) | 0 / 0 |
| least_loaded | 0.19 (0.16–0.23) | 16 (12–22) % | 26543 (23603–30640) | 69754 (48823–89050) | 39836 (36151–42671) | 0 / 0 |
| prefix_affinity | 0.20 (0.17–0.24) | 23 (15–32) % | 20138 (14900–27966) | 72739 (58901–89097) | 39533 (37476–42206) | 0 / 0 |

Per repetition (requests/s, hit ratio, 1-min load average before → after):

| rep | round_robin | least_loaded | prefix_affinity |
|---|---|---|---|
| 1 | 0.17 req/s, 9 %, load 17.26 → 116.59 | 0.17 req/s, 14 %, load 100.81 → 28.73 | 0.19 req/s, 21 %, load 26.77 → 20.31 |
| 2 | 0.16 req/s, 3 %, load 19.96 → 21.27 | 0.16 req/s, 12 %, load 20.55 → 19.80 | 0.17 req/s, 15 %, load 19.66 → 19.63 |
| 3 | 0.21 req/s, 16 %, load 19.74 → 17.75 | 0.23 req/s, 22 %, load 17.48 → 19.49 | 0.24 req/s, 32 %, load 18.64 → 18.63 |

Ratios per repetition, prefix_affinity / round_robin: requests/s 1.09×, 1.07×, 1.16×


### Autoscaling (prefix affinity, 1–4 replicas, target 4 in flight per replica; 1 client 20 s, 12 clients 90 s, 1 client 60 s)

| rep | requests | failed | shed | latency p50 / p99 (ms) | max replicas | launches / stops | load before → after |
|---|---|---|---|---|---|---|---|
| 1 | 47 | 0 | 0 | 15060 / 68769 | 3 | 3 / 2 | 18.63 → 15.03 |
| 2 | 48 | 0 | 0 | 41226 / 64307 | 3 | 3 / 2 | 15.19 → 8.29 |
| 3 | 86 | 0 | 0 | 7761 / 42810 | 3 | 3 / 2 | 8.29 → 2.23 |

Replica events, repetition 1:

```
23:24:11.481 launched replica-1 (pid 25175, http://127.0.0.1:36921)
23:24:37.589 launched replica-2 (pid 26094, http://127.0.0.1:36623)
23:24:40.641 launched replica-3 (pid 26239, http://127.0.0.1:35289)
23:26:44.482 draining replica-3
23:26:44.704 stopped replica-3
23:26:45.482 draining replica-2
23:26:45.665 stopped replica-2
```

Completed requests per second, repetition 1, averaged over 10 s:

```
t=  1- 10s clients= 1 completed/s= 0.00
t= 11- 20s clients= 1 completed/s= 0.10
t= 21- 30s clients=12 completed/s= 0.10
t= 31- 40s clients=12 completed/s= 0.00
t= 41- 50s clients=12 completed/s= 0.00
t= 51- 60s clients=12 completed/s= 0.00
t= 61- 70s clients=12 completed/s= 0.00
t= 71- 80s clients=12 completed/s= 0.00
t= 81- 90s clients=12 completed/s= 1.20
t= 91-100s clients=12 completed/s= 0.00
t=101-110s clients=12 completed/s= 0.40
t=111-120s clients=10 completed/s= 0.20
t=121-130s clients=10 completed/s= 0.00
t=131-140s clients=10 completed/s= 0.00
t=141-150s clients= 1 completed/s= 1.10
t=151-160s clients= 1 completed/s= 0.50
t=161-170s clients= 1 completed/s= 0.40
t=171-180s clients= 1 completed/s= 0.30
t=181-190s clients= 1 completed/s= 0.10
t=191-200s clients= 1 completed/s= 0.20
t=201-209s clients= 1 completed/s= 0.00
```


### Reading the results

* **The real model is about 100× slower per request than the 3M model,** so the experiment is different in kind. A ~250-token prompt costs about 0.27 GFLOP per token through 30 layers, on one thread per replica, on a contended machine. Each run completed only 19–29 requests, and the median request took about 40 s. Prefill dominates.
* **Prefix affinity still gets the most cache reuse**: 23 % of prompt tokens from cache on average, against 16 % (least loaded) and 9 % (round robin), and it was highest in each of the 3 repetitions. Throughput is higher in every repetition too, but only by 1.07–1.16×, and the ranges of the three policies overlap. With ~25 requests per run, most prefixes are seen only once or twice per replica, so the caches barely warm up. The 1.68× of the 3M-model run (below, ~600 requests per run) is **not reproduced** at this request count.
* **First attempt, 16 clients for 60 s** (`bench/results/2026-10-15-smollm2/attempt_c16_60s/`): only 16 requests finished per run (0.14 req/s, TTFT p50 76–94 s), one per client. That load was too high to measure anything, so the run was stopped after two policies and the client count halved and the duration doubled.
* **Autoscaling works with the real model.** Under the 1 → 12 → 1 client step, the gateway launched 2 more replicas (desired = ⌈12 / 4⌉ = 3), and drained and stopped them after the load dropped, in all 3 repetitions. No request failed and none was shed. A new replica needs 2–25 s to load the 271 MB GGUF on this machine, during which the queue grows; the timeline shows near-zero completions for the first minute of the step.
* **What would make this benchmark meaningful:** more cores per replica, or a GPU, and runs of several minutes so that each replica's cache warms up. Neither was possible on this shared machine in the time available.

## Earlier run: the 3M-parameter in-house model (2026-10-14)

Produced by `bench/run.sh 2026-10-14` on a 4-vCPU cloud VM. InferX launches real `llm-inference-engine` replicas: `llm serve` with one rayon thread each and a 160-block (2,560-token) KV pool, so the cache is under pressure. The load generator streams completions (`max_tokens` 16).

**Workload.** 16 shared prefixes of real held-out Python code (~600 bytes, ~250 tokens each), chosen at random per request, plus a unique 1-line suffix. This is the system-prompt / shared-context pattern that prefix caching serves. The prefix-cache hit ratio is the share of prompt tokens the engines report as served from their KV cache (`usage.cached_tokens`).

### Routing policy (4 replicas, 16 concurrent clients, 40 s)

| policy | requests/s | prefix-cache hit ratio | TTFT p50 / p99 | latency p50 / p99 | per-replica requests |
|---|---|---|---|---|---|
| round_robin | 18.3 | 48 % | 256 / 1181 ms | 607 / 2782 ms | 187 / 187 / 187 / 187 |
| least_loaded | 22.0 | 47 % | 207 / 857 ms | 700 / 1568 ms | 225 / 227 / 208 / 226 |
| prefix_affinity | 30.6 | 66 % | 197 / 1224 ms | 454 / 1700 ms | 289 / 250 / 340 / 364 |

### Autoscaling (prefix affinity, 1–4 replicas, target 4 in flight per replica)

Load schedule: 2 clients for 15 s, 24 clients for 40 s, then 2 clients for 30 s. Scale-down needs 10 s of low load.

Overall: 1911 requests, 0 shed, 0 failed; latency p50 / p99 431 / 2262 ms.

Replica events (gateway log):

```
12:20:53.965 launched replica-1 (pid 18762, http://127.0.0.1:41545)
12:21:10.019 launched replica-2 (pid 18795, http://127.0.0.1:37497)
12:21:10.071 launched replica-3 (pid 18801, http://127.0.0.1:38351)
12:21:10.122 launched replica-4 (pid 18807, http://127.0.0.1:40291)
12:21:59.966 draining replica-4
12:21:59.970 stopped replica-4
12:22:00.967 draining replica-3
12:22:00.972 stopped replica-3
12:22:01.967 draining replica-2
12:22:02.074 stopped replica-2
```

Completed requests per second, averaged over 5 s:

```
t=  1-  5s clients= 2 completed/s=  4.6
t=  6- 10s clients= 2 completed/s=  6.2
t= 11- 15s clients= 2 completed/s=  5.6
t= 16- 20s clients=24 completed/s= 20.2
t= 21- 25s clients=24 completed/s= 44.0
t= 26- 30s clients=24 completed/s= 34.2
t= 31- 35s clients=24 completed/s= 35.8
t= 36- 40s clients=24 completed/s= 35.0
t= 41- 45s clients=24 completed/s= 38.6
t= 46- 50s clients=24 completed/s= 43.4
t= 51- 55s clients=24 completed/s= 40.6
t= 56- 60s clients= 2 completed/s= 21.6
t= 61- 65s clients= 2 completed/s= 23.4
t= 66- 70s clients= 2 completed/s= 11.6
t= 71- 75s clients= 2 completed/s=  5.2
t= 76- 80s clients= 2 completed/s=  4.2
t= 81- 85s clients= 2 completed/s=  6.8
```

### Reading the results

* **Prefix affinity turns the KV cache into a shared-context cache.** It reaches 66 % of prompt tokens served from cache, against 48 % under round robin. With affinity each replica mostly sees its own few of the 16 prefixes; under round robin each sees all 16, and its 2,560-token pool, shared with the running sequences, keeps evicting them. Requests per second go from 18.3 to 30.6 (1.68×), and the median time to first token from 256 to 197 ms.
* **Bounded load limits hot-spotting, but not completely.** Every request is hashed to a preferred replica, yet per-replica counts stay within about 20 % of the mean. The price is in the tail: least-loaded routing has the lowest TTFT p99, because it never queues behind a busy replica that holds the prefix.
* **The autoscaler follows the step.** It launches replicas at once when 24 clients arrive, and retires them one per tick after load has stayed low for the stabilisation window. Draining replicas finish their in-flight requests first: no request failed and none was shed.
* **Scaling down has a cost the autoscaler does not see.** With 2 clients, 4 replicas complete about 22 requests/s and 1 replica about 5/s. Each replica adds its own KV cache: with 4, the 16 prefixes mostly stay cached and each request gets a CPU thread of its own; on 1 they are evicted again. The autoscaler sizes the pool by requests in flight only ([limitations](limitations.md)). A cache-aware policy would weigh hit rate and latency too.

