<div align="center">

# 🚦 InferX

### A KV-cache-aware inference gateway and autoscaler for LLM replicas: it routes requests that share a prompt prefix to the replica that already has it cached

**Prefix-affinity consistent hashing with bounded load · admission control & load shedding · health checks & ejection · autoscaling real model processes · streaming passthrough**

<br/>

![Go](https://img.shields.io/badge/Go-stdlib_only-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![OpenAI API](https://img.shields.io/badge/OpenAI--style-/v1/completions_·_SSE-10a37f?style=for-the-badge)
![Engines](https://img.shields.io/badge/backends-real_LLM_engine_processes-b7410e?style=for-the-badge)

![Throughput](https://img.shields.io/badge/prefix_affinity-1.68×_requests%2Fs-16a34a?style=flat-square)
![Cache](https://img.shields.io/badge/KV--cache_hit_ratio-48%25_→_66%25-16a34a?style=flat-square)
![Autoscale](https://img.shields.io/badge/autoscaler-1→4→1_replicas,_0_failed-0891b2?style=flat-square)
![Race](https://img.shields.io/badge/go_test-‑race_clean-7c3aed?style=flat-square)
![Deps](https://img.shields.io/badge/third--party_deps-0-64748b?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-64748b?style=flat-square)

[**Why**](#-why-cache-aware-routing) · [**Request path**](#%EF%B8%8F-request-path) · [**Routing**](#-routing-consistent-hashing-with-bounded-load) · [**Autoscaling**](#-autoscaling--replica-lifecycle) · [**Results**](#-results) · [**Testing**](#-testing) · [**Run it**](#-running-it)

</div>

---

## 💡 Why cache-aware routing?

Modern LLM engines (vLLM, SGLang, and my own [LLM inference engine](https://github.com/siddhaantxsingh/LLM-INFERENCE-ENGINE)) keep a **paged KV cache with prefix caching**: if a prompt starts with tokens the replica has already seen (a system prompt, few-shot examples, a file header), that prefill is skipped.

A **round-robin load balancer throws that away.** It spreads every prefix across every replica, so each replica's cache keeps evicting the others' prefixes. InferX hashes the prompt prefix onto a ring, so requests that share context land where the context is already cached, and **bounded load** stops a hot prefix from overwhelming a single replica.

| | |
|---|---|
| 🎯 **Routing** | `round_robin`, `least_loaded`, or **`prefix_affinity`**: consistent hashing (64 virtual nodes per replica) with **bounded loads** (ε = 0.25, Mirrokni–Thorup–Zadimoghaddam) |
| 🚧 **Admission control** | Global in-flight cap that scales with replica count · bounded FIFO wait queue · **503 + `Retry-After`** shedding · per-tenant caps → **429** |
| 🩺 **Health** | Active `/healthz` probes · passive **outlier ejection** after 3 failed requests · automatic readmission · one safe retry on connection failure |
| 📈 **Autoscaler** | Launches and **gracefully drains real engine processes**; scales up immediately, scales down after a stabilisation window |
| 🌊 **Proxy** | Streams SSE chunk by chunk · `X-Inferx-Replica` header · `/metrics` with TTFT and latency p50/p99, shed counts, per-replica load |

---

## 🛣️ Request path

```mermaid
flowchart LR
    C(["🧑‍💻 Client<br/>Bearer tenant key"]):::cl --> AUTH{"🔑 Known<br/>tenant?"}:::chk
    AUTH -- no --> E401(["401"]):::bad
    AUTH --> TC{"👥 Tenant<br/>in-flight cap?"}:::chk
    TC -- over --> E429(["429"]):::bad
    TC --> GL{"🚧 Global limit<br/>max_inflight × replicas"}:::chk
    GL -- full --> Q[["⏳ Bounded FIFO queue<br/>max_queue · max_wait_ms"]]:::q
    Q -- timeout --> E503(["503 + Retry-After"]):::bad
    Q -- slot --> R
    GL -- ok --> R{{"🎯 Router<br/>prefix_affinity · least_loaded<br/>· round_robin"}}:::rt
    R --> REP["⚙️ Replica<br/>llm serve process"]:::rep
    REP -- "SSE stream, flushed per chunk" --> C
    REP -. "connection failed before<br/>first byte → retry once" .-> R

    classDef cl fill:#475569,stroke:#0f172a,color:#ffffff
    classDef chk fill:#f59e0b,stroke:#92400e,color:#111827
    classDef bad fill:#dc2626,stroke:#7f1d1d,color:#ffffff
    classDef q fill:#7c3aed,stroke:#4c1d95,color:#ffffff
    classDef rt fill:#00ADD8,stroke:#0e5a6b,color:#0f172a,stroke-width:2px
    classDef rep fill:#16a34a,stroke:#14532d,color:#ffffff,stroke-width:2px
```

<sub>A replica's <code>5xx</code> is passed through, not retried, because generation isn't idempotent under sampling. It still counts toward ejection.</sub>

---

## 🎯 Routing: consistent hashing with bounded load

```mermaid
flowchart TB
    P(["📝 Prompt<br/>first 256 bytes"]):::in --> H["#️⃣ hash"]:::hash --> RING
    subgraph RING["Hash ring: 64 virtual nodes per replica"]
        direction LR
        V1["vnode → R1"]:::r1 --- V2["vnode → R3"]:::r3 --- V3["vnode → R2"]:::r2 --- V4["vnode → R4"]:::r4
    end
    RING --> CHK{"Preferred replica in-flight<br/>≤ ⌈(1 + ε) × average⌉ ?"}:::chk
    CHK -- yes --> GO(["✅ route there:<br/>prefix already in its KV cache"]):::ok
    CHK -- "no (hot prefix)" --> WALK["↻ walk clockwise<br/>to next replica under bound"]:::walk --> GO

    classDef in fill:#475569,stroke:#0f172a,color:#ffffff
    classDef hash fill:#64748b,stroke:#0f172a,color:#ffffff
    classDef r1 fill:#2563eb,stroke:#1e3a8a,color:#ffffff
    classDef r2 fill:#16a34a,stroke:#14532d,color:#ffffff
    classDef r3 fill:#7c3aed,stroke:#4c1d95,color:#ffffff
    classDef r4 fill:#db2777,stroke:#831843,color:#ffffff
    classDef chk fill:#f59e0b,stroke:#92400e,color:#111827
    classDef ok fill:#16a34a,stroke:#14532d,color:#ffffff,stroke-width:2px
    classDef walk fill:#00ADD8,stroke:#0e5a6b,color:#0f172a
    style RING fill:transparent,stroke:#00ADD8,stroke-width:2px,stroke-dasharray:6 4
```

**Properties, each tested:**
- 🧲 **Stickiness:** a prefix keeps its replica, and only the first `prefix_bytes` matter.
- ⚖️ **Bounded load:** 100 concurrent requests with **one** hot prefix spread so no replica exceeds ⌈1.25 × average⌉ + 1.
- 🔁 **Minimal remapping:** adding a 5th replica moves only about 1/5 of prefixes, and only to the new replica.
- 🩺 **Health-aware:** unhealthy replicas are skipped by every policy.

---

## 📈 Autoscaling & replica lifecycle

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Launching: autoscaler wants more<br/>(spawn process on free port)
    Launching --> Healthy: /healthz answers → joins router
    Healthy --> Ejected: 3 consecutive failed requests<br/>or failed health check
    Ejected --> Healthy: health check passes again
    Healthy --> Draining: autoscaler shrinks<br/>(leaves router immediately)
    Draining --> Stopped: in-flight requests finished<br/>→ SIGINT (SIGKILL after 5 s)
    Healthy --> [*]: process exited → removed
    Stopped --> [*]

    classDef ok fill:#16a34a,color:#ffffff,stroke:#14532d
    classDef warn fill:#f59e0b,color:#111827,stroke:#92400e
    classDef bad fill:#dc2626,color:#ffffff,stroke:#7f1d1d
    classDef mid fill:#2563eb,color:#ffffff,stroke:#1e3a8a
    class Healthy ok
    class Launching,Draining mid
    class Ejected bad
    class Stopped warn
```

**Sizing:** desired = ⌈(in-flight + queued) / `target_inflight_per_replica`⌉, clamped to [min, max]. **Scale up immediately**; scale down to the **maximum desired value over the stabilisation window** (no flapping), one replica per tick. The admission limit follows the pool size.

---

## 📊 Results

### Routing policy: 4 real engine replicas, 16 streaming clients, 16 shared code prefixes

```mermaid
%%{init: {"themeVariables": {"xyChart": {"plotColorPalette": "#00ADD8"}}}}%%
xychart-beta
    title "Requests per second by routing policy"
    x-axis ["round_robin", "least_loaded", "prefix_affinity"]
    y-axis "requests / s" 0 --> 35
    bar [18.3, 22.0, 30.6]
```

| Policy | Requests/s | **KV-cache hit ratio** | TTFT p50 | Latency p50 | Per-replica requests |
|---|:---:|:---:|:---:|:---:|:---:|
| round_robin | 18.3 | 48% | 256 ms | 607 ms | 187 / 187 / 187 / 187 |
| least_loaded | 22.0 | 47% | 207 ms | 700 ms | 225 / 227 / 208 / 226 |
| **prefix_affinity** | **30.6 (1.68×)** | **66%** | **197 ms** | **454 ms** | 289 / 250 / 340 / 364 |

<sub>Replicas are real <code>llm serve</code> processes from my <a href="https://github.com/siddhaantxsingh/LLM-INFERENCE-ENGINE">LLM inference engine</a> (the 3M-parameter model, 1 thread each, a deliberately tight 2,560-token KV pool). The hit ratio is reported by the engines themselves (<code>usage.cached_tokens</code>). The trade-off is visible in the tail: least-loaded has the best TTFT p99, because it never queues behind a busy replica that holds the prefix.</sub>

### Autoscaler under a 2 → 24 → 2 client step

```mermaid
%%{init: {"themeVariables": {"xyChart": {"plotColorPalette": "#16a34a, #f59e0b"}}}}%%
xychart-beta
    title "Completed requests/s (5 s buckets) while clients step 2 → 24 → 2"
    x-axis "time (s)" [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85]
    y-axis "requests / s" 0 --> 50
    line [4.6, 6.2, 5.6, 20.2, 44.0, 34.2, 35.8, 35.0, 38.6, 43.4, 40.6, 21.6, 23.4, 11.6, 5.2, 4.2, 6.8]
    line [2, 2, 2, 24, 24, 24, 24, 24, 24, 24, 24, 2, 2, 2, 2, 2, 2]
```
<sub>🟩 completed requests/s · 🟧 concurrent clients. Replicas went 1 → 4 the moment load arrived, then were drained one per tick after load stayed low: <b>1,911 requests, 0 failed, 0 shed.</b></sub>

### Re-run with a pretrained model: SmolLM2-135M-Instruct replicas

| Policy | Requests/s | KV-cache hit ratio | TTFT p50 |
|---|:---:|:---:|:---:|
| round_robin | 0.18 | 9% | 23.4 s |
| least_loaded | 0.19 | 16% | 26.5 s |
| **prefix_affinity** | **0.20** | **23%** | **20.1 s** |

> [!NOTE]
> **Reported honestly:** prefix affinity had the **highest cache-hit ratio in all 3 repetitions**, but with one CPU thread per 135M replica on a heavily shared 4-vCPU machine, each 120 s run finished only about 25 requests. Caches barely warm up at that count, so the throughput gain (1.07–1.16×) is within noise and the 1.68× above is **not** reproduced here. The autoscaler still went 1 → 3 → 1 with **0 failed and 0 shed** across 3 repetitions. Full method and per-run load averages: [`benchmarks.md`](docs/benchmarks.md)

---

## ✅ Testing

`go test -race ./...` runs in seconds with **no engine binary**: `internal/fake` is a simulated engine with a block-level prefix cache, configurable prefill/decode cost, concurrency slots and **fault injection**.

| Package | What's proven |
|---|---|
| `router` | Stickiness · bounded load under a single hot prefix · ~1/n remapping when adding a replica · unhealthy replicas skipped · `least_loaded` equalises load |
| `gateway` | JSON and SSE passthrough (5 chunks + `[DONE]`) · 401 for unknown keys · **cache hit ratio under affinity ≥ 0.85 and ≥ 0.15 above round robin** · admission: limit 2 + queue 2 → exactly 2 succeed, 8 get 503 + `Retry-After` · 429 per tenant · dead replica retried elsewhere and ejected · 500s passed through and counted · ejected replica readmitted on recovery |
| `autoscale` | Immediate scale-up, capped at max · no scale-down inside the window · one-per-step shrink to min |
| `pool` | **Real processes:** launched and joined after `/healthz` · a retiring replica leaves routing immediately but is **not stopped while a request is in flight** |

**Bugs the tests caught:**
- 🐞 **A data race** (`go test -race`): retiring a replica called `exec.Cmd.Wait` concurrently with the launch goroutine's `Wait`. Now only the launcher waits and closes an `exited` channel.
- 🧪 **An invalid experiment:** prefixes picked with a fixed stride gave each round-robin replica only 4 prefixes, making round robin look as good as affinity. Random selection fixed it.
- 🩺 **A spurious health event:** the health loop probed draining replicas. It now skips them.

---

## 🚀 Running it

```sh
go build -o inferx ./cmd/inferx
./inferx -config configs/example.json            # launches `llm serve` replicas from the engine repo

curl -N localhost:8080/v1/completions -H 'Authorization: Bearer change-me-team-a' \
     -d '{"prompt":"import os\n","max_tokens":16,"stream":true}'

go test -race ./...                              # simulated engine, no binaries needed
bench/run.sh                                     # real-engine benchmark (needs ../llm-inference-engine)
bench/run_smollm2.sh 2026-10-15 3                # same with SmolLM2-135M replicas
```

<details>
<summary><b>⚙️ Example config</b></summary>

<br/>

```json
{
  "listen": "127.0.0.1:8080",
  "policy": "prefix_affinity",
  "prefix_bytes": 256,
  "replica_command": ["../llm-inference-engine/target/release/llm", "serve", "--listen", "127.0.0.1:{port}", "..."],
  "min_replicas": 1,
  "max_replicas": 4,
  "target_inflight_per_replica": 4,
  "max_inflight_per_replica": 16,
  "max_queue": 256,
  "max_wait_ms": 5000,
  "scale_down_after_s": 30,
  "tenants": {"change-me-team-a": "team-a", "change-me-team-b": "team-b"},
  "tenant_max_inflight": {"team-b": 8}
}
```

</details>

---

## ⚠️ Limitations

- **CPU replicas only.** GPU replicas, multi-GPU placement and GPU-memory-aware routing aren't done (no GPU available). See [NEBULA_GPU_CLOUD](https://github.com/siddhaantxsingh/NEBULA_GPU_CLOUD) for the GPU scheduling side.
- **Routing uses a byte prefix, not tokens.** Shared prefixes shorter than `prefix_bytes` spread across replicas.
- **Single gateway instance:** admission state and routing tables are in memory.
- **The autoscaler only sees request counts.** It doesn't use tokens/s, KV occupancy, latency SLOs or model load time (2–25 s for SmolLM2), and it runs local processes only.
- **Static API keys, no TLS.** One retry only, with no hedging.

More: [`design.md`](docs/design.md) · [`testing.md`](docs/testing.md) · [`limitations.md`](docs/limitations.md) · [`requirements.md`](docs/requirements.md)

---

## 📁 Repository layout

```text
INFERX/
├── cmd/
│   ├── inferx/        🚦 the gateway
│   ├── loadgen/       📊 streaming load generator
│   └── fakeengine/    🧪 fake replica process for pool tests
├── internal/
│   ├── router/        🎯 round robin · least loaded · prefix affinity (bounded-load ring)
│   ├── admit/         🚧 global limit, queue, shedding, tenant caps
│   ├── pool/          ⚙️ process launch, health, ejection, graceful drain
│   ├── autoscale/     📈 sizing + stabilisation window
│   ├── gateway/       🌊 HTTP proxy, SSE streaming, retries, metrics
│   └── fake/          🧪 simulated engine with prefix cache + fault injection
├── bench/             📊 run scripts, prefix workload, recorded results
├── configs/           ⚙️ example.json
└── docs/              📖 design · testing · benchmarks · limitations · requirements
```

---

<div align="center">

**Send the request to where its context already lives.**

<br/>

Built by **[Siddhant Singh](https://github.com/siddhaantxsingh)** · <sub>MIT License</sub>

</div>
