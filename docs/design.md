# Design

InferX is an HTTP gateway in front of LLM engine replicas: `llm-inference-engine` processes, or anything that speaks OpenAI-style `/v1/completions`. It is written in Go with only the standard library.

```
clients ──► inferx: tenant key → admission control → router → replica ──► llm serve (process)
                                  (limit, queue,        (policy)          llm serve
                                   per-tenant caps)                       llm serve
            pool: launch/stop processes, health checks, ejection
            autoscaler: replica count from in-flight + queued load
```

## Routing (`internal/router`)

* `round_robin`, and `least_loaded` (fewest requests in flight).
* `prefix_affinity`: consistent hashing of the first `prefix_bytes` of the prompt.
  * **Why.** Requests that share a prefix (a system prompt, few-shot examples, a file header) reach the same replica. That replica's paged KV cache already holds the prefix's blocks, so prefill is skipped (see `llm-inference-engine/docs/design.md`).
  * **Ring.** 64 virtual nodes per replica; positions depend only on the replica id. Adding or removing a replica moves only about 1/n of the prefixes (tested).
  * **Bounded load** (Mirrokni, Thorup, Zadimoghaddam, *Consistent Hashing with Bounded Loads*). A replica already above ⌈(1 + ε) × average in-flight⌉ is skipped, and the walk continues around the ring (ε = 0.25). So a hot prefix cannot overload one replica (tested: 100 concurrent requests with one prefix are spread across all four replicas).

## Admission control (`internal/admit`)

* **Global limit.** In-flight requests are capped at `max_inflight_per_replica` × the current replica count. Beyond the cap, requests wait in a bounded FIFO queue (`max_queue`) for at most `max_wait_ms`; then they are shed with `503` and `Retry-After`.
* **Per-tenant limit.** Each tenant has an in-flight cap. A request beyond it gets `429` immediately, so one tenant cannot take the whole queue.

## Pool (`internal/pool`)

* **Launching.** A replica is a process started from `replica_command` (`{port}` is replaced by a free port). It joins the router once `/healthz` answers. Static replicas can also be listed.
* **Health.**
  * Active: `/healthz` every second.
  * Passive: three consecutive failed proxied requests (5xx or connection errors) eject a replica for `EjectFor`; it is readmitted when its health check passes again.
  * A replica process that exits is removed.
* **Graceful removal.** A retiring replica leaves the router at once. It is stopped (SIGINT, then SIGKILL after 5 s) only when its in-flight requests are finished (tested with a real process).

## Autoscaling (`internal/autoscale`)

* **Sizing.** Desired replicas = ⌈(in-flight + queued) / `target_inflight_per_replica`⌉, clamped to [min, max].
* **Scaling up** happens at once.
* **Scaling down.** The target is the maximum desired value over the last `scale_down_after_s`; that stabilisation window prevents flapping. The pool then shrinks by one replica per tick.
* **Coupling.** The admission limit follows the pool size.

## Proxying (`internal/gateway`)

* **Body.** The request body is read once (up to 4 MiB) to get the prompt, then forwarded unchanged.
* **Streaming.** Responses, server-sent events included, are copied and flushed chunk by chunk. `X-Inferx-Replica` names the replica that served the request.
* **Retries.** A connection failure before any byte reached the client is retried once on another replica. A `5xx` from a replica is passed through, not retried, because generation is not idempotent under sampling; it does count toward ejection.
* **Metrics.** `/metrics` reports:
  * in-flight and queued requests, the admission limit and shed counts;
  * responses by status code and retries;
  * latency and time to first token (p50/p99);
  * per-replica load.
