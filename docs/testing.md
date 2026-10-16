# Testing

`go test ./...` runs everything in a few seconds and needs no engine binary. It uses `internal/fake`, a simulated engine with a block-level prefix cache, configurable prefill and decode costs, concurrency slots and fault injection.

| Package | Tests |
|---|---|
| `router` | **Stickiness.** A prefix keeps its replica, and only the first `prefix_bytes` bytes matter. **Bounded load.** 100 concurrent requests with one hot prefix spread so that no replica exceeds ⌈1.25 × average⌉ + 1, and in-flight accounting returns to zero. **Minimal remapping.** Adding a fifth replica moves only prefixes to the new replica, about 1/5 of them. **Health.** Unhealthy replicas are skipped by every policy; with none healthy, `ErrNoReplica`. **least_loaded** equalises load. |
| `gateway` | **Proxying.** JSON and SSE are passed through: 5 chunks plus `[DONE]`, and the replica header is set. Unknown keys get 401. **Cache benefit.** On fake engines with a shared-prefix workload, the prefix-cache hit ratio under affinity is at least 0.85, and at least 0.15 above round robin; the ceiling is ~0.93. **Admission.** Limit 2, queue 2, 150 ms wait, 10 concurrent requests: exactly 2 succeed and 8 get 503 with `Retry-After`. A tenant over its cap gets 429. **Failure handling.** A dead replica's requests are retried elsewhere, and the replica is ejected. Upstream 500s are passed through and count toward ejection. A replica failing `/healthz` is ejected and readmitted once it recovers. |
| `autoscale` | Scaling up happens at once and is capped at max. There is no scale-down inside the stabilisation window; afterwards the pool shrinks one replica per step down to min. |
| `pool` | **Real processes.** Two `fakeengine` processes are launched and joined after `/healthz`. Retiring the newest removes it from routing at once. It is not stopped while a request is in flight, and it is stopped and removed once the request ends. |

## What testing caught while developing

* **An invalid cache-benefit experiment.** The first version picked prefixes with a fixed stride, `(7i) mod 16`. Under round robin over 4 replicas, that stride gave each replica only 4 prefixes, so round robin looked as good as affinity. Random choice fixed the experiment.
* **A spurious health event.** In the first benchmark run, the health loop probed a replica that was draining and logged a failed health check. The loop now skips draining replicas.

* **A data race, found by `go test -race` (which CI runs).** Retiring a replica called `exec.Cmd.Wait` a second time, concurrently with the launch goroutine's own `Wait`. The event log was also read without its lock. Now only the launch goroutine waits; it closes an `exited` channel that retirement waits on, and the event log is read under the lock.

The real-engine measurements are in [benchmarks.md](benchmarks.md).
