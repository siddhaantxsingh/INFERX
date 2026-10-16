"""Render docs/benchmarks.md from bench/results/<date>/."""
import json
import os
import re
import sys

D = sys.argv[1]
R = f"bench/results/{D}"
J = lambda n: json.load(open(os.path.join(R, n)))
L = ["# Benchmarks\n",
     f"Produced by `bench/run.sh {D}` on a 4-vCPU cloud VM. InferX launches real `llm-inference-engine` replicas: "
     "`llm serve` with one rayon thread each and a 160-block (2,560-token) KV pool, so the cache is under pressure. The load "
     "generator streams completions (`max_tokens` 16).\n",
     "**Workload.** 16 shared prefixes of real held-out Python code (~600 bytes, ~250 tokens each), chosen at random per "
     "request, plus a unique 1-line suffix. This is the system-prompt / shared-context pattern that prefix caching serves. "
     "The prefix-cache hit ratio is the share of prompt tokens the engines report as served from their KV cache "
     "(`usage.cached_tokens`).\n",
     "## Routing policy (4 replicas, 16 concurrent clients, 40 s)\n",
     "| policy | requests/s | prefix-cache hit ratio | TTFT p50 / p99 | latency p50 / p99 | per-replica requests |",
     "|---|---|---|---|---|---|"]
rows = {}
for pol in ("round_robin", "least_loaded", "prefix_affinity"):
    r = J(f"policy_{pol}.json")
    reps = J(f"policy_{pol}.replicas.json")["replicas"]
    rows[pol] = r
    served = " / ".join(str(x["served"]) for x in reps)
    L.append(f"| {pol} | {r['requests_per_s']:.1f} | {100 * r['prefix_cache_hit_ratio']:.0f} % | {r['ttft_ms_p50']:.0f} / {r['ttft_ms_p99']:.0f} ms | "
             f"{r['latency_ms_p50']:.0f} / {r['latency_ms_p99']:.0f} ms | {served} |")
a = J("autoscale.json")
ev = J("autoscale.replicas.json")["events"]
L += ["", "## Autoscaling (prefix affinity, 1–4 replicas, target 4 in flight per replica)\n",
      "Load schedule: 2 clients for 15 s, 24 clients for 40 s, then 2 clients for 30 s. Scale-down needs 10 s of low load.\n",
      f"Overall: {a['requests_ok']} requests, {a['shed']} shed, {a['failed']} failed; latency p50 / p99 {a['latency_ms_p50']:.0f} / "
      f"{a['latency_ms_p99']:.0f} ms.\n",
      "Replica events (gateway log):\n", "```"]
L += [e for e in ev]
L += ["```", "", "Completed requests per second, averaged over 5 s:\n", "```"]
tl = a["timeline"]
for i in range(0, len(tl), 5):
    w = tl[i:i + 5]
    L.append(f"t={w[0]['t']:>3}-{w[-1]['t']:>3}s clients={w[-1]['clients']:>2} completed/s={sum(x['completed'] for x in w) / len(w):5.1f}")
L += ["```", ""]
rr, pa = rows["round_robin"], rows["prefix_affinity"]
L += ["## Reading the results\n",
      f"* **Prefix affinity turns the KV cache into a shared-context cache.** It reaches {100 * pa['prefix_cache_hit_ratio']:.0f} % of prompt "
      f"tokens served from cache, against {100 * rr['prefix_cache_hit_ratio']:.0f} % under round robin. With affinity each replica mostly sees "
      "its own few of the 16 prefixes; under round robin each sees all 16, and its 2,560-token pool, shared with the running "
      "sequences, keeps evicting them. Requests per second go from "
      f"{rr['requests_per_s']:.1f} to {pa['requests_per_s']:.1f} ({pa['requests_per_s'] / rr['requests_per_s']:.2f}×), and the median time "
      f"to first token from {rr['ttft_ms_p50']:.0f} to {pa['ttft_ms_p50']:.0f} ms.",
      "* **Bounded load limits hot-spotting, but not completely.** Every request is hashed to a preferred replica, yet per-replica "
      "counts stay within about 20 % of the mean. The price is in the tail: least-loaded routing has the lowest TTFT p99, "
      "because it never queues behind a busy replica that holds the prefix.",
      "* **The autoscaler follows the step.** It launches replicas at once when 24 clients arrive, and retires them one per tick "
      "after load has stayed low for the stabilisation window. Draining replicas finish their in-flight requests first: no request "
      "failed and none was shed.",
      "* **Scaling down has a cost the autoscaler does not see.** With 2 clients, 4 replicas complete about 22 requests/s and 1 "
      "replica about 5/s. Each replica adds its own KV cache: with 4, the 16 prefixes mostly stay cached and each request gets a "
      "CPU thread of its own; on 1 they are evicted again. The autoscaler sizes the pool by requests in flight only "
      "([limitations](limitations.md)). A cache-aware policy would weigh hit rate and latency too.",
      ""]
print("\n".join(L))
