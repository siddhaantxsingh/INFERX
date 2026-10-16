"""Summarise bench/results/<date>-smollm2/ (3 repetitions) as Markdown."""
import json
import os
import statistics as st
import sys

R = sys.argv[1]
J = lambda n: json.load(open(os.path.join(R, n)))
reps = sorted({int(f.rsplit("_r", 1)[1].split(".")[0]) for f in os.listdir(R) if f.startswith("policy_round_robin_r") and f.endswith(".replicas.json")})


def load_avg(name):
    lines = open(os.path.join(R, name + ".uptime")).read().strip().splitlines()
    return " → ".join(l.split("load average:")[1].split(",")[0].strip() for l in lines)


def fmt(xs, f="%.1f"):
    return (f % st.mean(xs)) + " (" + (f % min(xs)) + "–" + (f % max(xs)) + ")"


L = ["| policy | requests/s | prefix-cache hit ratio | TTFT p50 (ms) | TTFT p99 (ms) | latency p50 (ms) | failed / shed |", "|---|---|---|---|---|---|---|"]
per = {}
for pol in ("round_robin", "least_loaded", "prefix_affinity"):
    rs = [J(f"policy_{pol}_r{r}.json") for r in reps]
    per[pol] = rs
    L.append(f"| {pol} | {fmt([x['requests_per_s'] for x in rs], '%.2f')} | {fmt([100 * x['prefix_cache_hit_ratio'] for x in rs], '%.0f')} % | "
             f"{fmt([x['ttft_ms_p50'] for x in rs], '%.0f')} | {fmt([x['ttft_ms_p99'] for x in rs], '%.0f')} | {fmt([x['latency_ms_p50'] for x in rs], '%.0f')} | "
             f"{sum(x['failed'] for x in rs)} / {sum(x['shed'] for x in rs)} |")
L += ["", "Per repetition (requests/s, hit ratio, 1-min load average before → after):", "", "| rep | round_robin | least_loaded | prefix_affinity |", "|---|---|---|---|"]
for i, r in enumerate(reps):
    cells = [f"{per[p][i]['requests_per_s']:.2f} req/s, {100 * per[p][i]['prefix_cache_hit_ratio']:.0f} %, load {load_avg(f'policy_{p}_r{r}')}"
             for p in ("round_robin", "least_loaded", "prefix_affinity")]
    L.append(f"| {r} | " + " | ".join(cells) + " |")
L += ["", "Ratios per repetition, prefix_affinity / round_robin: requests/s " +
      ", ".join(f"{per['prefix_affinity'][i]['requests_per_s'] / per['round_robin'][i]['requests_per_s']:.2f}×" for i in range(len(reps))), ""]
L += ["### Autoscaling (prefix affinity, 1–4 replicas, target 4 in flight per replica; 1 client 20 s, 12 clients 90 s, 1 client 60 s)", "",
      "| rep | requests | failed | shed | latency p50 / p99 (ms) | max replicas | launches / stops | load before → after |", "|---|---|---|---|---|---|---|---|"]
for r in reps:
    a = J(f"autoscale_r{r}.json")
    ev = J(f"autoscale_r{r}.replicas.json")["events"]
    launched = sum("launched" in e for e in ev)
    stopped = sum("stopped" in e for e in ev)
    L.append(f"| {r} | {a['requests_ok']} | {a['failed']} | {a['shed']} | {a['latency_ms_p50']:.0f} / {a['latency_ms_p99']:.0f} | {launched} | "
             f"{launched} / {stopped} | {load_avg(f'autoscale_r{r}')} |")
r0 = reps[0]
L += ["", f"Replica events, repetition {r0}:", "", "```"] + J(f"autoscale_r{r0}.replicas.json")["events"] + ["```", "",
      f"Completed requests per second, repetition {r0}, averaged over 10 s:", "", "```"]
tl = J(f"autoscale_r{r0}.json")["timeline"]
for i in range(0, len(tl), 10):
    w = tl[i:i + 10]
    L.append(f"t={w[0]['t']:>3}-{w[-1]['t']:>3}s clients={w[-1]['clients']:>2} completed/s={sum(x['completed'] for x in w) / len(w):5.2f}")
L += ["```"]
print("\n".join(L))
