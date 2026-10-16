# Limitations

* **No GPUs.** Replicas are CPU processes of `llm-inference-engine`. GPU scheduling, multi-GPU replicas and GPU memory-aware placement are BLOCKED (no GPU here). See `nebula-gpu-cloud` for the scheduling side.
* **Routing by a byte prefix of the prompt.** The router never tokenizes; it hashes the first `prefix_bytes` bytes. That works when shared prefixes are at least that long. A shorter shared prefix spreads over several replicas.
* **Single gateway instance.** Admission state and routing tables are in memory. Several gateways would each enforce their own limits.
* **The autoscaler only knows request counts.** It does not use tokens/s, KV-cache occupancy or the latency SLO, and it launches processes on the local machine only. It has no node pool or cloud provider.
* **Static API keys** for tenants. There is no TLS.
* **One retry**, and only for connection failures before the response starts. Requests are not hedged.
* **The SmolLM2 benchmark is small and noisy.** One CPU thread per replica on a shared 4-vCPU machine gives ~0.2 requests/s, so each 120 s run has ~25 requests. That is not enough to warm the replicas' caches or to separate the policies' throughput. The autoscaler also does not account for the 2–25 s a replica needs to load the model.
