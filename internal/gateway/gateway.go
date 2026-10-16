// Package gateway is the HTTP front end: tenant identification, admission
// control, replica selection, forwarding (including server-sent-event
// streams, flushed chunk by chunk) and metrics.
package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"inferx/internal/admit"
	"inferx/internal/pool"
	"inferx/internal/router"
)

type Gateway struct {
	RT      *router.Router
	Pool    *pool.Pool
	Admit   *admit.Controller
	Tenants map[string]string // API key -> tenant ("" key = anonymous allowed if present)
	client  *http.Client

	mu        sync.Mutex
	latencies []float64 // ms, ring buffer for p50/p99
	ttft      []float64
	statuses  map[int]int64
	retries   int64
}

func New(rt *router.Router, p *pool.Pool, a *admit.Controller, tenants map[string]string) *Gateway {
	return &Gateway{RT: rt, Pool: p, Admit: a, Tenants: tenants, statuses: map[int]int64{},
		client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, ResponseHeaderTimeout: 120 * time.Second}}}
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/completions", g.completions)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /metrics", g.metrics)
	mux.HandleFunc("GET /v1/replicas", g.replicas)
	return mux
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": msg}})
}

func (g *Gateway) record(status int, ms, ttftMs float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.statuses[status]++
	if ms >= 0 {
		g.latencies = append(g.latencies, ms)
		if len(g.latencies) > 100000 {
			g.latencies = g.latencies[50000:]
		}
	}
	if ttftMs >= 0 {
		g.ttft = append(g.ttft, ttftMs)
		if len(g.ttft) > 100000 {
			g.ttft = g.ttft[50000:]
		}
	}
}

func (g *Gateway) tenant(r *http.Request) (string, bool) {
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	t, ok := g.Tenants[key]
	return t, ok
}

func (g *Gateway) completions(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	tenant, ok := g.tenant(r)
	if !ok {
		jsonError(w, 401, "unknown API key")
		g.record(401, -1, -1)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		jsonError(w, 400, "cannot read body")
		return
	}
	var req struct {
		Prompt string `json:"prompt"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		jsonError(w, 400, "invalid JSON")
		g.record(400, -1, -1)
		return
	}
	if err := g.Admit.Acquire(r.Context(), tenant); err != nil {
		code := 503
		if errors.Is(err, admit.ErrTenantLimit) {
			code = 429
		}
		w.Header().Set("Retry-After", "1")
		jsonError(w, code, "overloaded: "+err.Error())
		g.record(code, -1, -1)
		return
	}
	defer g.Admit.Release(tenant)

	// at most two attempts: a connection failure before any byte reached the
	// client is retried on another replica
	var tried *router.Replica
	for attempt := 0; attempt < 2; attempt++ {
		rep, err := g.RT.Pick([]byte(req.Prompt))
		if err == nil && rep == tried {
			g.RT.Done(rep)
			rep, err = g.RT.Pick([]byte(req.Prompt + "\x00retry"))
		}
		if err != nil {
			jsonError(w, 503, "no healthy replica")
			g.record(503, -1, -1)
			return
		}
		tried = rep
		status, ttft, sent, err := g.forward(w, r, rep, body)
		g.RT.Done(rep)
		g.Pool.Report(rep, err == nil && status < 500)
		if err != nil && !sent {
			g.mu.Lock()
			g.retries++
			g.mu.Unlock()
			continue
		}
		g.record(status, float64(time.Since(start).Microseconds())/1000, ttft)
		return
	}
	jsonError(w, 502, "replicas unreachable")
	g.record(502, -1, -1)
}

// forward proxies one request; sent reports whether anything reached the client.
func (g *Gateway) forward(w http.ResponseWriter, r *http.Request, rep *router.Replica, body []byte) (status int, ttftMs float64, sent bool, err error) {
	t0 := time.Now()
	out, err := http.NewRequestWithContext(r.Context(), "POST", rep.URL+"/v1/completions", bytes.NewReader(body))
	if err != nil {
		return 0, -1, false, err
	}
	out.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(out)
	if err != nil {
		return 0, -1, false, err
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if k == "Content-Length" || k == "Connection" {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Inferx-Replica", rep.ID)
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 16<<10)
	ttftMs = -1
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if ttftMs < 0 {
				ttftMs = float64(time.Since(t0).Microseconds()) / 1000
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				return resp.StatusCode, ttftMs, true, nil // client went away
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr == io.EOF {
			return resp.StatusCode, ttftMs, true, nil
		}
		if rerr != nil {
			return resp.StatusCode, ttftMs, true, rerr
		}
	}
}

func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[int(q*float64(len(s)-1)+0.5)]
}

func (g *Gateway) replicas(w http.ResponseWriter, r *http.Request) {
	var out []map[string]any
	for _, m := range g.Pool.Members() {
		out = append(out, map[string]any{"id": m.ID, "url": m.URL, "healthy": m.Healthy(), "inflight": m.Inflight(), "served": m.Served()})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"policy": g.RT.Policy(), "replicas": out, "events": g.Pool.Events()})
}

func (g *Gateway) metrics(w http.ResponseWriter, r *http.Request) {
	in, q, limit := g.Admit.Stats()
	g.mu.Lock()
	defer g.mu.Unlock()
	fmt.Fprintf(w, "inferx_inflight %d\ninferx_queued %d\ninferx_admit_limit %d\ninferx_shed_total %d\ninferx_retries_total %d\n", in, q, limit, g.Admit.Shed, g.retries)
	fmt.Fprintf(w, "inferx_replicas %d\n", g.Pool.Size())
	for code, n := range g.statuses {
		fmt.Fprintf(w, "inferx_responses_total{code=\"%d\"} %d\n", code, n)
	}
	fmt.Fprintf(w, "inferx_latency_ms{quantile=\"0.5\"} %.2f\ninferx_latency_ms{quantile=\"0.99\"} %.2f\n", pct(g.latencies, 0.5), pct(g.latencies, 0.99))
	fmt.Fprintf(w, "inferx_ttft_ms{quantile=\"0.5\"} %.2f\ninferx_ttft_ms{quantile=\"0.99\"} %.2f\n", pct(g.ttft, 0.5), pct(g.ttft, 0.99))
	for _, m := range g.Pool.Members() {
		fmt.Fprintf(w, "inferx_replica_inflight{replica=%q} %d\ninferx_replica_served_total{replica=%q} %d\n", m.ID, m.Inflight(), m.ID, m.Served())
	}
}
