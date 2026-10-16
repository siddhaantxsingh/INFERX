package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"inferx/internal/admit"
	"inferx/internal/fake"
	"inferx/internal/pool"
	"inferx/internal/router"
)

type env struct {
	gw      *Gateway
	srv     *httptest.Server
	engines []*fake.Engine
	backs   []*httptest.Server
	pool    *pool.Pool
}

func setup(t *testing.T, policy router.Policy, n int, mk func() *fake.Engine, limit, queue int, wait time.Duration, caps map[string]int) *env {
	t.Helper()
	rt := router.New(policy)
	p := pool.New(rt, nil)
	e := &env{pool: p}
	for i := 0; i < n; i++ {
		en := mk()
		s := httptest.NewServer(en)
		e.engines = append(e.engines, en)
		e.backs = append(e.backs, s)
		p.AddStatic(s.URL)
	}
	e.gw = New(rt, p, admit.New(limit, queue, wait, caps), map[string]string{"k1": "alice", "k2": "bob"})
	e.srv = httptest.NewServer(e.gw.Handler())
	t.Cleanup(func() {
		e.srv.Close()
		for _, b := range e.backs {
			b.Close()
		}
	})
	return e
}

func post(url, key string, body map[string]any) (*http.Response, error) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url+"/v1/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+key)
	return http.DefaultClient.Do(req)
}

func quick() *fake.Engine { return fake.New(64, 1024, 0, 0) }

func TestProxyJSONAndStreaming(t *testing.T) {
	e := setup(t, router.RoundRobin, 2, func() *fake.Engine { return fake.New(8, 64, 0, time.Millisecond) }, 100, 10, time.Second, nil)
	resp, err := post(e.srv.URL, "k1", map[string]any{"prompt": "hello", "max_tokens": 3})
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("status %v err %v", resp, err)
	}
	if resp.Header.Get("X-Inferx-Replica") == "" {
		t.Fatal("replica header missing")
	}
	resp.Body.Close()
	resp, _ = post(e.srv.URL, "k1", map[string]any{"prompt": "hello", "max_tokens": 5, "stream": true})
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	sc := bufio.NewScanner(resp.Body)
	var events []string
	for sc.Scan() {
		if l := sc.Text(); strings.HasPrefix(l, "data: ") {
			events = append(events, l)
		}
	}
	resp.Body.Close()
	if len(events) != 6 || events[5] != "data: [DONE]" {
		t.Fatalf("events %v", events)
	}
	if r, _ := post(e.srv.URL, "nope", map[string]any{"prompt": "x"}); r.StatusCode != 401 {
		t.Fatalf("unknown key: %d", r.StatusCode)
	}
}

// 16 "system prompts" shared by many requests. Round robin scatters each
// prefix over every replica's cache; prefix affinity keeps each on one.
func cacheHitRatio(t *testing.T, policy router.Policy) float64 {
	e := setup(t, policy, 4, func() *fake.Engine { return fake.New(64, 96, 0, 0) }, 1000, 100, time.Second, nil)
	var system []string
	for s := 0; s < 16; s++ {
		system = append(system, strings.Repeat(fmt.Sprintf("system prompt %02d. ", s), 30)) // ~600 bytes
	}
	rng := rand.New(rand.NewSource(1)) // a fixed stride would align with round robin
	for i := 0; i < 800; i++ {
		prompt := system[rng.Intn(16)] + fmt.Sprintf("user question %d", i)
		resp, err := post(e.srv.URL, "k1", map[string]any{"prompt": prompt, "max_tokens": 1})
		if err != nil || resp.StatusCode != 200 {
			t.Fatal(resp, err)
		}
		resp.Body.Close()
	}
	var cached, total int64
	for _, en := range e.engines {
		cached += en.Cached.Load()
		total += en.Prompt.Load()
	}
	return float64(cached) / float64(total)
}

func TestPrefixAffinityRaisesCacheHits(t *testing.T) {
	rr := cacheHitRatio(t, router.RoundRobin)
	pa := cacheHitRatio(t, router.PrefixAffinity)
	t.Logf("prefix-cache hit ratio: round robin %.2f, prefix affinity %.2f", rr, pa)
	// each replica caches 96 blocks = 6 KB, i.e. ~10 of the 16 prefixes: round
	// robin thrashes, affinity gives each replica only its ~4. The ceiling is
	// ~0.93 (only the 9 full 64-byte blocks of a ~616-byte prompt are
	// cacheable: 576/616) less the 16 cold misses.
	if pa < 0.85 || pa < rr+0.15 {
		t.Fatalf("affinity %.2f vs round robin %.2f", pa, rr)
	}
}

func TestAdmissionQueuesThenSheds(t *testing.T) {
	slow := func() *fake.Engine { return fake.New(64, 64, 0, 20*time.Millisecond) }
	e := setup(t, router.LeastLoaded, 1, slow, 2, 2, 150*time.Millisecond, map[string]int{"bob": 1})
	var wg sync.WaitGroup
	codes := make(chan int, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := post(e.srv.URL, "k1", map[string]any{"prompt": "x", "max_tokens": 10}) // ~200 ms each
			if err != nil {
				codes <- -1
				return
			}
			if r.StatusCode == 503 && r.Header.Get("Retry-After") == "" {
				codes <- -2
			}
			codes <- r.StatusCode
			r.Body.Close()
		}()
	}
	wg.Wait()
	close(codes)
	got := map[int]int{}
	for c := range codes {
		got[c]++
	}
	// 2 run, 2 wait (and time out after 150 ms while the first two take 200 ms), 6 shed at once
	if got[200] != 2 || got[503] != 8 {
		t.Fatalf("status counts %v", got)
	}
	// per-tenant cap: bob may have one request in flight
	done := make(chan int)
	go func() {
		r, _ := post(e.srv.URL, "k2", map[string]any{"prompt": "x", "max_tokens": 10})
		r.Body.Close()
		done <- r.StatusCode
	}()
	time.Sleep(50 * time.Millisecond)
	r, _ := post(e.srv.URL, "k2", map[string]any{"prompt": "x", "max_tokens": 1})
	if r.StatusCode != 429 {
		t.Fatalf("second bob request: %d", r.StatusCode)
	}
	if c := <-done; c != 200 {
		t.Fatalf("first bob request: %d", c)
	}
}

func TestFailingReplicaIsRetriedEjectedAndReadmitted(t *testing.T) {
	e := setup(t, router.RoundRobin, 3, quick, 100, 10, time.Second, nil)
	e.pool.EjectFor = 300 * time.Millisecond
	dead := e.backs[1]
	dead.Close() // connection refused from now on
	for i := 0; i < 30; i++ {
		r, err := post(e.srv.URL, "k1", map[string]any{"prompt": fmt.Sprint(i), "max_tokens": 1})
		if err != nil || r.StatusCode != 200 {
			t.Fatalf("request %d: %v %v", i, r, err)
		}
		r.Body.Close()
	}
	if e.gw.retries == 0 {
		t.Fatal("expected retries on the dead replica")
	}
	ejected := false
	for _, m := range e.pool.Members() {
		if m.URL == dead.URL && !m.Healthy() {
			ejected = true
		}
	}
	if !ejected {
		t.Fatal("dead replica not ejected")
	}
	// a 500 from a live replica is passed through (not retried) but counts against it
	e.engines[0].FailNext.Store(3)
	fails := 0
	for i := 0; i < 6; i++ {
		r, _ := post(e.srv.URL, "k1", map[string]any{"prompt": "y", "max_tokens": 1})
		if r.StatusCode == 500 {
			fails++
		}
		r.Body.Close()
	}
	if fails != 3 {
		t.Fatalf("%d upstream 500s passed through", fails)
	}
	// health loop readmits a replica that recovers
	e.engines[2].Down.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.pool.HealthLoop(ctx, 50*time.Millisecond)
	waitFor(t, func() bool { return !memberByURL(e.pool, e.backs[2].URL).Healthy() })
	e.engines[2].Down.Store(false)
	waitFor(t, func() bool { return memberByURL(e.pool, e.backs[2].URL).Healthy() })
}

func memberByURL(p *pool.Pool, url string) *pool.Member {
	for _, m := range p.Members() {
		if m.URL == url {
			return m
		}
	}
	return nil
}

func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
