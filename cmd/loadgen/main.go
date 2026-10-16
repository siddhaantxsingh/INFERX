// loadgen drives an OpenAI-style completions endpoint with a shared-prefix
// workload and reports throughput, latency, time to first token and the
// prefix-cache hit ratio reported by the engines (usage.cached_tokens).
//
//	loadgen -url http://127.0.0.1:8080 -key k -c 16 -d 30s -prefixes prefixes.txt (separated by ----- lines) -max-tokens 16 [-stream] [-out r.json]
//	loadgen ... -schedule "10s:2,20s:16,20s:2"   (concurrency over time)
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stats struct {
	mu                   sync.Mutex
	lat, ttft            []float64
	ok, fail, shed       int64
	prompt, cached, toks int64
	timeline             []map[string]any
}

func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[int(q*float64(len(s)-1)+0.5)]
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080", "gateway or engine base URL")
	key := flag.String("key", "", "API key")
	conc := flag.Int("c", 8, "concurrent clients")
	dur := flag.Duration("d", 20*time.Second, "duration")
	schedule := flag.String("schedule", "", "phases dur:clients,... (overrides -c/-d)")
	prefixFile := flag.String("prefixes", "", "file of shared prefixes separated by lines of -----")
	maxTokens := flag.Int("max-tokens", 16, "tokens to generate")
	stream := flag.Bool("stream", false, "use server-sent events (measures TTFT)")
	out := flag.String("out", "", "write JSON results here")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	var prefixes []string
	if *prefixFile != "" {
		raw, err := os.ReadFile(*prefixFile)
		if err != nil {
			panic(err)
		}
		for _, p := range strings.Split(string(raw), "\n-----\n") {
			if strings.TrimSpace(p) != "" {
				prefixes = append(prefixes, strings.TrimRight(p, "\n")+"\n")
			}
		}
	} else {
		prefixes = []string{"def main():\n    "}
	}
	type phase struct {
		d time.Duration
		c int
	}
	var phases []phase
	if *schedule != "" {
		for _, s := range strings.Split(*schedule, ",") {
			p := strings.SplitN(s, ":", 2)
			d, _ := time.ParseDuration(p[0])
			c, _ := strconv.Atoi(p[1])
			phases = append(phases, phase{d, c})
		}
	} else {
		phases = []phase{{*dur, *conc}}
	}
	st := &stats{}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512}, Timeout: 120 * time.Second}
	var seq atomic.Int64
	start := time.Now()
	one := func(rng *rand.Rand) {
		n := seq.Add(1)
		prompt := prefixes[rng.Intn(len(prefixes))] + fmt.Sprintf("    # request %d\n", n)
		body, _ := json.Marshal(map[string]any{"prompt": prompt, "max_tokens": *maxTokens, "stream": *stream})
		req, _ := http.NewRequest("POST", *url+"/v1/completions", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+*key)
		t0 := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			atomic.AddInt64(&st.fail, 1)
			time.Sleep(50 * time.Millisecond)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == 503 || resp.StatusCode == 429 {
			io.Copy(io.Discard, resp.Body)
			atomic.AddInt64(&st.shed, 1)
			time.Sleep(50 * time.Millisecond)
			return
		}
		if resp.StatusCode != 200 {
			io.Copy(io.Discard, resp.Body)
			atomic.AddInt64(&st.fail, 1)
			return
		}
		ttft := -1.0
		var usage struct {
			Prompt int `json:"prompt_tokens"`
			Cached int `json:"cached_tokens"`
			Comp   int `json:"completion_tokens"`
		}
		if *stream {
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				l := sc.Text()
				if !strings.HasPrefix(l, "data: ") || l == "data: [DONE]" {
					continue
				}
				if ttft < 0 {
					ttft = float64(time.Since(t0).Microseconds()) / 1000
				}
				var ev struct {
					Usage *struct {
						Prompt int `json:"prompt_tokens"`
						Cached int `json:"cached_tokens"`
						Comp   int `json:"completion_tokens"`
					} `json:"usage"`
				}
				if json.Unmarshal([]byte(l[6:]), &ev) == nil && ev.Usage != nil {
					usage.Prompt, usage.Cached, usage.Comp = ev.Usage.Prompt, ev.Usage.Cached, ev.Usage.Comp
				}
			}
		} else {
			var r struct {
				Usage struct {
					Prompt int `json:"prompt_tokens"`
					Cached int `json:"cached_tokens"`
					Comp   int `json:"completion_tokens"`
				} `json:"usage"`
			}
			json.NewDecoder(resp.Body).Decode(&r)
			usage.Prompt, usage.Cached, usage.Comp = r.Usage.Prompt, r.Usage.Cached, r.Usage.Comp
		}
		ms := float64(time.Since(t0).Microseconds()) / 1000
		st.mu.Lock()
		st.ok++
		st.lat = append(st.lat, ms)
		if ttft >= 0 {
			st.ttft = append(st.ttft, ttft)
		}
		st.prompt += int64(usage.Prompt)
		st.cached += int64(usage.Cached)
		st.toks += int64(usage.Comp)
		st.mu.Unlock()
	}
	// per-second timeline of completed requests and active clients
	var active atomic.Int64
	stopTL := make(chan struct{})
	go func() {
		last := int64(0)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopTL:
				return
			case <-t.C:
				st.mu.Lock()
				st.timeline = append(st.timeline, map[string]any{"t": int(time.Since(start).Seconds() + 0.5), "clients": active.Load(), "completed": st.ok - last})
				last = st.ok
				st.mu.Unlock()
			}
		}
	}()
	for pi, ph := range phases {
		end := time.Now().Add(ph.d)
		var wg sync.WaitGroup
		for i := 0; i < ph.c; i++ {
			wg.Add(1)
			active.Add(1)
			go func(i int) {
				defer wg.Done()
				defer active.Add(-1)
				rng := rand.New(rand.NewSource(*seed*1000 + int64(pi*100+i)))
				for time.Now().Before(end) {
					one(rng)
				}
			}(i)
		}
		wg.Wait()
	}
	close(stopTL)
	el := time.Since(start).Seconds()
	res := map[string]any{
		"requests_ok": st.ok, "failed": st.fail, "shed": st.shed, "seconds": el, "requests_per_s": float64(st.ok) / el,
		"tokens_per_s": float64(st.toks) / el, "latency_ms_p50": pct(st.lat, 0.5), "latency_ms_p99": pct(st.lat, 0.99),
		"ttft_ms_p50": pct(st.ttft, 0.5), "ttft_ms_p99": pct(st.ttft, 0.99),
		"prefix_cache_hit_ratio": float64(st.cached) / float64(max(st.prompt, 1)), "timeline": st.timeline,
	}
	b, _ := json.MarshalIndent(res, "", " ")
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, b, 0o644)
	}
}
