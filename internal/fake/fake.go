// Package fake is a simulated LLM engine for tests and CI: it models a
// block-level prefix cache (64-byte blocks, chain-hashed, LRU), prefill cost
// per uncached prompt byte, decode cost per generated token, and a limited
// number of concurrent sequences. It speaks the same /v1/completions API as
// llm-inference-engine (JSON or server-sent events).
package fake

import (
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Engine struct {
	PrefillPerByte time.Duration
	DecodePerToken time.Duration
	CacheBlocks    int
	Slots          chan struct{}
	FailNext       atomic.Int32 // respond 500 to this many requests
	Down           atomic.Bool  // /healthz fails and requests 503

	mu       sync.Mutex
	lru      *list.List
	index    map[uint64]*list.Element
	Requests atomic.Int64
	Cached   atomic.Int64 // prompt bytes served from cache
	Prompt   atomic.Int64 // prompt bytes total
}

const Block = 64

func New(slots, cacheBlocks int, prefill, decode time.Duration) *Engine {
	return &Engine{PrefillPerByte: prefill, DecodePerToken: decode, CacheBlocks: cacheBlocks, Slots: make(chan struct{}, slots),
		lru: list.New(), index: map[uint64]*list.Element{}}
}

// lookupInsert returns how many leading prompt bytes were cached, and caches
// every full block of the prompt.
func (e *Engine) lookupInsert(prompt string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	var parent uint64
	cached, missed := 0, false
	for i := 0; i+Block <= len(prompt); i += Block {
		h := fnv.New64a()
		fmt.Fprintf(h, "%d|%s", parent, prompt[i:i+Block])
		parent = h.Sum64()
		if el, ok := e.index[parent]; ok {
			e.lru.MoveToFront(el)
			if !missed {
				cached += Block
			}
			continue
		}
		missed = true
		e.index[parent] = e.lru.PushFront(parent)
		for e.lru.Len() > e.CacheBlocks {
			old := e.lru.Back()
			e.lru.Remove(old)
			delete(e.index, old.Value.(uint64))
		}
	}
	return cached
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		if e.Down.Load() {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte("ok"))
	case "/v1/completions":
		e.complete(w, r)
	default:
		w.WriteHeader(404)
	}
}

func (e *Engine) complete(w http.ResponseWriter, r *http.Request) {
	e.Requests.Add(1)
	if e.Down.Load() {
		w.WriteHeader(503)
		return
	}
	if e.FailNext.Load() > 0 && e.FailNext.Add(-1) >= 0 {
		w.WriteHeader(500)
		return
	}
	var req struct {
		Prompt    string `json:"prompt"`
		MaxTokens int    `json:"max_tokens"`
		Stream    bool   `json:"stream"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Prompt == "" {
		w.WriteHeader(400)
		return
	}
	if req.MaxTokens == 0 {
		req.MaxTokens = 16
	}
	select {
	case e.Slots <- struct{}{}:
	case <-r.Context().Done():
		return
	}
	defer func() { <-e.Slots }()
	cached := e.lookupInsert(req.Prompt)
	e.Cached.Add(int64(cached))
	e.Prompt.Add(int64(len(req.Prompt)))
	time.Sleep(time.Duration(len(req.Prompt)-cached) * e.PrefillPerByte)
	usage := map[string]int{"prompt_tokens": len(req.Prompt), "completion_tokens": req.MaxTokens, "cached_tokens": cached}
	if !req.Stream {
		time.Sleep(time.Duration(req.MaxTokens) * e.DecodePerToken)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"choices": []map[string]any{{"index": 0, "text": "tok", "finish_reason": "length"}}, "usage": usage})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	f := w.(http.Flusher)
	for i := 0; i < req.MaxTokens; i++ {
		time.Sleep(e.DecodePerToken)
		fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"text\":\"t%d \"}]}\n\n", i)
		f.Flush()
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	f.Flush()
}
