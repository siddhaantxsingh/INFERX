// Package router picks a replica for each request.
//
// Policies:
//   - round_robin: rotate over healthy replicas;
//   - least_loaded: fewest in-flight requests (ties: round robin);
//   - prefix_affinity: consistent hashing of the prompt's first PrefixBytes
//     bytes, so requests that share a prefix (a system prompt, a few-shot
//     header) land on the replica whose KV cache already holds it. Load is
//     bounded (Mirrokni et al., "Consistent Hashing with Bounded Loads"):
//     a replica already above (1 + epsilon) x the average in-flight load is
//     skipped and the walk continues around the ring.
package router

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type Policy string

const (
	RoundRobin     Policy = "round_robin"
	LeastLoaded    Policy = "least_loaded"
	PrefixAffinity Policy = "prefix_affinity"
)

// Replica is one backend as the router sees it.
type Replica struct {
	ID       string
	URL      string
	inflight atomic.Int64
	healthy  atomic.Bool
	served   atomic.Int64
}

func NewReplica(id, url string) *Replica {
	r := &Replica{ID: id, URL: url}
	r.healthy.Store(true)
	return r
}

func (r *Replica) Inflight() int64   { return r.inflight.Load() }
func (r *Replica) Healthy() bool     { return r.healthy.Load() }
func (r *Replica) SetHealthy(h bool) { r.healthy.Store(h) }
func (r *Replica) Served() int64     { return r.served.Load() }

type Router struct {
	mu          sync.RWMutex
	replicas    []*Replica
	ring        []point // sorted by hash
	policy      Policy
	rr          atomic.Uint64
	PrefixBytes int
	Epsilon     float64
	VNodes      int
}

type point struct {
	h uint64
	r *Replica
}

var ErrNoReplica = errors.New("no healthy replica")

func New(policy Policy) *Router {
	return &Router{policy: policy, PrefixBytes: 256, Epsilon: 0.25, VNodes: 64}
}

func (rt *Router) Policy() Policy { return rt.policy }

func hash64(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	x := h.Sum64()
	// finaliser (splitmix64) to spread FNV's low-entropy high bits
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// SetReplicas replaces the replica set and rebuilds the ring. Replicas keep
// their ring positions across changes (positions depend only on the ID), so
// adding or removing one moves only ~1/n of the prefixes.
func (rt *Router) SetReplicas(rs []*Replica) {
	ring := make([]point, 0, len(rs)*rt.VNodes)
	for _, r := range rs {
		for v := 0; v < rt.VNodes; v++ {
			ring = append(ring, point{hash64([]byte(r.ID + "#" + strconv.Itoa(v))), r})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].h < ring[j].h })
	rt.mu.Lock()
	rt.replicas = append([]*Replica(nil), rs...)
	rt.ring = ring
	rt.mu.Unlock()
}

func (rt *Router) Replicas() []*Replica {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return append([]*Replica(nil), rt.replicas...)
}

// Pick chooses a replica for the prompt and marks a request in flight on
// it; the caller must call Done(replica) when the request ends.
func (rt *Router) Pick(prompt []byte) (*Replica, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	var healthy []*Replica
	var total int64
	for _, r := range rt.replicas {
		if r.Healthy() {
			healthy = append(healthy, r)
			total += r.Inflight()
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoReplica
	}
	var pick *Replica
	switch rt.policy {
	case LeastLoaded:
		start := int(rt.rr.Add(1)) % len(healthy)
		for i := range healthy {
			r := healthy[(start+i)%len(healthy)]
			if pick == nil || r.Inflight() < pick.Inflight() {
				pick = r
			}
		}
	case PrefixAffinity:
		key := prompt
		if len(key) > rt.PrefixBytes {
			key = key[:rt.PrefixBytes]
		}
		// bound: ceil((1+eps) * (total+1) / n), at least 1
		bound := int64((1+rt.Epsilon)*float64(total+1)/float64(len(healthy)) + 0.999999)
		if bound < 1 {
			bound = 1
		}
		h := hash64(key)
		i := sort.Search(len(rt.ring), func(i int) bool { return rt.ring[i].h >= h })
		for n := 0; n < len(rt.ring); n++ {
			r := rt.ring[(i+n)%len(rt.ring)].r
			if r.Healthy() && r.Inflight() < bound {
				pick = r
				break
			}
		}
		if pick == nil { // every replica at the bound: fall back to least loaded
			for _, r := range healthy {
				if pick == nil || r.Inflight() < pick.Inflight() {
					pick = r
				}
			}
		}
	default:
		pick = healthy[int(rt.rr.Add(1))%len(healthy)]
	}
	pick.inflight.Add(1)
	pick.served.Add(1)
	return pick, nil
}

func (rt *Router) Done(r *Replica) { r.inflight.Add(-1) }

// Hold and Release adjust the in-flight count directly (tests).
func (r *Replica) Hold()    { r.inflight.Add(1) }
func (r *Replica) Release() { r.inflight.Add(-1) }
