package router

import (
	"fmt"
	"math"
	"testing"
)

func replicas(n int) []*Replica {
	var rs []*Replica
	for i := 0; i < n; i++ {
		rs = append(rs, NewReplica(fmt.Sprintf("r%d", i), ""))
	}
	return rs
}

func TestPrefixAffinityIsStickyAndBalanced(t *testing.T) {
	rt := New(PrefixAffinity)
	rs := replicas(4)
	rt.SetReplicas(rs)
	// sequential requests (no load): the same prefix always maps to the same replica
	where := map[string]string{}
	for p := 0; p < 400; p++ {
		prompt := []byte(fmt.Sprintf("system prompt %d|user question", p%40))
		r, _ := rt.Pick(prompt)
		rt.Done(r)
		key := string(prompt)
		if w, ok := where[key]; ok && w != r.ID {
			t.Fatalf("prefix %q moved from %s to %s", key, w, r.ID)
		}
		where[key] = r.ID
	}
	// the 40 prefixes spread over all replicas
	count := map[string]int{}
	for _, id := range where {
		count[id]++
	}
	if len(count) != 4 {
		t.Fatalf("prefixes used %d of 4 replicas: %v", len(count), count)
	}
	// only the first PrefixBytes matter
	rt.PrefixBytes = 8
	a, _ := rt.Pick([]byte("12345678 suffix one"))
	rt.Done(a)
	b, _ := rt.Pick([]byte("12345678 another suffix"))
	rt.Done(b)
	if a != b {
		t.Fatalf("same 8-byte prefix went to %s and %s", a.ID, b.ID)
	}
}

func TestBoundedLoadCapsAHotPrefix(t *testing.T) {
	rt := New(PrefixAffinity)
	rs := replicas(4)
	rt.SetReplicas(rs)
	// 100 concurrent requests with ONE prefix: without a bound they would all
	// go to one replica; with epsilon = 0.25 none exceeds ceil(1.25*avg)+1
	var held []*Replica
	for i := 0; i < 100; i++ {
		r, _ := rt.Pick([]byte("hot prefix"))
		held = append(held, r)
	}
	for _, r := range rs {
		if r.Inflight() > int64(math.Ceil(1.25*100/4))+1 {
			t.Fatalf("replica %s holds %d of 100", r.ID, r.Inflight())
		}
	}
	for _, r := range held {
		rt.Done(r)
	}
	for _, r := range rs {
		if r.Inflight() != 0 {
			t.Fatal("inflight accounting leaked")
		}
	}
}

func TestMinimalRemappingWhenAReplicaIsAdded(t *testing.T) {
	rt := New(PrefixAffinity)
	rs := replicas(4)
	rt.SetReplicas(rs)
	before := map[int]string{}
	for p := 0; p < 2000; p++ {
		r, _ := rt.Pick([]byte(fmt.Sprintf("prefix-%d", p)))
		rt.Done(r)
		before[p] = r.ID
	}
	rt.SetReplicas(append(rs, NewReplica("r4", "")))
	moved := 0
	for p := 0; p < 2000; p++ {
		r, _ := rt.Pick([]byte(fmt.Sprintf("prefix-%d", p)))
		rt.Done(r)
		if r.ID != before[p] {
			moved++
			if r.ID != "r4" {
				t.Fatalf("prefix %d moved between old replicas", p)
			}
		}
	}
	// ideal: 1/5 of the keys move
	if moved < 200 || moved > 650 {
		t.Fatalf("%d of 2000 prefixes moved (expected about 400)", moved)
	}
}

func TestUnhealthyReplicasAreSkipped(t *testing.T) {
	for _, pol := range []Policy{RoundRobin, LeastLoaded, PrefixAffinity} {
		rt := New(pol)
		rs := replicas(3)
		rt.SetReplicas(rs)
		rs[1].SetHealthy(false)
		for i := 0; i < 50; i++ {
			r, err := rt.Pick([]byte(fmt.Sprintf("p%d", i)))
			if err != nil || r.ID == "r1" {
				t.Fatalf("%s: picked %v err %v", pol, r, err)
			}
			rt.Done(r)
		}
		rs[0].SetHealthy(false)
		rs[2].SetHealthy(false)
		if _, err := rt.Pick(nil); err != ErrNoReplica {
			t.Fatalf("%s: expected ErrNoReplica, got %v", pol, err)
		}
	}
}

func TestLeastLoadedEqualises(t *testing.T) {
	rt := New(LeastLoaded)
	rs := replicas(3)
	rt.SetReplicas(rs)
	for i := 0; i < 30; i++ {
		rt.Pick(nil)
	}
	for _, r := range rs {
		if r.Inflight() != 10 {
			t.Fatalf("%s has %d in flight", r.ID, r.Inflight())
		}
	}
}
