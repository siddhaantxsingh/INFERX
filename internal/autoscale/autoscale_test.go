package autoscale

import (
	"context"
	"testing"
	"time"
)

type fakePool struct{ n, launches, retires int }

func (p *fakePool) Size() int                        { return p.n }
func (p *fakePool) Launch(ctx context.Context) error { p.n++; p.launches++; return nil }
func (p *fakePool) Retire() bool                     { p.n--; p.retires++; return true }

func TestScalesUpAtOnceAndDownSlowly(t *testing.T) {
	load := 0
	p := &fakePool{n: 1}
	s := &Scaler{Pool: p, Load: func() (int, int) { return load, 0 }, TargetPerReplica: 4, Min: 1, Max: 6, DownAfter: 10 * time.Second}
	t0 := time.Unix(1000, 0)
	ctx := context.Background()
	load = 17 // ceil(17/4) = 5
	s.Step(ctx, t0)
	if p.n != 5 {
		t.Fatalf("scale up to 5, got %d", p.n)
	}
	load = 100
	s.Step(ctx, t0.Add(time.Second))
	if p.n != 6 {
		t.Fatalf("capped at max 6, got %d", p.n)
	}
	load = 0
	for i := 2; i <= 10; i++ {
		s.Step(ctx, t0.Add(time.Duration(i)*time.Second))
	}
	if p.n != 6 {
		t.Fatalf("no scale-down within the stabilisation window, got %d", p.n)
	}
	for i := 12; i <= 30; i++ {
		s.Step(ctx, t0.Add(time.Duration(i)*time.Second))
	}
	if p.n != 1 {
		t.Fatalf("back to min 1, got %d", p.n)
	}
	if p.retires != 5 {
		t.Fatalf("one retirement per step: %d", p.retires)
	}
}
