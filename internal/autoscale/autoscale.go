// Package autoscale sizes the replica pool from observed load.
//
// Every tick it computes the desired size from the busiest of
//   - in-flight + queued requests / TargetPerReplica, and
//   - the same from the previous ticks' peak (scale-down stabilisation:
//     the pool only shrinks after load has stayed low for DownAfter);
//
// clamped to [Min, Max]. Scale-up happens at once (one launch per tick may
// still be in progress); scale-down retires one replica per tick.
package autoscale

import (
	"context"
	"math"
	"time"
)

type Pool interface {
	Size() int
	Launch(ctx context.Context) error
	Retire() bool
}

type Load func() (inflight, queued int)

type Scaler struct {
	Pool             Pool
	Load             Load
	TargetPerReplica float64
	Min, Max         int
	DownAfter        time.Duration
	OnResize         func(size int)
	history          []sample
}

type sample struct {
	at      time.Time
	desired int
}

// Desired is the replica count for the given load (exported for tests).
func (s *Scaler) Desired(inflight, queued int) int {
	d := int(math.Ceil(float64(inflight+queued) / s.TargetPerReplica))
	if d < s.Min {
		d = s.Min
	}
	if d > s.Max {
		d = s.Max
	}
	return d
}

// Step runs one scaling decision at time now and returns the target size.
func (s *Scaler) Step(ctx context.Context, now time.Time) int {
	in, q := s.Load()
	want := s.Desired(in, q)
	s.history = append(s.history, sample{now, want})
	// keep DownAfter worth of history; the target is its maximum
	cut := 0
	for cut < len(s.history) && now.Sub(s.history[cut].at) > s.DownAfter {
		cut++
	}
	s.history = s.history[cut:]
	target := 0
	for _, h := range s.history {
		if h.desired > target {
			target = h.desired
		}
	}
	size := s.Pool.Size()
	switch {
	case size < target:
		for i := size; i < target; i++ {
			if err := s.Pool.Launch(ctx); err != nil {
				break
			}
		}
	case size > target:
		s.Pool.Retire()
	}
	if s.OnResize != nil {
		s.OnResize(s.Pool.Size())
	}
	return target
}

func (s *Scaler) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.Step(ctx, now)
		}
	}
}
